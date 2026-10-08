package loadgen

import (
	"math"
	"sync/atomic"
)

// Histogram records latencies in microseconds into fixed log-linear buckets,
// so its memory does not grow with the number of samples:
//
//	0 – 1 ms     in 10 µs steps
//	1 – 10 ms    in 100 µs steps
//	10 – 100 ms  in 1 ms steps
//	100 ms – 1 s in 10 ms steps
//	1 – 60 s     in 1 s steps
//	60 s and up  in one overflow bucket
//
// A percentile is reported as the UPPER edge of the bucket it falls in, so it
// is never understated and is at most one step high. The maximum is exact.
type Histogram struct {
	shards [histShards]shard
	max    atomic.Int64
}

const (
	histShards  = 32
	histBuckets = 430
	usPerMs     = 1000
)

type shard struct {
	counts [histBuckets]atomic.Uint64
	_      [64]byte // keep neighbouring shards off one cache line
}

type band struct{ lo, step int64 }

var bands = []band{
	{0, 10},
	{1 * usPerMs, 100},
	{10 * usPerMs, 1 * usPerMs},
	{100 * usPerMs, 10 * usPerMs},
	{1000 * usPerMs, 1000 * usPerMs},
}

var bandStart = []int{0, 100, 190, 280, 370}

const overflowBucket = histBuckets - 1

func bucketOf(us int64) int {
	if us < 0 {
		us = 0
	}
	for i := len(bands) - 1; i >= 0; i-- {
		if us >= bands[i].lo {
			idx := bandStart[i] + int((us-bands[i].lo)/bands[i].step)
			if idx >= overflowBucket {
				return overflowBucket
			}
			return idx
		}
	}
	return 0
}

// upperEdge is the largest latency (µs, exclusive) a bucket can hold.
func upperEdge(idx int) int64 {
	if idx >= overflowBucket {
		return -1
	}
	for i := len(bands) - 1; i >= 0; i-- {
		if idx >= bandStart[i] {
			return bands[i].lo + int64(idx-bandStart[i]+1)*bands[i].step
		}
	}
	return 0
}

// Record adds one sample; shard spreads concurrent writers.
func (h *Histogram) Record(shardKey int, us int64) {
	h.shards[shardKey%histShards].counts[bucketOf(us)].Add(1)
	for {
		m := h.max.Load()
		if us <= m || h.max.CompareAndSwap(m, us) {
			return
		}
	}
}

// Percentiles summarises the histogram.
type Percentiles struct {
	Samples   uint64 `json:"samples"`
	P50Micros int64  `json:"p50_us"`
	P95Micros int64  `json:"p95_us"`
	P99Micros int64  `json:"p99_us"`
	MaxMicros int64  `json:"max_us"`
}

// Summary returns the sample count, p50/p95/p99 (bucket upper edges) and the
// exact maximum. A percentile in the overflow bucket reports the maximum.
func (h *Histogram) Summary() Percentiles {
	var merged [histBuckets]uint64
	var total uint64
	for s := range h.shards {
		for b := range merged {
			n := h.shards[s].counts[b].Load()
			merged[b] += n
			total += n
		}
	}
	p := Percentiles{Samples: total, MaxMicros: h.max.Load()}
	if total == 0 {
		return p
	}
	at := func(q float64) int64 {
		// Nearest rank: the ceil(q·N)-th smallest sample, counted from one.
		rank := uint64(math.Ceil(q*float64(total))) - 1
		if rank >= total {
			rank = total - 1
		}
		var seen uint64
		for b, n := range merged {
			seen += n
			if seen > rank {
				if e := upperEdge(b); e >= 0 && e < p.MaxMicros {
					return e
				}
				return p.MaxMicros
			}
		}
		return p.MaxMicros
	}
	p.P50Micros = at(0.50)
	p.P95Micros = at(0.95)
	p.P99Micros = at(0.99)
	return p
}
