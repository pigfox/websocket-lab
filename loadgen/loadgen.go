// Package loadgen opens many real WebSocket subscribers against a hub and
// measures what they receive.
//
// Latency is measured, not estimated: every quote carries the moment the hub
// read it off the upstream socket (RecvNanos), and each subscriber subtracts
// that from its own receive time. That is only meaningful when the generator
// and the hub share a clock, i.e. run on the same machine — which is how both
// the in-page test and the docker compose runs are set up.
package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pigfox/websocket-lab/hub"
)

// Config describes one run.
type Config struct {
	// URLs are the hub endpoints; subscribers are spread over them round
	// robin (several ports let one machine exceed one port's 4-tuple space).
	URLs []string
	// Clients is how many subscribers to open.
	Clients int
	// Duration is the measurement window, which opens once every dial has
	// finished.
	Duration time.Duration
	// DialConcurrency bounds dials in flight.
	DialConcurrency int
	// DialTimeout bounds one dial.
	DialTimeout time.Duration
	// Header is sent with every dial.
	Header http.Header
	// Dialer defaults to a dialer with small buffers.
	Dialer *websocket.Dialer
	// Progress, if set, is called about once a second.
	Progress func(Progress)
	// Window, if set, is called with true as the measurement window opens
	// and false as it closes.
	Window func(open bool)
}

// Progress is a running count.
type Progress struct {
	Phase     string `json:"phase"`
	Connected int64  `json:"connected"`
	Quotes    uint64 `json:"quotes"`
}

// Result is what a run measured.
type Result struct {
	ClientsRequested int `json:"clients_requested"`
	// Upgraded counts WebSocket handshakes that completed.
	Upgraded int64 `json:"upgraded"`
	// Connected is the peak number of subscribers the hub was actually
	// serving: a subscriber counts once its first frame from the hub (the
	// snapshot) has arrived, not when its handshake completed.
	Connected int64 `json:"connected"`
	// NeverServed counts subscribers whose handshake completed but which
	// received no frame at all before the run ended.
	NeverServed     int64          `json:"never_served"`
	ConnectFailed   int64          `json:"connect_failed"`
	ConnectErrors   map[string]int `json:"connect_errors,omitempty"`
	DroppedByHub    int64          `json:"dropped_by_hub"`
	ByeReasons      map[string]int `json:"bye_reasons,omitempty"`
	RampSeconds     float64        `json:"ramp_seconds"`
	WindowSeconds   float64        `json:"window_seconds"`
	Quotes          uint64         `json:"quotes"`
	QuotesPerSecond float64        `json:"quotes_per_second"`
	Frames          uint64         `json:"frames"`
	Conflated       uint64         `json:"conflated"`
	Latency         Percentiles    `json:"latency"`
	Aborted         string         `json:"aborted,omitempty"`
}

// Defaults.
const (
	DefaultDialConcurrency = 256
	DefaultDialTimeout     = 10 * time.Second
	progressEvery          = time.Second
	maxErrorKinds          = 8
	maxErrorText           = 120
	nanosPerMicro          = 1000
	readBuffer             = 4096
	writeBuffer            = 256
)

type run struct {
	cfg      Config
	hist     Histogram
	window   atomic.Bool
	stopping atomic.Bool

	upgraded   atomic.Int64
	served     atomic.Int64
	connected  atomic.Int64
	peak       atomic.Int64
	failed     atomic.Int64
	dropped    atomic.Int64
	quotes     atomic.Uint64
	frames     atomic.Uint64
	conflated  atomic.Uint64
	errMu      sync.Mutex
	connErrors map[string]int
	byeReasons map[string]int

	connMu sync.Mutex
	conns  []*websocket.Conn
}

// Run opens the subscribers, measures for cfg.Duration, closes them and
// returns the result. Cancelling ctx ends the run early; the result is still
// returned, with Aborted set to the context's cause.
func Run(ctx context.Context, cfg Config) Result {
	if cfg.DialConcurrency <= 0 {
		cfg.DialConcurrency = DefaultDialConcurrency
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.Dialer == nil {
		cfg.Dialer = &websocket.Dialer{ReadBufferSize: readBuffer, WriteBufferSize: writeBuffer, HandshakeTimeout: cfg.DialTimeout}
	}
	r := &run{cfg: cfg, connErrors: map[string]int{}, byeReasons: map[string]int{}}
	res := Result{ClientsRequested: cfg.Clients}

	progressDone := make(chan struct{})
	phase := atomic.Value{}
	phase.Store("connecting")
	if cfg.Progress != nil {
		go func() {
			t := time.NewTicker(progressEvery)
			defer t.Stop()
			for {
				select {
				case <-progressDone:
					return
				case <-t.C:
					cfg.Progress(Progress{Phase: phase.Load().(string), Connected: r.connected.Load(), Quotes: r.quotes.Load()})
				}
			}
		}()
	}

	var readers sync.WaitGroup
	start := time.Now()
	sem := make(chan struct{}, cfg.DialConcurrency)
	var dials sync.WaitGroup
dialing:
	for i := 0; i < cfg.Clients; i++ {
		select {
		case <-ctx.Done():
			break dialing
		case sem <- struct{}{}:
		}
		dials.Add(1)
		go func(i int) {
			defer dials.Done()
			defer func() { <-sem }()
			r.dial(ctx, i, &readers)
		}(i)
	}
	dials.Wait()
	res.RampSeconds = time.Since(start).Seconds()

	windowStart := time.Now()
	if ctx.Err() == nil {
		phase.Store("measuring")
		if cfg.Window != nil {
			cfg.Window(true)
		}
		r.window.Store(true)
		select {
		case <-ctx.Done():
		case <-time.After(cfg.Duration):
		}
		r.window.Store(false)
		if cfg.Window != nil {
			cfg.Window(false)
		}
	}
	res.WindowSeconds = time.Since(windowStart).Seconds()
	phase.Store("closing")

	r.stopping.Store(true)
	r.connMu.Lock()
	for _, c := range r.conns {
		if err := c.Close(); err != nil {
			continue // already closed by the hub; it was counted when it happened
		}
	}
	r.connMu.Unlock()
	readers.Wait()
	close(progressDone)

	if ctx.Err() != nil {
		res.Aborted = context.Cause(ctx).Error()
	}
	res.Upgraded = r.upgraded.Load()
	res.Connected = r.peak.Load()
	res.NeverServed = res.Upgraded - r.served.Load()
	res.ConnectFailed = r.failed.Load()
	res.DroppedByHub = r.dropped.Load()
	res.Quotes = r.quotes.Load()
	res.Frames = r.frames.Load()
	res.Conflated = r.conflated.Load()
	if res.WindowSeconds > 0 {
		res.QuotesPerSecond = float64(res.Quotes) / res.WindowSeconds
	}
	res.Latency = r.hist.Summary()
	if len(r.connErrors) > 0 {
		res.ConnectErrors = r.connErrors
	}
	if len(r.byeReasons) > 0 {
		res.ByeReasons = r.byeReasons
	}
	return res
}

func (r *run) noteError(m map[string]int, err string) {
	if len(err) > maxErrorText {
		err = err[:maxErrorText]
	}
	r.errMu.Lock()
	defer r.errMu.Unlock()
	if _, ok := m[err]; ok || len(m) < maxErrorKinds {
		m[err]++
	} else {
		m["other"]++
	}
}

func (r *run) dial(ctx context.Context, i int, readers *sync.WaitGroup) {
	url := r.cfg.URLs[i%len(r.cfg.URLs)]
	dctx, cancel := context.WithTimeout(ctx, r.cfg.DialTimeout)
	conn, resp, err := r.cfg.Dialer.DialContext(dctx, url, r.cfg.Header)
	cancel()
	if resp != nil && resp.Body != nil {
		if cerr := resp.Body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if err != nil {
		if ctx.Err() == nil {
			r.failed.Add(1)
			r.noteError(r.connErrors, err.Error())
		}
		return
	}
	r.connMu.Lock()
	if r.stopping.Load() {
		r.connMu.Unlock()
		if cerr := conn.Close(); cerr != nil {
			return
		}
		return
	}
	r.conns = append(r.conns, conn)
	r.connMu.Unlock()
	r.upgraded.Add(1)
	readers.Add(1)
	go r.read(i, conn, readers)
}

// frame is the subset of hub.Frame a subscriber reads.
type frame struct {
	Quotes   []hub.Quote `json:"q"`
	Snapshot []hub.Quote `json:"snap"`
	Bye      string      `json:"bye"`
}

func (r *run) read(i int, conn *websocket.Conn, readers *sync.WaitGroup) {
	defer readers.Done()
	served := false
	defer func() {
		if served {
			r.connected.Add(-1)
		}
	}()
	lastSeq := map[string]uint64{}
	var f frame
	bye := ""
	for {
		_, msg, err := conn.ReadMessage()
		now := time.Now().UnixNano()
		if err == nil && !served {
			served = true
			r.served.Add(1)
			n := r.connected.Add(1)
			for {
				p := r.peak.Load()
				if n <= p || r.peak.CompareAndSwap(p, n) {
					break
				}
			}
		}
		if err != nil {
			if !r.stopping.Load() {
				r.dropped.Add(1)
				if bye == "" {
					bye = closeReason(err)
				}
				r.noteError(r.byeReasons, bye)
			}
			return
		}
		f.Quotes, f.Snapshot, f.Bye = f.Quotes[:0], f.Snapshot[:0], ""
		if json.Unmarshal(msg, &f) != nil {
			continue
		}
		if f.Bye != "" {
			bye = f.Bye
		}
		for _, q := range f.Snapshot {
			lastSeq[q.Coin] = q.Seq
		}
		if len(f.Quotes) == 0 {
			continue
		}
		measuring := r.window.Load()
		if measuring {
			r.frames.Add(1)
		}
		for _, q := range f.Quotes {
			prev := lastSeq[q.Coin]
			lastSeq[q.Coin] = q.Seq
			if !measuring {
				continue
			}
			if prev > 0 && q.Seq > prev+1 {
				r.conflated.Add(q.Seq - prev - 1)
			}
			r.quotes.Add(1)
			r.hist.Record(i, (now-q.RecvNanos)/nanosPerMicro)
		}
	}
}

func closeReason(err error) string {
	var ce *websocket.CloseError
	if errors.As(err, &ce) && ce.Text != "" {
		return ce.Text
	}
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 {
		s = s[i+2:]
	}
	return s
}

// SortedKeys returns m's keys in order, for stable printing.
func SortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
