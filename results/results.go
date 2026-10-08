// Package results carries the committed at-scale measurements, so anything
// that shows them — pigfox.com's /demos/websocket-lab among them — reads this
// exact file rather than a retyped copy of its numbers.
//
// summary.json is written by cmd/summarize from the raw per-run files in
// results/raw, which cmd/loadgen writes. Nothing here edits a figure.
package results

import (
	_ "embed"
	"encoding/json"
	"errors"
)

//go:embed summary.json
var summaryJSON []byte

// Run is one level of the scale test.
type Run struct {
	Clients int `json:"clients"`
	// Upgraded is how many handshakes completed; Connected is the peak the
	// hub was actually serving (a subscriber counts once its first frame
	// arrived); NeverServed is the difference at the end of the run.
	Upgraded          int64   `json:"upgraded"`
	Connected         int64   `json:"connected"`
	NeverServed       int64   `json:"never_served"`
	ConnectFailed     int64   `json:"connect_failed"`
	DroppedByHub      int64   `json:"dropped_by_hub"`
	WindowSeconds     float64 `json:"window_seconds"`
	RampSeconds       float64 `json:"ramp_seconds"`
	Quotes            uint64  `json:"quotes"`
	QuotesPerSecond   float64 `json:"quotes_per_second"`
	Conflated         uint64  `json:"conflated"`
	P50Micros         int64   `json:"p50_us"`
	P95Micros         int64   `json:"p95_us"`
	P99Micros         int64   `json:"p99_us"`
	MaxMicros         int64   `json:"max_us"`
	HubPeakGoroutines int     `json:"hub_peak_goroutines"`
	HubPeakHeapMiB    float64 `json:"hub_peak_heap_mib"`
	HubPeakSysMiB     float64 `json:"hub_peak_sys_mib"`
	// HubCPUSeconds is the hub process's CPU time inside the window.
	HubCPUSeconds float64 `json:"hub_cpu_seconds"`
	// UpstreamQuotes is what the feed sent inside the window; the market,
	// not the test, sets this, so runs at different times see different rates.
	UpstreamQuotes    uint64  `json:"upstream_quotes"`
	UpstreamPerSecond float64 `json:"upstream_quotes_per_second"`
	// HubDelivered and HubConflated are the hub's own counters for the window.
	HubDelivered uint64 `json:"hub_delivered"`
	HubConflated uint64 `json:"hub_conflated"`
	// HubOffered is how many quote offers the hub made in the window
	// (one per subscriber per feed price); offered = delivered + replaced,
	// give or take what was still in mailboxes at the window's edges.
	HubOffered uint64 `json:"hub_offered"`
	// HubFanoutSeconds is the time the hub spent offering in the window.
	HubFanoutSeconds float64 `json:"hub_fanout_seconds"`
	// HubFanned is how many feed prices went out to subscribers in the
	// window, and HubInboxConflated how many were replaced in the hub's own
	// inbox by a newer price for the same symbol before a pass reached them.
	HubFanned         uint64 `json:"hub_fanned"`
	HubInboxConflated uint64 `json:"hub_inbox_conflated"`
	StartedUTC        string `json:"started_utc"`
	RawFile           string `json:"raw_file"`
	Note              string `json:"note,omitempty"`
}

// Machine is where the runs happened, as the load generator read it from the
// kernel during the run.
type Machine struct {
	CPUModel string `json:"cpu_model"`
	CPUs     int    `json:"cpus"`
	MemTotal string `json:"mem_total"`
	Kernel   string `json:"kernel"`
	GoArch   string `json:"goarch"`
}

// Summary is the whole committed file.
type Summary struct {
	Machine     Machine  `json:"machine"`
	GoVersion   string   `json:"go_version"`
	Date        string   `json:"date"`
	Feed        string   `json:"feed"`
	Setup       string   `json:"setup"`
	Runs        []Run    `json:"runs"`
	Limitations []string `json:"limitations,omitempty"`
}

// Raw returns summary.json byte for byte.
func Raw() []byte { return append([]byte(nil), summaryJSON...) }

// Load parses summary.json.
func Load() (Summary, error) {
	var s Summary
	if err := json.Unmarshal(summaryJSON, &s); err != nil {
		return Summary{}, err
	}
	if len(s.Runs) == 0 {
		return Summary{}, errors.New("results: summary.json has no runs")
	}
	return s, nil
}
