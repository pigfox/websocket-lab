// Command loadgen opens N real WebSocket subscribers against a running hub,
// measures what they receive, samples the hub's own /stats for its peak
// goroutines and memory, and writes one JSON result file.
//
//	loadgen -clients 10000 -duration 60s \
//	  -urls ws://hub:8080/ws,ws://hub:8081/ws -stats http://hub:8080/stats \
//	  -out results/raw/10000.json
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pigfox/websocket-lab/hub"
	"github.com/pigfox/websocket-lab/loadgen"
)

// HubSample is the hub's state as /stats reported it.
type HubSample struct {
	Goroutines int       `json:"goroutines"`
	HeapMiB    float64   `json:"heap_mib"`
	SysMiB     float64   `json:"sys_mib"`
	CPUSeconds float64   `json:"cpu_seconds"`
	Hub        hub.Stats `json:"hub"`
}

// HubPeaks are the largest values sampled during the run.
type HubPeaks struct {
	Goroutines int     `json:"goroutines"`
	HeapMiB    float64 `json:"heap_mib"`
	SysMiB     float64 `json:"sys_mib"`
	Samples    int     `json:"samples"`
}

// Machine describes where the run happened, read from the kernel at run time.
type Machine struct {
	CPUModel string `json:"cpu_model"`
	CPUs     int    `json:"cpus"`
	MemTotal string `json:"mem_total"`
	Kernel   string `json:"kernel"`
	GoArch   string `json:"goarch"`
}

// File is one run's output.
type File struct {
	Label      string         `json:"label"`
	StartedUTC string         `json:"started_utc"`
	Machine    Machine        `json:"machine"`
	GoVersion  string         `json:"go_version"`
	Clients    int            `json:"clients"`
	Duration   string         `json:"duration"`
	URLs       []string       `json:"urls"`
	Result     loadgen.Result `json:"result"`
	HubPeaks   HubPeaks       `json:"hub_peaks"`
	HubBefore  *HubSample     `json:"hub_before,omitempty"`
	HubAfter   *HubSample     `json:"hub_after,omitempty"`
	HubCPUSec  float64        `json:"hub_cpu_seconds_during_run"`
	// Upstream counts what the feed sent inside the measurement window, read
	// from the hub's own counter at the window's two edges.
	UpstreamQuotes       uint64   `json:"upstream_quotes_in_window"`
	UpstreamQuotesPerSec float64  `json:"upstream_quotes_per_second"`
	HubDeliveredInWindow uint64   `json:"hub_delivered_in_window"`
	HubConflatedInWindow uint64   `json:"hub_conflated_in_window"`
	HubCPUInWindow       float64  `json:"hub_cpu_seconds_in_window"`
	HubOfferedInWindow   uint64   `json:"hub_offered_in_window"`
	HubFanoutSecInWindow float64  `json:"hub_fanout_seconds_in_window"`
	HubFannedInWindow    uint64   `json:"hub_fanned_in_window"`
	HubInboxConflInWin   uint64   `json:"hub_inbox_conflated_in_window"`
	Notes                []string `json:"notes,omitempty"`
}

const (
	sampleEvery = 500 * time.Millisecond
	httpTimeout = 5 * time.Second
	fileMode    = 0o644
	dirMode     = 0o755
)

func main() {
	clients := flag.Int("clients", 1000, "subscribers to open")
	duration := flag.Duration("duration", time.Minute, "measurement window after ramp-up")
	urls := flag.String("urls", "ws://localhost:8080/ws", "hub endpoints, comma separated")
	stats := flag.String("stats", "http://localhost:8080/stats", "hub stats URL")
	out := flag.String("out", "", "result file (default: stdout)")
	label := flag.String("label", "", "free-text label for the run")
	conc := flag.Int("dial-concurrency", loadgen.DefaultDialConcurrency, "dials in flight")
	flag.Parse()

	f := File{
		Label:      *label,
		StartedUTC: time.Now().UTC().Format(time.RFC3339),
		Machine:    machine(),
		GoVersion:  runtime.Version(),
		Clients:    *clients,
		Duration:   duration.String(),
		URLs:       strings.Split(*urls, ","),
	}
	hc := &http.Client{Timeout: httpTimeout}
	before, err := sample(hc, *stats)
	if err != nil {
		log.Fatalf("hub stats before the run: %v", err)
	}
	f.HubBefore = before
	if !before.Hub.Feed.Up {
		f.Notes = append(f.Notes, "upstream feed was down when the run started: "+before.Hub.Feed.LastError)
	}

	var mu sync.Mutex
	peaks := HubPeaks{}
	ctx, cancel := context.WithCancel(context.Background())
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		t := time.NewTicker(sampleEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s, err := sample(hc, *stats)
				if err != nil {
					continue
				}
				mu.Lock()
				peaks.Samples++
				peaks.Goroutines = max(peaks.Goroutines, s.Goroutines)
				peaks.HeapMiB = max(peaks.HeapMiB, s.HeapMiB)
				peaks.SysMiB = max(peaks.SysMiB, s.SysMiB)
				mu.Unlock()
			}
		}
	}()

	var winOpen, winClose *HubSample
	var winOpenAt, winCloseAt time.Time
	res := loadgen.Run(context.Background(), loadgen.Config{
		Window: func(open bool) {
			s, err := sample(hc, *stats)
			if err != nil {
				log.Printf("hub stats at the window edge: %v", err)
				return
			}
			if open {
				winOpen, winOpenAt = s, time.Now()
			} else {
				winClose, winCloseAt = s, time.Now()
			}
		},
		URLs:            f.URLs,
		Clients:         *clients,
		Duration:        *duration,
		DialConcurrency: *conc,
		Progress: func(p loadgen.Progress) {
			log.Printf("%s connected=%d quotes=%d", p.Phase, p.Connected, p.Quotes)
		},
	})
	cancel()
	<-samplerDone
	f.Result = res
	f.HubPeaks = peaks
	if winOpen != nil && winClose != nil {
		f.UpstreamQuotes = winClose.Hub.Published - winOpen.Hub.Published
		f.UpstreamQuotesPerSec = float64(f.UpstreamQuotes) / winCloseAt.Sub(winOpenAt).Seconds()
		f.HubDeliveredInWindow = winClose.Hub.Delivered - winOpen.Hub.Delivered
		f.HubConflatedInWindow = winClose.Hub.Conflated - winOpen.Hub.Conflated
		f.HubCPUInWindow = winClose.CPUSeconds - winOpen.CPUSeconds
		f.HubOfferedInWindow = winClose.Hub.Offered - winOpen.Hub.Offered
		f.HubFanoutSecInWindow = float64(winClose.Hub.FanoutNanos-winOpen.Hub.FanoutNanos) / 1e9
		f.HubFannedInWindow = winClose.Hub.Fanned - winOpen.Hub.Fanned
		f.HubInboxConflInWin = winClose.Hub.UpstreamConflated - winOpen.Hub.UpstreamConflated
	} else {
		f.Notes = append(f.Notes, "the hub's counters could not be read at both window edges")
	}
	if after, err := sample(hc, *stats); err == nil {
		f.HubAfter = after
		f.HubCPUSec = after.CPUSeconds - before.CPUSeconds
	}

	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	b = append(b, '\n')
	if *out == "" {
		if _, err := os.Stdout.Write(b); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := os.MkdirAll(filepath.Dir(*out), dirMode); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*out, b, fileMode); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s: connected=%d p50=%dus p99=%dus quotes/s=%.0f conflated=%d dropped=%d",
		*out, res.Connected, res.Latency.P50Micros, res.Latency.P99Micros, res.QuotesPerSecond, res.Conflated, res.DroppedByHub)
}

func sample(hc *http.Client, url string) (*HubSample, error) {
	resp, err := hc.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			log.Printf("stats body: %v", cerr)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stats: HTTP %d", resp.StatusCode)
	}
	var s HubSample
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func machine() Machine {
	m := Machine{CPUs: runtime.NumCPU(), GoArch: runtime.GOARCH}
	m.CPUModel = procField("/proc/cpuinfo", "model name")
	m.MemTotal = procField("/proc/meminfo", "MemTotal")
	var u syscall.Utsname
	if syscall.Uname(&u) == nil {
		m.Kernel = utsString(u.Release[:])
	}
	return m
}

func procField(path, key string) string {
	f, err := os.Open(path) // a fixed /proc path chosen above, never input
	if err != nil {
		return ""
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			log.Printf("%s: %v", path, cerr)
		}
	}()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if ok && strings.TrimSpace(k) == key {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func utsString(b []int8) string {
	out := make([]byte, 0, len(b))
	for _, c := range b {
		if c == 0 {
			break
		}
		out = append(out, byte(c))
	}
	return string(out)
}
