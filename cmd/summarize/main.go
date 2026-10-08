// Command summarize reads every raw run file in a directory (as written by
// cmd/loadgen) and writes results/summary.json. It copies figures; it does not
// compute new ones beyond picking them out of each file.
//
//	summarize -raw results/raw -out results/summary.json -limit "..." -limit "..."
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pigfox/websocket-lab/results"
)

type rawFile struct {
	StartedUTC string          `json:"started_utc"`
	Machine    results.Machine `json:"machine"`
	GoVersion  string          `json:"go_version"`
	Clients    int             `json:"clients"`
	Result     struct {
		Upgraded        int64   `json:"upgraded"`
		Connected       int64   `json:"connected"`
		NeverServed     int64   `json:"never_served"`
		ConnectFailed   int64   `json:"connect_failed"`
		DroppedByHub    int64   `json:"dropped_by_hub"`
		RampSeconds     float64 `json:"ramp_seconds"`
		WindowSeconds   float64 `json:"window_seconds"`
		Quotes          uint64  `json:"quotes"`
		QuotesPerSecond float64 `json:"quotes_per_second"`
		Conflated       uint64  `json:"conflated"`
		Latency         struct {
			P50 int64 `json:"p50_us"`
			P95 int64 `json:"p95_us"`
			P99 int64 `json:"p99_us"`
			Max int64 `json:"max_us"`
		} `json:"latency"`
		ConnectErrors map[string]int `json:"connect_errors"`
	} `json:"result"`
	HubPeaks struct {
		Goroutines int     `json:"goroutines"`
		HeapMiB    float64 `json:"heap_mib"`
		SysMiB     float64 `json:"sys_mib"`
	} `json:"hub_peaks"`
	HubCPUSec float64  `json:"hub_cpu_seconds_during_run"`
	Upstream  uint64   `json:"upstream_quotes_in_window"`
	UpRate    float64  `json:"upstream_quotes_per_second"`
	HubDeliv  uint64   `json:"hub_delivered_in_window"`
	HubConfl  uint64   `json:"hub_conflated_in_window"`
	HubCPUWin float64  `json:"hub_cpu_seconds_in_window"`
	HubOffer  uint64   `json:"hub_offered_in_window"`
	HubFanout float64  `json:"hub_fanout_seconds_in_window"`
	HubFanned uint64   `json:"hub_fanned_in_window"`
	HubInbox  uint64   `json:"hub_inbox_conflated_in_window"`
	Notes     []string `json:"notes"`
}

const fileMode = 0o644

func main() {
	raw := flag.String("raw", "results/raw", "directory of raw run files")
	out := flag.String("out", "results/summary.json", "summary file to write")
	md := flag.String("md", "results/SUMMARY.md", "markdown table to write beside it (\"\" for none)")
	notesPath := flag.String("notes", "results/notes.json", "the setup sentence and limitations, written by hand")
	flag.Parse()
	var notes struct {
		Setup       string   `json:"setup"`
		Limitations []string `json:"limitations"`
	}
	nb, err := os.ReadFile(*notesPath) // a path given on the command line
	if err != nil {
		log.Fatalf("notes: %v", err)
	}
	if err := json.Unmarshal(nb, &notes); err != nil {
		log.Fatalf("notes: %v", err)
	}

	paths, err := filepath.Glob(filepath.Join(*raw, "*.json"))
	if err != nil || len(paths) == 0 {
		log.Fatalf("no raw files in %s (%v)", *raw, err)
	}
	s := results.Summary{Feed: "Hyperliquid public WebSocket API, bbo channel (best bid and offer)", Setup: notes.Setup, Limitations: notes.Limitations}
	for _, p := range paths {
		b, err := os.ReadFile(p) // a path globbed from the directory given on the command line
		if err != nil {
			log.Fatal(err)
		}
		var f rawFile
		if err := json.Unmarshal(b, &f); err != nil {
			log.Fatalf("%s: %v", p, err)
		}
		if s.Machine.CPUModel == "" {
			s.Machine, s.GoVersion = f.Machine, f.GoVersion
		} else if s.Machine != f.Machine {
			log.Fatalf("%s ran on a different machine (%+v vs %+v); summarize one machine at a time", p, f.Machine, s.Machine)
		}
		if d := f.StartedUTC[:10]; s.Date == "" || d > s.Date {
			s.Date = d
		}
		r := results.Run{
			Clients:           f.Clients,
			Upgraded:          f.Result.Upgraded,
			Connected:         f.Result.Connected,
			NeverServed:       f.Result.NeverServed,
			HubOffered:        f.HubOffer,
			HubFanoutSeconds:  f.HubFanout,
			HubFanned:         f.HubFanned,
			HubInboxConflated: f.HubInbox,
			ConnectFailed:     f.Result.ConnectFailed,
			DroppedByHub:      f.Result.DroppedByHub,
			WindowSeconds:     f.Result.WindowSeconds,
			RampSeconds:       f.Result.RampSeconds,
			Quotes:            f.Result.Quotes,
			QuotesPerSecond:   f.Result.QuotesPerSecond,
			Conflated:         f.Result.Conflated,
			P50Micros:         f.Result.Latency.P50,
			P95Micros:         f.Result.Latency.P95,
			P99Micros:         f.Result.Latency.P99,
			MaxMicros:         f.Result.Latency.Max,
			HubPeakGoroutines: f.HubPeaks.Goroutines,
			HubPeakHeapMiB:    f.HubPeaks.HeapMiB,
			HubPeakSysMiB:     f.HubPeaks.SysMiB,
			HubCPUSeconds:     f.HubCPUWin,
			UpstreamQuotes:    f.Upstream,
			UpstreamPerSecond: f.UpRate,
			HubDelivered:      f.HubDeliv,
			HubConflated:      f.HubConfl,
			StartedUTC:        f.StartedUTC,
			RawFile:           filepath.ToSlash(filepath.Join("results", "raw", filepath.Base(p))),
			Note:              strings.Join(f.Notes, "; "),
		}
		s.Runs = append(s.Runs, r)
	}
	sort.Slice(s.Runs, func(i, j int) bool { return s.Runs[i].Clients < s.Runs[j].Clients })
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*out, append(b, '\n'), fileMode); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s with %d runs", *out, len(s.Runs))
	if *md != "" {
		if err := os.WriteFile(*md, []byte(markdown(s)), fileMode); err != nil {
			log.Fatal(err)
		}
		log.Printf("wrote %s", *md)
	}
}

// markdown renders the summary as a table. It is generated from summary.json
// by this command, so the README links here rather than retyping a figure.
func markdown(s results.Summary) string {
	var b strings.Builder
	b.WriteString("# Results\n\n")
	b.WriteString("Generated by `cmd/summarize` from [`summary.json`](summary.json), which is generated from the raw files in [`raw/`](raw/). Do not edit by hand.\n\n")
	fmt.Fprintf(&b, "- **Machine:** %s, %d logical CPUs, %s, Linux %s, %s\n", s.Machine.CPUModel, s.Machine.CPUs, s.Machine.MemTotal, s.Machine.Kernel, s.GoVersion)
	fmt.Fprintf(&b, "- **Date:** %s\n- **Feed:** %s\n", s.Date, s.Feed)
	if s.Setup != "" {
		fmt.Fprintf(&b, "- **Setup:** %s\n", s.Setup)
	}
	b.WriteString("\n| Subscribers | Served (peak) | Never served | Dropped | p50 | p95 | p99 | Slowest | Delivered/s | Feed/s | Replaced in hub inbox | Replaced in mailboxes | Hub heap peak MiB | Hub goroutines peak | Hub CPUs busy |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	ms := func(us int64) string { return fmt.Sprintf("%.1f ms", float64(us)/1000) }
	for _, r := range s.Runs {
		fmt.Fprintf(&b, "| [%d](%s) | %d | %d | %d | %s | %s | %s | %s | %.0f | %.2f | %d | %d | %.0f | %d | %.1f |\n",
			r.Clients, strings.TrimPrefix(r.RawFile, "results/"), r.Connected, r.NeverServed, r.DroppedByHub,
			ms(r.P50Micros), ms(r.P95Micros), ms(r.P99Micros), ms(r.MaxMicros),
			r.QuotesPerSecond, r.UpstreamPerSecond, r.HubInboxConflated, r.HubConflated,
			r.HubPeakHeapMiB, r.HubPeakGoroutines, r.HubCPUSeconds/r.WindowSeconds)
	}
	if len(s.Limitations) > 0 {
		b.WriteString("\n## Limitations\n\n")
		for _, l := range s.Limitations {
			fmt.Fprintf(&b, "- %s\n", l)
		}
	}
	return b.String()
}
