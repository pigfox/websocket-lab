package results

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTheCommittedSummaryLoadsAndPointsAtRealRawFiles(t *testing.T) {
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if s.Machine.CPUModel == "" || s.Machine.CPUs == 0 || s.Date == "" || s.GoVersion == "" {
		t.Fatalf("the summary does not say where and when it was measured: %+v", s)
	}
	for i, r := range s.Runs {
		if i > 0 && r.Clients <= s.Runs[i-1].Clients {
			t.Fatal("runs must be in ascending order of subscribers")
		}
		if !strings.HasPrefix(r.RawFile, "results/raw/") {
			t.Fatalf("run %d names %q", r.Clients, r.RawFile)
		}
		// The test runs in results/, so the repository root is one up.
		if _, err := os.Stat(filepath.Join("..", filepath.FromSlash(r.RawFile))); err != nil {
			t.Errorf("run %d: its raw file is not committed: %v", r.Clients, err)
		}
		if r.Connected > int64(r.Clients) || r.P50Micros > r.P95Micros || r.P95Micros > r.P99Micros || r.P99Micros > r.MaxMicros {
			t.Errorf("run %d is internally inconsistent: %+v", r.Clients, r)
		}
	}
	if string(Raw()) != string(summaryJSON) {
		t.Fatal("Raw must return the embedded bytes")
	}
}
