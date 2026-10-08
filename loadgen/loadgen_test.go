package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pigfox/websocket-lab/hub"
)

func TestBucketEdgesAreContiguousAndNeverUnderstate(t *testing.T) {
	prev := int64(0)
	for b := 0; b < overflowBucket; b++ {
		e := upperEdge(b)
		if e <= prev {
			t.Fatalf("bucket %d edge %d not above %d", b, e, prev)
		}
		if bucketOf(e-1) != b || bucketOf(e) != b+1 {
			t.Fatalf("bucket %d: edge %d maps to %d/%d", b, e, bucketOf(e-1), bucketOf(e))
		}
		prev = e
	}
	if upperEdge(overflowBucket-1) != 60_000_000 {
		t.Fatalf("last finite edge = %d µs, want 60 s", upperEdge(overflowBucket-1))
	}
	if bucketOf(-5) != 0 || bucketOf(1<<40) != overflowBucket {
		t.Fatal("out-of-range samples misfiled")
	}
}

func TestPercentilesAreBucketUpperEdgesCappedAtTheMax(t *testing.T) {
	var h Histogram
	for i := 0; i < 98; i++ {
		h.Record(i, 120) // 0.12 ms -> bucket [120,130)
	}
	h.Record(0, 4_321)  // 4.321 ms
	h.Record(1, 90_000) // 90 ms
	p := h.Summary()
	if p.Samples != 100 || p.P50Micros != 130 || p.P95Micros != 130 || p.MaxMicros != 90_000 {
		t.Fatalf("%+v", p)
	}
	if p.P99Micros != 4_400 {
		t.Fatalf("p99 = %d, want the 4.3-4.4 ms bucket's upper edge 4400", p.P99Micros)
	}
	var one Histogram
	one.Record(0, 7)
	if p := one.Summary(); p.P50Micros != 7 || p.P99Micros != 7 {
		t.Fatalf("a single sample must report itself, not its bucket edge: %+v", p)
	}
}

func TestRunMeasuresRealSubscribers(t *testing.T) {
	h := hub.New(hub.DefaultConfig())
	up := hub.NewUpgrader(func(*http.Request) bool { return true })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_, _ = h.Serve(conn, "load")
	}))
	defer srv.Close()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				h.Publish(hub.Quote{Coin: "BTC", Bid: "1.0", Ask: "2.0", RecvNanos: time.Now().UnixNano()})
			}
		}
	}()

	res := Run(context.Background(), Config{
		URLs:     []string{"ws" + strings.TrimPrefix(srv.URL, "http")},
		Clients:  40,
		Duration: 500 * time.Millisecond,
	})
	if res.Connected != 40 || res.ConnectFailed != 0 || res.DroppedByHub != 0 {
		t.Fatalf("%+v", res)
	}
	if res.Quotes == 0 || res.Latency.Samples != res.Quotes || res.Latency.P50Micros <= 0 {
		t.Fatalf("nothing measured: %+v", res)
	}
	if res.Latency.P99Micros > res.Latency.MaxMicros || res.Latency.P50Micros > res.Latency.P99Micros {
		t.Fatalf("percentiles out of order: %+v", res.Latency)
	}
	st := h.Stats()
	if st.Classes["load"].Delivered < res.Quotes {
		t.Fatalf("subscribers counted %d quotes, hub delivered only %d", res.Quotes, st.Classes["load"].Delivered)
	}
}

func TestACancelledRunSaysWhy(t *testing.T) {
	h := hub.New(hub.DefaultConfig())
	up := hub.NewUpgrader(func(*http.Request) bool { return true })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_, _ = h.Serve(conn, "load")
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel(errAbortForTest)
	}()
	res := Run(ctx, Config{URLs: []string{"ws" + strings.TrimPrefix(srv.URL, "http")}, Clients: 5, Duration: time.Minute})
	if res.Aborted != errAbortForTest.Error() || res.WindowSeconds > 5 {
		t.Fatalf("%+v", res)
	}
}

type abortErr struct{}

func (abortErr) Error() string { return "probe p99 over limit" }

var errAbortForTest = abortErr{}
