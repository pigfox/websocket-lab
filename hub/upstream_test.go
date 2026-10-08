package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeFeed accepts a connection, checks the subscriptions, sends one bbo
// message per connection with a price that depends on the connection number,
// then hangs up — so every session ends in a loss the upstream must recover.
func fakeFeed(t *testing.T, sessions *atomic.Int32, subs chan<- []string) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		n := sessions.Add(1)
		var got []string
		for range 2 {
			var m struct {
				Method       string            `json:"method"`
				Subscription map[string]string `json:"subscription"`
			}
			if err := c.ReadJSON(&m); err != nil {
				return
			}
			got = append(got, m.Method+":"+m.Subscription["type"]+":"+m.Subscription["coin"])
		}
		subs <- got
		px := "100.0"
		if n > 1 {
			px = "101.5"
		}
		msg := `{"channel":"bbo","data":{"coin":"BTC","time":` + time.Now().Format("20060102") + `,"bbo":[{"px":"` + px + `"},{"px":"102.0"}]}}`
		if err := c.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
			return
		}
		// A message for a symbol nobody asked for must not be published.
		if err := c.WriteMessage(websocket.TextMessage, []byte(strings.ReplaceAll(msg, `"BTC"`, `"DOGE"`))); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}))
}

func TestUpstreamReconnectsAndKeepsTheLastRealPriceWhileDown(t *testing.T) {
	var sessions atomic.Int32
	subs := make(chan []string, 8)
	feed := fakeFeed(t, &sessions, subs)
	defer feed.Close()

	h := New(DefaultConfig())
	u := NewUpstream()
	u.URL = "ws" + strings.TrimPrefix(feed.URL, "http")
	u.Coins = []string{"BTC", "ETH"}
	u.MinBackoff = 20 * time.Millisecond
	u.MaxBackoff = 40 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { u.Run(ctx, h); close(done) }()

	want := []string{"subscribe:bbo:BTC", "subscribe:bbo:ETH"}
	for i := range 2 {
		select {
		case got := <-subs:
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("session %d subscribed %v, want %v", i+1, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("session %d never subscribed", i+1)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	// The last price is set by the fanout pass, which runs after Publish
	// returns, so wait for it rather than for the publish count.
	for st := h.Stats(); st.Published < 2 || len(st.Last) == 0 || st.Last[0].Seq < 2; st = h.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("published = %d after two sessions", h.Stats().Published)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	st := h.Stats()
	if st.Published != 2 {
		t.Fatalf("published %d quotes, want 2 (DOGE must be ignored)", st.Published)
	}
	if len(st.Last) != 1 || st.Last[0].Coin != "BTC" || st.Last[0].Bid != "101.5" || st.Last[0].Seq != 2 {
		t.Fatalf("last = %+v, want the second session's real BTC price", st.Last)
	}
	if st.Feed.Reconnects < 1 {
		t.Fatalf("reconnects = %d", st.Feed.Reconnects)
	}
}

func TestADownFeedReportsWhenItWentDownAndWhy(t *testing.T) {
	h := New(DefaultConfig())
	h.Publish(Quote{Coin: "BTC", Bid: "1.0", Ask: "2.0", ExchMillis: 42})
	u := NewUpstream()
	u.URL = "ws://127.0.0.1:1/ws" // nothing listens on port 1
	u.MinBackoff = 10 * time.Millisecond
	u.MaxBackoff = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	since := h.Status().SinceMillis
	u.Run(ctx, h)
	st := h.Status()
	if st.Up || !strings.Contains(st.LastError, "dial") {
		t.Fatalf("status = %+v, want down with a dial error", st)
	}
	if st.SinceMillis != since {
		t.Fatalf("failed reconnects moved 'down since' from %d to %d", since, st.SinceMillis)
	}
	b, err := json.Marshal(h.Last())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"b":"1.0"`) || !strings.Contains(string(b), `"x":42`) {
		t.Fatalf("last price lost while down: %s", b)
	}
}
