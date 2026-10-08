package hub

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// serveHub starts an httptest server whose /ws subscribes to h.
func serveHub(t *testing.T, h *Hub, wrap func(net.Listener) net.Listener) (string, chan DropReason) {
	t.Helper()
	reasons := make(chan DropReason, 16)
	up := NewUpgrader(func(*http.Request) bool { return true })
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		reason, err := h.Serve(conn, "test")
		if err == nil {
			reasons <- reason
		}
	}))
	if wrap != nil {
		srv.Listener = wrap(srv.Listener)
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws", reasons
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func readFrame(t *testing.T, c *websocket.Conn) Frame {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, b, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var f Frame
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	return f
}

// readQuotes reads frames until a quote frame arrives.
func readQuotes(t *testing.T, c *websocket.Conn) []Quote {
	t.Helper()
	for {
		if f := readFrame(t, c); len(f.Quotes) > 0 {
			return f.Quotes
		}
	}
}

func waitClients(t *testing.T, h *Hub, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for h.Stats().Clients != n {
		if time.Now().After(deadline) {
			t.Fatalf("clients = %d, want %d", h.Stats().Clients, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestEverySubscriberGetsTheSameQuote(t *testing.T) {
	h := New(DefaultConfig())
	url, _ := serveHub(t, h, nil)
	a, b := dial(t, url), dial(t, url)
	for _, c := range []*websocket.Conn{a, b} {
		if f := readFrame(t, c); f.Snapshot != nil {
			t.Fatalf("empty hub sent a snapshot: %+v", f.Snapshot)
		}
		if f := readFrame(t, c); f.Status == nil || f.Status.Up {
			t.Fatalf("second frame should be the (down) feed status, got %+v", f)
		}
	}
	waitClients(t, h, 2)
	h.Publish(Quote{Coin: "BTC", Bid: "81764.0", Ask: "81765.0", ExchMillis: 1, RecvNanos: 2})
	qa, qb := readQuotes(t, a), readQuotes(t, b)
	want := Quote{Coin: "BTC", Bid: "81764.0", Ask: "81765.0", ExchMillis: 1, RecvNanos: 2, Seq: 1}
	if len(qa) != 1 || qa[0] != want || len(qb) != 1 || qb[0] != want {
		t.Fatalf("a=%+v b=%+v, want both %+v", qa, qb, want)
	}
	// A subscriber can read its frame a moment before the hub's writer counts
	// the delivery, so the counters are awaited rather than read once.
	deadline := time.Now().Add(5 * time.Second)
	for st := h.Stats(); st.Published != 1 || st.Delivered != 2 || st.Fanned != 1; st = h.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("published=%d fanned=%d delivered=%d, want 1, 1 and 2", st.Published, st.Fanned, st.Delivered)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestANewSubscriberGetsTheLastRealPriceFirst(t *testing.T) {
	h := New(DefaultConfig())
	h.Publish(Quote{Coin: "ETH", Bid: "2468.9", Ask: "2469.0", ExchMillis: 7})
	url, _ := serveHub(t, h, nil)
	c := dial(t, url)
	f := readFrame(t, c)
	if len(f.Snapshot) != 1 || f.Snapshot[0].Bid != "2468.9" || f.Snapshot[0].ExchMillis != 7 {
		t.Fatalf("snapshot = %+v", f.Snapshot)
	}
}

func TestConflationKeepsOnlyTheLatestPerSymbol(t *testing.T) {
	h := New(DefaultConfig())
	c := newClient(h, nil, "test")
	c.counters = &classCounters{drops: map[DropReason]uint64{}}
	now := time.Now()
	enc := func(q Quote) []byte {
		b, err := json.Marshal(q)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	offer := func(q Quote) { c.offer([]string{q.Coin}, [][]byte{enc(q)}, now) }
	offer(Quote{Coin: "BTC", Bid: "1", Seq: 1})
	offer(Quote{Coin: "ETH", Bid: "2", Seq: 1})
	offer(Quote{Coin: "BTC", Bid: "3", Seq: 2})
	offer(Quote{Coin: "BTC", Bid: "4", Seq: 3})
	frames, n := c.take()
	if n != 2 || len(frames) != 1 {
		t.Fatalf("take: %d quotes in %d frames, want 2 in 1", n, len(frames))
	}
	var f Frame
	if err := json.Unmarshal(frames[0], &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Quotes) != 2 || f.Quotes[0].Coin != "BTC" || f.Quotes[0].Bid != "4" || f.Quotes[0].Seq != 3 || f.Quotes[1].Coin != "ETH" {
		t.Fatalf("frame = %+v, want BTC@4 (seq 3) then ETH", f.Quotes)
	}
	if got := c.counters.conflated.Load(); got != 2 {
		t.Fatalf("conflated = %d, want 2", got)
	}
	if frames, n := c.take(); n != 0 || len(frames) != 0 {
		t.Fatalf("mailbox not empty after take: %d", n)
	}
}

func TestASubscriberStuckPastStuckAfterIsDropped(t *testing.T) {
	cfg := DefaultConfig()
	h := New(cfg)
	c := newClient(h, nil, "test")
	c.counters = &classCounters{drops: map[DropReason]uint64{}}
	t0 := time.Now()
	one := [][]byte{[]byte(`{}`)}
	btc := []string{"BTC"}
	c.offer(btc, one, t0)
	c.offer(btc, one, t0.Add(cfg.StuckAfter))
	select {
	case <-c.done:
		t.Fatal("dropped at exactly StuckAfter")
	default:
	}
	c.offer(btc, one, t0.Add(cfg.StuckAfter+time.Millisecond))
	select {
	case <-c.done:
	default:
		t.Fatal("not dropped after StuckAfter")
	}
	if c.reason != DropStuck || c.counters.drops[DropStuck] != 1 {
		t.Fatalf("reason=%q drops=%v", c.reason, c.counters.drops)
	}
}

// tinyBuffers shrinks every accepted socket's send buffer so a subscriber that
// stops reading backs the hub's writes up within a few kilobytes.
type tinyBuffers struct{ net.Listener }

func (l tinyBuffers) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok && err == nil {
		_ = tc.SetWriteBuffer(4096)
	}
	return c, err
}

func TestASlowSubscriberIsDroppedWithoutStallingTheOthers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WriteTimeout = 300 * time.Millisecond
	h := New(cfg)
	url, reasons := serveHub(t, h, func(l net.Listener) net.Listener { return tinyBuffers{l} })

	// The slow one: a tiny receive buffer and it never reads.
	d := websocket.Dialer{NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if tc, ok := c.(*net.TCPConn); ok && err == nil {
			_ = tc.SetReadBuffer(4096)
		}
		return c, err
	}}
	slow, resp, err := d.Dial(url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slow.Close() }()

	fast := dial(t, url)
	var got sync.WaitGroup
	got.Add(1)
	big := strings.Repeat("9", 4000)
	go func() {
		defer got.Done()
		seen := 0
		for seen < 400 {
			f := readFrame(t, fast)
			seen += len(f.Quotes)
		}
	}()
	waitClients(t, h, 2)
	for i := 0; i < 400; i++ {
		// Distinct symbols, so nothing conflates and the bytes pile up.
		h.Publish(Quote{Coin: "C" + strings.Repeat("x", i%50) + big[:1] + string(rune('a'+i%26)) + big[:i%7], Bid: big, Ask: "1"})
		time.Sleep(time.Millisecond)
	}
	select {
	case r := <-reasons:
		if r != DropWriteTimeout {
			t.Fatalf("slow subscriber dropped for %q, want %q", r, DropWriteTimeout)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("slow subscriber was never dropped")
	}
	got.Wait()
	if st := h.Stats(); st.Drops[string(DropWriteTimeout)] != 1 {
		t.Fatalf("drops = %v", st.Drops)
	}
}

func TestTheCapRefusesWithAReason(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxClients = 1
	h := New(cfg)
	url, _ := serveHub(t, h, nil)
	first := dial(t, url)
	readFrame(t, first)
	waitClients(t, h, 1)
	second := dial(t, url)
	if f := readFrame(t, second); f.Bye != ByeFull {
		t.Fatalf("second subscriber got %+v, want bye %q", f, ByeFull)
	}
	if h.Stats().Clients != 1 {
		t.Fatal("cap exceeded")
	}
}

func TestPingIsEchoedAsPong(t *testing.T) {
	h := New(DefaultConfig())
	url, _ := serveHub(t, h, nil)
	c := dial(t, url)
	readFrame(t, c)
	readFrame(t, c)
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"ping":12345}`)); err != nil {
		t.Fatal(err)
	}
	if f := readFrame(t, c); string(f.Pong) != "12345" {
		t.Fatalf("pong = %q", f.Pong)
	}
}

func TestParseBBOPassesPricesThroughOrSkips(t *testing.T) {
	good := `{"channel":"bbo","data":{"coin":"BTC","time":1791491290820,"bbo":[{"px":"81764.0","sz":"0.01529","n":1},{"px":"81765.0","sz":"47.2","n":118}]}}`
	q, ok := ParseBBO([]byte(good), 99)
	if !ok || q != (Quote{Coin: "BTC", Bid: "81764.0", Ask: "81765.0", ExchMillis: 1791491290820, RecvNanos: 99}) {
		t.Fatalf("good: %+v %v", q, ok)
	}
	for name, msg := range map[string]string{
		"empty bid side":   `{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[null,{"px":"1.0"}]}}`,
		"exponent":         `{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"1e5"},{"px":"1.0"}]}}`,
		"negative":         `{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"-1"},{"px":"1.0"}]}}`,
		"empty price":      `{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":""},{"px":"1.0"}]}}`,
		"other channel":    `{"channel":"pong"}`,
		"one side":         `{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"1.0"}]}}`,
		"no coin":          `{"channel":"bbo","data":{"time":1,"bbo":[{"px":"1.0"},{"px":"1.0"}]}}`,
		"not json":         `bbo`,
		"number not text":  `{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":1.5},{"px":"1.0"}]}}`,
		"subscription ack": `{"channel":"subscriptionResponse","data":{"method":"subscribe"}}`,
	} {
		if q, ok := ParseBBO([]byte(msg), 1); ok {
			t.Errorf("%s: accepted %+v", name, q)
		}
	}
}

// A new subscriber must be able to register while a fanout is in progress.
// The first version held the hub-wide read lock for the whole fanout, so at
// 100k subscribers registrations and publishes took turns and most upgraded
// sockets were never registered (measured in the PF-S443 scale runs). Here one
// subscriber's mailbox is held locked, so a fanout that reaches it stalls
// there; registration must still go through.
func TestRegistrationIsNotBlockedByAFanoutInProgress(t *testing.T) {
	h := New(DefaultConfig())
	victim := newClient(h, nil, "test")
	if err := h.register(victim); err != nil {
		t.Fatal(err)
	}
	victim.mu.Lock()
	published := make(chan struct{})
	go func() {
		h.Publish(Quote{Coin: "BTC", Bid: "1.0", Ask: "2.0"})
		close(published)
	}()
	// Wait until the fanout is parked on the victim's mailbox.
	deadline := time.Now().Add(2 * time.Second)
	for h.published.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Publish never started")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	registered := make(chan error, 1)
	go func() { registered <- h.register(newClient(h, nil, "test")) }()
	select {
	case err := <-registered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		victim.mu.Unlock()
		<-published
		t.Fatal("registration waited for a fanout in progress")
	}
	victim.mu.Unlock()
	<-published
}

// The upstream reader must never wait for a fanout pass. Publish used to fan
// out inline, so at 100k subscribers the reader fell more than a second behind
// the feed and the delay it caused never showed in the measured latency
// (PF-S443). Here the pass is parked on one subscriber's locked mailbox, and
// Publish must still return at once — and a symbol that moves twice while the
// pass is parked must go out once, newest, with the older one counted.
func TestPublishNeverWaitsForAFanoutAndKeepsTheNewest(t *testing.T) {
	h := New(DefaultConfig())
	defer h.Close()
	victim := newClient(h, nil, "test")
	observer := newClient(h, nil, "test")
	for _, c := range []*client{victim, observer} {
		if err := h.register(c); err != nil {
			t.Fatal(err)
		}
	}
	victim.mu.Lock()
	// Every Publish runs with a deadline, so a Publish that waits for the
	// parked pass fails the test instead of hanging it.
	publish := func(q Quote) {
		done := make(chan struct{})
		go func() { h.Publish(q); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			victim.mu.Unlock()
			t.Fatalf("Publish(%s %s) waited for a fanout pass", q.Coin, q.Bid)
		}
	}
	publish(Quote{Coin: "BTC", Bid: "1.0", Ask: "2.0"})
	// Wait until the first pass has taken BTC#1 and is parked on the victim.
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.quoteMu.Lock()
		empty := len(h.inboxOrder) == 0
		h.quoteMu.Unlock()
		if empty {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the fanout loop never took the first quote")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	publish(Quote{Coin: "BTC", Bid: "1.5", Ask: "2.0"})
	publish(Quote{Coin: "BTC", Bid: "1.7", Ask: "2.0"})
	publish(Quote{Coin: "ETH", Bid: "3.0", Ask: "4.0"})
	if d := time.Since(start); d > 100*time.Millisecond {
		victim.mu.Unlock()
		t.Fatalf("three publishes took %s while a pass was parked; the reader waited for the fanout", d)
	}
	victim.mu.Unlock()

	deadline = time.Now().Add(2 * time.Second)
	for h.Stats().Fanned < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("stats: %+v", h.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	st := h.Stats()
	if st.Published != 4 || st.UpstreamConflated != 1 || st.Fanned != 3 {
		t.Fatalf("published=%d upstream_conflated=%d fanned=%d, want 4, 1 and 3", st.Published, st.UpstreamConflated, st.Fanned)
	}
	frames, _ := observer.take()
	var f Frame
	if err := json.Unmarshal(frames[len(frames)-1], &f); err != nil {
		t.Fatal(err)
	}
	for _, q := range f.Quotes {
		if q.Coin == "BTC" && (q.Bid != "1.7" || q.Seq != 3) {
			t.Fatalf("BTC went out as %+v, want the newest (1.7, seq 3)", q)
		}
	}
}
