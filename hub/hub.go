package hub

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Config bounds what one hub will do for its subscribers.
type Config struct {
	// MaxClients is the global connection cap; 0 means no cap.
	MaxClients int
	// WriteTimeout is the longest one write to a subscriber may take. A write
	// that takes longer drops the subscriber.
	WriteTimeout time.Duration
	// StuckAfter drops a subscriber whose prices have waited this long with
	// no write completed.
	StuckAfter time.Duration
	// PingEvery is the keepalive interval. It must be well under any proxy's
	// idle timeout (Sliplane's edge closes an idle WebSocket at 60 s).
	PingEvery time.Duration
	// PongWait is the read deadline, extended by every pong or message.
	PongWait time.Duration
	// ReadLimit caps one inbound message; subscribers only ever send pings.
	ReadLimit int64
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Default limits.
const (
	DefaultWriteTimeout = 5 * time.Second
	DefaultStuckAfter   = 10 * time.Second
	DefaultPingEvery    = 25 * time.Second
	DefaultPongWait     = 60 * time.Second
	DefaultReadLimit    = 512
	byeWriteTimeout     = time.Second
)

// DefaultConfig returns the limits the lab runs with.
func DefaultConfig() Config {
	return Config{
		WriteTimeout: DefaultWriteTimeout,
		StuckAfter:   DefaultStuckAfter,
		PingEvery:    DefaultPingEvery,
		PongWait:     DefaultPongWait,
		ReadLimit:    DefaultReadLimit,
	}
}

// DropReason is why the hub dropped a subscriber.
type DropReason string

// The reasons a subscriber is dropped. A subscriber that closes its own
// connection is not dropped and is not counted.
const (
	DropWriteTimeout DropReason = "write timed out"
	DropWriteError   DropReason = "write failed"
	DropStuck        DropReason = "stuck: prices waited with nothing written"
	DropShutdown     DropReason = "hub closed"
)

// ErrFull is returned by Serve when the hub is at MaxClients.
var ErrFull = errors.New("hub: at its connection cap")

// ErrClosed is returned by Serve after Close.
var ErrClosed = errors.New("hub: closed")

// ByeFull is the reason sent to a subscriber refused at the cap.
const ByeFull = "hub full: at its connection cap"

// ClassStats are the counters for one class of subscriber (for example the
// lab's own visitors versus simulated load-test subscribers).
type ClassStats struct {
	Clients   int               `json:"clients"`
	Offered   uint64            `json:"offered"`
	Delivered uint64            `json:"delivered"`
	Conflated uint64            `json:"conflated"`
	Drops     map[string]uint64 `json:"drops"`
}

// Stats is a point-in-time copy of the hub's counters.
type Stats struct {
	Clients     int    `json:"clients"`
	PeakClients int    `json:"peak_clients"`
	Published   uint64 `json:"published"`
	// Fanned counts quotes that went out to subscribers; UpstreamConflated
	// counts quotes replaced in the hub's own inbox by a newer price for the
	// same symbol before a fanout pass reached them. Published = Fanned +
	// UpstreamConflated, give or take one pass in progress.
	Fanned            uint64                `json:"fanned"`
	UpstreamConflated uint64                `json:"upstream_conflated"`
	FanoutNanos       uint64                `json:"fanout_nanos"`
	Offered           uint64                `json:"offered"`
	Delivered         uint64                `json:"delivered"`
	Frames            uint64                `json:"frames"`
	Conflated         uint64                `json:"conflated"`
	Drops             map[string]uint64     `json:"drops"`
	Classes           map[string]ClassStats `json:"classes"`
	Feed              FeedStatus            `json:"feed"`
	Last              []Quote               `json:"last"`
}

type classCounters struct {
	clients   int
	offered   atomic.Uint64
	delivered atomic.Uint64
	conflated atomic.Uint64
	dropsMu   sync.Mutex
	drops     map[DropReason]uint64
}

// Hub fans quotes out to subscribers.
type Hub struct {
	cfg Config

	mu      sync.RWMutex
	clients map[*client]struct{}
	classes map[string]*classCounters
	peak    int
	closed  bool

	quoteMu    sync.Mutex
	last       map[string]Quote
	seq        map[string]uint64
	status     FeedStatus
	inbox      map[string]Quote // newest unfanned quote per symbol
	inboxOrder []string

	fanNotify chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
	fanBuf    []*client // the fanout loop's own; never shared

	published   atomic.Uint64
	fanned      atomic.Uint64
	upConflated atomic.Uint64
	fanoutNs    atomic.Uint64
	frames      atomic.Uint64
}

// New returns a hub with no subscribers and a feed that is not yet up.
func New(cfg Config) *Hub {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	h := &Hub{
		cfg:       cfg,
		clients:   map[*client]struct{}{},
		classes:   map[string]*classCounters{},
		last:      map[string]Quote{},
		seq:       map[string]uint64{},
		inbox:     map[string]Quote{},
		fanNotify: make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	h.status = FeedStatus{SinceMillis: cfg.Now().UnixMilli(), LastError: "connecting"}
	go h.fanoutLoop()
	return h
}

func (h *Hub) now() time.Time { return h.cfg.Now() }

// Publish stamps q with its per-symbol sequence number and puts it in the
// hub's inbox, replacing an unfanned quote for the same symbol. It returns at
// once: the upstream reader is never held up by a fanout.
//
// WHY THE INBOX EXISTS (measured, PF-S443). Publish used to fan out inline, so
// the upstream reader waited for a full pass over every subscriber before
// reading the next message. At 100k subscribers a pass took over a second, the
// feed's messages queued in the socket, and they were stamped only when
// finally read — so the delivery latency left out exactly the wait it should
// have shown. Now the reader stamps on arrival and the wait in the inbox is
// inside the measured latency, and a symbol that moved twice during one pass
// is sent once, newest first, with the older one counted as replaced.
func (h *Hub) Publish(q Quote) {
	h.quoteMu.Lock()
	h.seq[q.Coin]++
	q.Seq = h.seq[q.Coin]
	if _, waiting := h.inbox[q.Coin]; waiting {
		h.upConflated.Add(1)
	} else {
		h.inboxOrder = append(h.inboxOrder, q.Coin)
	}
	h.inbox[q.Coin] = q
	h.quoteMu.Unlock()
	h.published.Add(1)
	select {
	case h.fanNotify <- struct{}{}:
	default:
	}
}

// fanoutLoop takes whatever is in the inbox and offers it to every subscriber,
// one pass at a time, until Close.
func (h *Hub) fanoutLoop() {
	for {
		select {
		case <-h.stop:
			return
		case <-h.fanNotify:
		}
		h.quoteMu.Lock()
		batch := make([]Quote, 0, len(h.inboxOrder))
		for _, coin := range h.inboxOrder {
			batch = append(batch, h.inbox[coin])
			delete(h.inbox, coin)
		}
		h.inboxOrder = h.inboxOrder[:0]
		h.quoteMu.Unlock()
		if len(batch) > 0 {
			h.fanout(batch)
		}
	}
}

// fanout offers one batch (at most one quote per symbol) to every subscriber.
//
// It copies the subscriber list under the read lock and offers after
// releasing it. Holding the lock for the whole pass made every new
// subscriber's registration (which needs the write lock) wait for a full pass
// over everyone else — at 100k subscribers, registrations and passes took
// turns one at a time and most upgraded sockets were never registered at all
// (measured, PF-S443). A subscriber removed after the copy is offered one
// batch it never reads, which costs nothing.
func (h *Hub) fanout(batch []Quote) {
	coins := make([]string, len(batch))
	raws := make([][]byte, len(batch))
	for i, q := range batch {
		raw, err := json.Marshal(q)
		if err != nil {
			return // a Quote of strings and integers always marshals
		}
		coins[i], raws[i] = q.Coin, raw
	}
	h.quoteMu.Lock()
	for _, q := range batch {
		h.last[q.Coin] = q
	}
	h.quoteMu.Unlock()

	now := h.now()
	h.mu.RLock()
	h.fanBuf = h.fanBuf[:0]
	for c := range h.clients {
		h.fanBuf = append(h.fanBuf, c)
	}
	h.mu.RUnlock()
	for i, c := range h.fanBuf {
		c.offer(coins, raws, now)
		h.fanBuf[i] = nil
	}
	h.fanned.Add(uint64(len(batch)))
	h.fanoutNs.Add(uint64(time.Since(now).Nanoseconds()))
}

// SetStatus records the upstream's state and tells every subscriber.
func (h *Hub) SetStatus(st FeedStatus) {
	h.quoteMu.Lock()
	h.status = st
	h.quoteMu.Unlock()
	h.mu.RLock()
	for c := range h.clients {
		c.setStatus(st)
	}
	h.mu.RUnlock()
}

// Status returns the upstream's last recorded state.
func (h *Hub) Status() FeedStatus {
	h.quoteMu.Lock()
	defer h.quoteMu.Unlock()
	return h.status
}

// Last returns the last quote received per symbol, sorted by symbol.
func (h *Hub) Last() []Quote {
	h.quoteMu.Lock()
	out := make([]Quote, 0, len(h.last))
	for _, q := range h.last {
		out = append(out, q)
	}
	h.quoteMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Coin < out[j].Coin })
	return out
}

// Stats returns a copy of the hub's counters.
func (h *Hub) Stats() Stats {
	st := Stats{
		Published:         h.published.Load(),
		Fanned:            h.fanned.Load(),
		UpstreamConflated: h.upConflated.Load(),
		FanoutNanos:       h.fanoutNs.Load(),
		Frames:            h.frames.Load(),
		Drops:             map[string]uint64{},
		Classes:           map[string]ClassStats{},
		Feed:              h.Status(),
		Last:              h.Last(),
	}
	h.mu.RLock()
	st.Clients = len(h.clients)
	st.PeakClients = h.peak
	for name, cc := range h.classes {
		cs := ClassStats{
			Clients:   cc.clients,
			Offered:   cc.offered.Load(),
			Delivered: cc.delivered.Load(),
			Conflated: cc.conflated.Load(),
			Drops:     map[string]uint64{},
		}
		cc.dropsMu.Lock()
		for r, n := range cc.drops {
			cs.Drops[string(r)] = n
			st.Drops[string(r)] += n
		}
		cc.dropsMu.Unlock()
		st.Offered += cs.Offered
		st.Delivered += cs.Delivered
		st.Conflated += cs.Conflated
		st.Classes[name] = cs
	}
	h.mu.RUnlock()
	return st
}

// Close drops every subscriber and refuses new ones.
func (h *Hub) Close() {
	h.stopOnce.Do(func() { close(h.stop) })
	h.mu.Lock()
	h.closed = true
	all := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		all = append(all, c)
	}
	h.mu.Unlock()
	for _, c := range all {
		c.drop(DropShutdown)
	}
}

func (h *Hub) register(c *client) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	if h.cfg.MaxClients > 0 && len(h.clients) >= h.cfg.MaxClients {
		return ErrFull
	}
	h.clients[c] = struct{}{}
	cc := h.classes[c.class]
	if cc == nil {
		cc = &classCounters{drops: map[DropReason]uint64{}}
		h.classes[c.class] = cc
	}
	cc.clients++
	c.counters = cc
	if len(h.clients) > h.peak {
		h.peak = len(h.clients)
	}
	return nil
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	c.counters.clients--
	h.mu.Unlock()
}

// Serve runs one subscriber on an upgraded connection until it closes or is
// dropped, and returns the drop reason ("" when the subscriber left on its
// own). class groups subscribers in Stats. Serve always closes conn.
func (h *Hub) Serve(conn *websocket.Conn, class string) (DropReason, error) {
	c := newClient(h, conn, class)
	if err := h.register(c); err != nil {
		if errors.Is(err, ErrFull) {
			c.writeBye(ByeFull)
		}
		closeConn(conn)
		return "", err
	}
	defer h.unregister(c)

	// The first frames: the last real price per symbol, then the feed's state.
	h.quoteMu.Lock()
	st := h.status
	h.quoteMu.Unlock()
	if err := c.writeFrame(Frame{Snapshot: h.Last()}); err != nil {
		closeConn(conn)
		return "", nil // the subscriber left before its first frame; not a hub error
	}
	if err := c.writeFrame(Frame{Status: &st}); err != nil {
		closeConn(conn)
		return "", nil
	}

	go c.readLoop()
	c.writeLoop()
	closeConn(conn)
	<-c.readerDone
	return c.reason, nil
}

func closeConn(conn *websocket.Conn) {
	if err := conn.Close(); err != nil {
		return // already closed by the other side; nothing to report
	}
}
