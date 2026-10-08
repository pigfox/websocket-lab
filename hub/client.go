package hub

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// client is one subscriber. Its mailbox holds at most one unsent quote per
// symbol; that bound IS the send buffer, so a slow subscriber costs the hub a
// fixed amount of memory however far behind it falls.
type client struct {
	h        *Hub
	conn     *websocket.Conn
	class    string
	counters *classCounters

	mu           sync.Mutex
	pending      map[string][]byte // symbol -> latest unsent quote, encoded
	order        []string          // symbols in the order they became pending
	pendingSince time.Time         // when the oldest unsent quote arrived
	status       *FeedStatus
	pong         json.RawMessage

	notify     chan struct{}
	done       chan struct{}
	readerDone chan struct{}
	dropOnce   sync.Once
	reason     DropReason
	peerGone   atomic.Bool
}

func newClient(h *Hub, conn *websocket.Conn, class string) *client {
	return &client{
		h:          h,
		conn:       conn,
		class:      class,
		pending:    map[string][]byte{},
		notify:     make(chan struct{}, 1),
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
	}
}

func (c *client) signal() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// offer puts a batch of quotes in the mailbox, each replacing an unsent one
// for the same symbol (that replacement is a conflation). It never blocks on
// the network.
func (c *client) offer(coins []string, raws [][]byte, now time.Time) {
	c.counters.offered.Add(uint64(len(coins)))
	c.mu.Lock()
	for i, coin := range coins {
		if _, waiting := c.pending[coin]; waiting {
			c.counters.conflated.Add(1)
		} else {
			c.order = append(c.order, coin)
		}
		c.pending[coin] = raws[i]
	}
	if c.pendingSince.IsZero() {
		c.pendingSince = now
	}
	stuck := now.Sub(c.pendingSince) > c.h.cfg.StuckAfter
	c.mu.Unlock()
	if stuck {
		c.drop(DropStuck)
		return
	}
	c.signal()
}

func (c *client) setStatus(st FeedStatus) {
	c.mu.Lock()
	c.status = &st
	c.mu.Unlock()
	c.signal()
}

func (c *client) setPong(token json.RawMessage) {
	c.mu.Lock()
	c.pong = token
	c.mu.Unlock()
	c.signal()
}

// take empties the mailbox into the frames to write, in a fixed order:
// status, pong, then quotes.
func (c *client) take() (frames [][]byte, quotes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status != nil {
		if b, err := json.Marshal(Frame{Status: c.status}); err == nil {
			frames = append(frames, b)
		}
		c.status = nil
	}
	if c.pong != nil {
		if b, err := json.Marshal(Frame{Pong: c.pong}); err == nil {
			frames = append(frames, b)
		}
		c.pong = nil
	}
	if len(c.order) > 0 {
		var buf bytes.Buffer
		buf.WriteString(`{"q":[`)
		for i, coin := range c.order {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.Write(c.pending[coin])
			delete(c.pending, coin)
		}
		buf.WriteString(`]}`)
		frames = append(frames, buf.Bytes())
		quotes = len(c.order)
		c.order = c.order[:0]
		c.pendingSince = time.Time{}
	}
	return frames, quotes
}

// drop ends the subscriber for reason, once. The write loop sends the reason
// as a Bye frame if it can, then closes the connection.
func (c *client) drop(reason DropReason) {
	c.dropOnce.Do(func() {
		c.reason = reason
		if reason != "" && !c.peerGone.Load() {
			c.counters.dropsMu.Lock()
			c.counters.drops[reason]++
			c.counters.dropsMu.Unlock()
		}
		close(c.done)
	})
}

func (c *client) write(b []byte) error {
	if err := c.conn.SetWriteDeadline(c.h.now().Add(c.h.cfg.WriteTimeout)); err != nil {
		return err
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return err
	}
	c.h.frames.Add(1)
	return nil
}

func (c *client) writeFrame(f Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return c.write(b)
}

func (c *client) writeBye(reason string) {
	b, err := json.Marshal(Frame{Bye: reason})
	if err != nil {
		return
	}
	if c.conn.SetWriteDeadline(c.h.now().Add(byeWriteTimeout)) != nil {
		return
	}
	if c.conn.WriteMessage(websocket.TextMessage, b) != nil {
		return
	}
	msg := websocket.FormatCloseMessage(websocket.ClosePolicyViolation, reason)
	if c.conn.WriteControl(websocket.CloseMessage, msg, c.h.now().Add(byeWriteTimeout)) != nil {
		return
	}
}

// failWrite turns a write error into a drop reason.
func (c *client) failWrite(err error) {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		c.drop(DropWriteTimeout)
		return
	}
	c.drop(DropWriteError)
}

func (c *client) writeLoop() {
	ping := time.NewTicker(c.h.cfg.PingEvery)
	defer ping.Stop()
	for {
		select {
		case <-c.done:
			if c.reason != "" && c.reason != DropWriteTimeout && c.reason != DropWriteError {
				c.writeBye(string(c.reason))
			}
			return
		case <-ping.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, c.h.now().Add(c.h.cfg.WriteTimeout)); err != nil {
				c.failWrite(err)
				return
			}
		case <-c.notify:
			frames, quotes := c.take()
			for _, f := range frames {
				if err := c.write(f); err != nil {
					c.failWrite(err)
					return
				}
			}
			c.counters.delivered.Add(uint64(quotes))
		}
	}
}

func (c *client) readLoop() {
	defer close(c.readerDone)
	c.conn.SetReadLimit(c.h.cfg.ReadLimit)
	extend := func() error { return c.conn.SetReadDeadline(c.h.now().Add(c.h.cfg.PongWait)) }
	if extend() != nil {
		c.peerGone.Store(true)
		c.drop("")
		return
	}
	c.conn.SetPongHandler(func(string) error { return extend() })
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			// The subscriber went away (or its connection was closed by a
			// drop). Either way the reader is finished; a peer leaving is not
			// counted as a drop.
			c.peerGone.Store(true)
			c.drop("")
			return
		}
		if extend() != nil {
			c.peerGone.Store(true)
			c.drop("")
			return
		}
		var in struct {
			Ping json.RawMessage `json:"ping"`
		}
		if json.Unmarshal(msg, &in) == nil && len(in.Ping) > 0 && len(in.Ping) <= maxPingToken {
			c.setPong(in.Ping)
		}
	}
}

// maxPingToken bounds the token a subscriber may ask to have echoed.
const maxPingToken = 32
