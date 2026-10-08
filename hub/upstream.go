package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// HyperliquidURL is the public market-data WebSocket the lab reads. No key and
// no account; its documented limits are 10 connections and 1000 subscriptions
// per IP, and the hub uses one connection and one subscription per symbol.
const HyperliquidURL = "wss://api.hyperliquid.xyz/ws"

// DefaultCoins are the symbols the lab subscribes to.
var DefaultCoins = []string{"BTC", "ETH", "SOL"}

// Upstream holds the one connection to the market feed and reconnects it with
// exponential backoff. While it is down the hub keeps the last real quote per
// symbol, with its timestamp, and reports the feed as down since the moment it
// was lost. It never fills the gap with anything.
type Upstream struct {
	URL   string
	Coins []string
	// Dialer defaults to websocket.DefaultDialer.
	Dialer *websocket.Dialer
	// PingEvery is how often the feed is pinged ({"method":"ping"}); the feed
	// closes a connection that sends nothing for 60 s.
	PingEvery time.Duration
	// ReadTimeout reconnects when no message at all arrives for this long.
	ReadTimeout time.Duration
	// MinBackoff and MaxBackoff bound the wait between reconnect attempts.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Upstream defaults.
const (
	DefaultUpstreamPing   = 30 * time.Second
	DefaultUpstreamRead   = 60 * time.Second
	DefaultMinBackoff     = time.Second
	DefaultMaxBackoff     = time.Minute
	upstreamWriteDeadline = 10 * time.Second
)

// NewUpstream returns the lab's upstream with its defaults.
func NewUpstream() *Upstream {
	return &Upstream{
		URL:         HyperliquidURL,
		Coins:       DefaultCoins,
		PingEvery:   DefaultUpstreamPing,
		ReadTimeout: DefaultUpstreamRead,
		MinBackoff:  DefaultMinBackoff,
		MaxBackoff:  DefaultMaxBackoff,
	}
}

func (u *Upstream) now() time.Time {
	if u.Now != nil {
		return u.Now()
	}
	return time.Now()
}

// Run connects, publishes every quote to h, and reconnects until ctx ends.
func (u *Upstream) Run(ctx context.Context, h *Hub) {
	backoff := u.MinBackoff
	var reconnects uint64
	everUp := false
	down := h.Status()
	for ctx.Err() == nil {
		got, err := u.session(ctx, h, func() {
			if everUp {
				reconnects++
			}
			everUp = true
			h.SetStatus(FeedStatus{Up: true, SinceMillis: u.now().UnixMilli(), Reconnects: reconnects})
		})
		if ctx.Err() != nil {
			return
		}
		msg := "connection closed"
		if err != nil {
			msg = err.Error()
		}
		// Down since the moment it was lost; a failed reconnect keeps the
		// original time, so the page can say how long the feed has been out.
		st := h.Status()
		if st.Up {
			down = FeedStatus{Up: false, SinceMillis: u.now().UnixMilli(), LastError: msg, Reconnects: reconnects}
		} else {
			down.LastError = msg
			down.Reconnects = reconnects
		}
		h.SetStatus(down)
		if got {
			backoff = u.MinBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > u.MaxBackoff {
			backoff = u.MaxBackoff
		}
	}
}

// session runs one connection. It reports whether any quote arrived.
func (u *Upstream) session(ctx context.Context, h *Hub, onUp func()) (bool, error) {
	d := u.Dialer
	if d == nil {
		d = websocket.DefaultDialer
	}
	conn, resp, err := d.DialContext(ctx, u.URL, nil)
	if resp != nil && resp.Body != nil {
		if cerr := resp.Body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { closeConn(conn) })
	defer stop()
	defer closeConn(conn)

	var wmu sync.Mutex
	send := func(v any) error {
		wmu.Lock()
		defer wmu.Unlock()
		if err := conn.SetWriteDeadline(u.now().Add(upstreamWriteDeadline)); err != nil {
			return err
		}
		return conn.WriteJSON(v)
	}
	for _, coin := range u.Coins {
		sub := map[string]any{"method": "subscribe", "subscription": map[string]string{"type": "bbo", "coin": coin}}
		if err := send(sub); err != nil {
			return false, fmt.Errorf("subscribe %s: %w", coin, err)
		}
	}
	onUp()

	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		t := time.NewTicker(u.PingEvery)
		defer t.Stop()
		for {
			select {
			case <-pingDone:
				return
			case <-t.C:
				if send(map[string]string{"method": "ping"}) != nil {
					closeConn(conn)
					return
				}
			}
		}
	}()

	wanted := map[string]bool{}
	for _, c := range u.Coins {
		wanted[c] = true
	}
	got := false
	for {
		if err := conn.SetReadDeadline(u.now().Add(u.ReadTimeout)); err != nil {
			return got, err
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return got, fmt.Errorf("read: %w", err)
		}
		recv := u.now().UnixNano()
		if q, ok := ParseBBO(msg, recv); ok && wanted[q.Coin] {
			got = true
			h.Publish(q)
		}
	}
}

// pricePattern is what an upstream price string must look like to be shown:
// plain decimal digits, as the feed sends them.
var pricePattern = regexp.MustCompile(`^[0-9]{1,15}(\.[0-9]{1,15})?$`)

// ParseBBO turns one upstream "bbo" message into a Quote. It is the only place
// a Quote's prices are set, and it sets them to the upstream's own strings.
// A message missing either side, or with a price that is not a plain decimal,
// is skipped rather than repaired.
func ParseBBO(msg []byte, recvNanos int64) (Quote, bool) {
	var m struct {
		Channel string `json:"channel"`
		Data    struct {
			Coin string `json:"coin"`
			Time int64  `json:"time"`
			BBO  []*struct {
				Px string `json:"px"`
			} `json:"bbo"`
		} `json:"data"`
	}
	if json.Unmarshal(msg, &m) != nil || m.Channel != "bbo" || len(m.Data.BBO) != 2 {
		return Quote{}, false
	}
	bid, ask := m.Data.BBO[0], m.Data.BBO[1]
	if bid == nil || ask == nil || !pricePattern.MatchString(bid.Px) || !pricePattern.MatchString(ask.Px) || m.Data.Coin == "" {
		return Quote{}, false
	}
	return Quote{
		Coin:       m.Data.Coin,
		Bid:        bid.Px,
		Ask:        ask.Px,
		ExchMillis: m.Data.Time,
		RecvNanos:  recvNanos,
	}, true
}
