// Package hub holds one upstream market-data connection and fans its prices
// out over WebSocket to any number of subscribers.
//
// The shape is one upstream, many downstream. A subscriber that cannot keep up
// is never allowed to slow the others: each one has a mailbox holding at most
// one unsent price per symbol, so a newer price for BTC replaces an older
// unsent one (conflation) instead of queueing behind it. A subscriber that
// stays stuck — a write that times out, or nothing written for StuckAfter
// while prices wait — is dropped, and the reason is counted.
//
// Prices are never made up. A Quote is built in exactly one place, from the
// upstream message's own price strings, and those strings are passed through
// untouched; nothing here parses them to a number or formats one back.
package hub

import "encoding/json"

// Quote is one best bid and best offer for one symbol, exactly as the
// upstream sent it, plus two stamps the hub adds.
type Quote struct {
	// Coin is the symbol, e.g. "BTC".
	Coin string `json:"c"`
	// Bid and Ask are the upstream's price strings, byte for byte.
	Bid string `json:"b"`
	Ask string `json:"a"`
	// ExchMillis is the upstream's own timestamp for the quote (Unix ms).
	ExchMillis int64 `json:"x"`
	// RecvNanos is when the hub read the quote off the upstream socket
	// (Unix ns). A subscriber on the same machine measures delivery latency
	// as its own receive time minus this.
	RecvNanos int64 `json:"r"`
	// Seq counts quotes per symbol as the hub receives them, so a subscriber
	// can see how many it never got because they were conflated away.
	Seq uint64 `json:"s"`
}

// FeedStatus is the upstream connection's state as subscribers see it.
type FeedStatus struct {
	Up bool `json:"up"`
	// SinceMillis is when Up last changed (Unix ms).
	SinceMillis int64 `json:"since"`
	// LastError is the reason the upstream last went down, if it has.
	LastError string `json:"err,omitempty"`
	// Reconnects counts successful reconnections after a loss.
	Reconnects uint64 `json:"reconnects"`
}

// Frame is everything the hub ever writes to a subscriber. Exactly one field
// is set per frame.
type Frame struct {
	// Quotes are new prices, at most one per symbol.
	Quotes []Quote `json:"q,omitempty"`
	// Snapshot is the last known price per symbol, sent once on connect. Its
	// quotes may be old; a subscriber measuring latency must skip them.
	Snapshot []Quote `json:"snap,omitempty"`
	// Status is the upstream's state, sent on connect and whenever it changes.
	Status *FeedStatus `json:"st,omitempty"`
	// Pong answers a subscriber's {"ping":<token>} with the same token, so a
	// browser can measure its own round trip without trusting either clock.
	Pong json.RawMessage `json:"pong,omitempty"`
	// Bye is the reason the hub is about to drop this subscriber.
	Bye string `json:"bye,omitempty"`
}
