package hub

import (
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// Buffer sizes. Frames are a few hundred bytes, subscribers send only pings,
// and the write buffer is pooled, so an idle subscriber holds no write buffer
// at all — which is most of what makes 100k subscribers fit.
const (
	readBufferSize  = 256
	writeBufferSize = 2048
)

var writePool sync.Pool

// NewUpgrader returns the upgrader the hub expects. checkOrigin decides which
// pages may connect; nil allows only same-origin requests (gorilla's default).
func NewUpgrader(checkOrigin func(*http.Request) bool) *websocket.Upgrader {
	return &websocket.Upgrader{
		ReadBufferSize:  readBufferSize,
		WriteBufferSize: writeBufferSize,
		WriteBufferPool: &writePool,
		CheckOrigin:     checkOrigin,
	}
}
