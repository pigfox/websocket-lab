// Command hub runs the fanout hub: one upstream connection to Hyperliquid's
// public market feed, fanned out to every subscriber on /ws.
//
//	HUB_ADDRS        listen addresses, comma separated (default ":8080")
//	HUB_MAX_CLIENTS  global connection cap (default 0: none)
//	HUB_COINS        symbols (default "BTC,ETH,SOL")
//
// GET /stats returns the hub's counters plus its own goroutine and memory
// figures, which the load generator samples to record the hub's peaks.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pigfox/websocket-lab/hub"
)

// StatsResponse is what /stats returns.
type StatsResponse struct {
	Hub        hub.Stats `json:"hub"`
	Goroutines int       `json:"goroutines"`
	HeapMiB    float64   `json:"heap_mib"`
	SysMiB     float64   `json:"sys_mib"`
	CPUSeconds float64   `json:"cpu_seconds"`
}

const (
	mib               = 1 << 20
	readHeaderTimeout = 10 * time.Second
	shutdownWait      = 5 * time.Second
)

func main() {
	addrs := strings.Split(env("HUB_ADDRS", ":8080"), ",")
	maxClients, err := strconv.Atoi(env("HUB_MAX_CLIENTS", "0"))
	if err != nil {
		log.Fatalf("HUB_MAX_CLIENTS: %v", err)
	}
	cfg := hub.DefaultConfig()
	cfg.MaxClients = maxClients
	h := hub.New(cfg)

	up := hub.NewUpstream()
	up.Coins = strings.Split(env("HUB_COINS", strings.Join(hub.DefaultCoins, ",")), ",")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go up.Run(ctx, h)

	upgrader := hub.NewUpgrader(func(*http.Request) bool { return true })
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return // Upgrade has already written the error response
		}
		if _, err := h.Serve(conn, "loadgen"); err != nil && err != hub.ErrFull {
			log.Printf("serve: %v", err)
		}
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		var ru syscall.Rusage
		cpu := 0.0
		if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
			cpu = time.Duration(ru.Utime.Nano() + ru.Stime.Nano()).Seconds()
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(StatsResponse{
			Hub:        h.Stats(),
			Goroutines: runtime.NumGoroutine(),
			HeapMiB:    float64(ms.HeapAlloc) / mib,
			SysMiB:     float64(ms.Sys) / mib,
			CPUSeconds: cpu,
		}); err != nil {
			log.Printf("stats: %v", err)
		}
	})

	servers := make([]*http.Server, 0, len(addrs))
	for _, a := range addrs {
		srv := &http.Server{Addr: strings.TrimSpace(a), Handler: mux, ReadHeaderTimeout: readHeaderTimeout}
		servers = append(servers, srv)
		go func() {
			log.Printf("hub listening on %s", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("listen %s: %v", srv.Addr, err)
			}
		}()
	}
	<-ctx.Done()
	h.Close()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownWait)
	defer cancel()
	for _, s := range servers {
		if err := s.Shutdown(sctx); err != nil {
			log.Printf("shutdown %s: %v", s.Addr, err)
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
