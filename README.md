# websocket-lab

A WebSocket fanout hub in Go: one upstream market-data connection, many
subscribers, the newest price per symbol for a slow one instead of a queue, and
a load generator with committed runs at 10k, 50k and 100k subscribers.

The hub is what runs the live page at
**<https://pigfox.com/demos/websocket-lab>** — that site imports this module's
`hub`, `loadgen` and `results` packages rather than keeping a copy, and its
at-scale table is read from [`results/summary.json`](results/summary.json).

The prices are market data on display, used here to have something real to fan
out. Nothing in this repository is advice or a recommendation about any asset.

## Run it

You need Docker with Compose. Nothing else: no Go toolchain, no account, no key,
no `.env` file.

```sh
git clone https://github.com/pigfox/websocket-lab
cd websocket-lab
docker compose up -d hub                 # one connection to the feed, fanout on :8080
curl -s http://127.0.0.1:8080/stats      # counters; "feed":{"up":true,...} once connected
```

Run the load levels (each starts a fresh hub, waits for the feed, measures for
`DURATION`, and writes `results/raw/<subscribers>.json`):

```sh
./scripts/scale.sh 10000 50000 100000    # DURATION=60s by default
DURATION=30s ./scripts/scale.sh 1000     # a quick one
```

Then rebuild the summary and its table from whatever raw files are present.
The setup sentence and the limitations are the one hand-written part, in
[`results/notes.json`](results/notes.json); every figure comes from the raw
files. This needs Go 1.27 on the host:

```sh
go run ./cmd/summarize        # raw/*.json + notes.json -> summary.json, SUMMARY.md
```

CI runs the same command and fails if the committed `summary.json` or
`SUMMARY.md` differ from what the committed raw files produce.

`docker compose down` stops everything.

## What it does

**One upstream, many downstream.** `hub.Upstream` holds one WebSocket to
[Hyperliquid's](https://hyperliquid.xyz) public API (`wss://api.hyperliquid.xyz/ws`)
with one `bbo` (best bid and offer) subscription per symbol — BTC, ETH and SOL by
default. The `bbo` channel carries prices and sizes only, no account addresses.
It reconnects with a doubling wait from 1 s to 1 min, pings every 30 s, and
reconnects if nothing arrives for 60 s. While it is down the hub keeps the last
real price per symbol with the feed's own timestamp and reports since when the
feed has been down. It never fills a gap.

**Prices are passed through, never computed.** `hub.ParseBBO` is the only place a
`Quote` gets its prices, and it copies the feed's own strings. A message with an
empty side, or a price that is not a plain decimal, is skipped, not repaired.

**Two places a price can be replaced, both counted.**

1. *The hub's inbox.* The feed reader stamps each price (`RecvNanos`) the moment
   it reads it and drops it into a latest-per-symbol inbox; a fanout goroutine
   takes the inbox one pass at a time. A symbol that moves twice during one pass
   goes out once, newest, and the older price is counted as `upstream_conflated`.
   The reader never waits for a fanout, so a busy hub shows up as latency rather
   than as unread messages queued in a socket.
2. *Each subscriber's mailbox.* At most one unsent price per symbol. A newer
   price replaces an unsent older one (`conflated`), so a subscriber that falls
   behind gets the latest price rather than a growing queue, and the hub never
   waits for it.

Every quote carries a per-symbol sequence number, so a subscriber can count what
it never received.

**Drops, with a reason.** A write that takes longer than 5 s drops that
subscriber (`write timed out`); so does one whose prices have waited 10 s with
nothing written (`stuck`). The hub sends the reason before closing when it can,
and counts every drop by reason. It pings every subscriber every 25 s, under the
60-second idle limit common at proxies. A subscriber that closes its own socket
is not a drop.

## What the load generator measures, and how

`loadgen.Run` opens N real WebSocket subscribers, waits until every dial has
finished, then measures for the given window:

- **Delivery latency** — the subscriber's receive time minus the moment the hub
  read that price from the feed. That needs one clock, so the hub and the load
  generator run on the same machine. Kept in fixed buckets (10 µs wide below
  1 ms, ten times wider each decade above, overflow at 60 s); a percentile is
  its bucket's upper edge, so it is never understated, and the maximum is exact.
- **Served, not just connected.** A subscriber counts as connected when its
  first frame from the hub arrives, not when its handshake completes;
  `never_served` reports any that completed a handshake and received nothing.
- **Delivered per second, replaced, dropped**, and the hub's own counters read
  at the window's two edges: what the feed sent, what the hub offered and
  delivered, what it replaced, and its CPU time. `cmd/loadgen` samples the hub's
  `/stats` every 500 ms for its peak goroutines and heap.

The hub listens on four ports so one generator can open more connections than a
single destination's port range allows; both containers raise their own
file-descriptor limit and widen their own port range in `docker-compose.yml`.
None of that needs root on the host.

## Results

See [`results/SUMMARY.md`](results/SUMMARY.md) (generated) and the raw files in
[`results/raw/`](results/raw/). The machine, date and Go version are read from
the kernel during each run and recorded in every file.

**The rate is the market's, not the test's.** The feed sends a price when the
best bid or offer changes, so a quiet hour sends fewer than a busy one; every
run records what the feed sent inside its own window.

## Two defects the scale runs found

Both are fixed, and each has a test that fails on the old code.

- **Registration starved behind fanout.** The first version held the hub's
  read lock for a whole fanout pass. Registering a subscriber needs the write
  lock, so at 100k subscribers registrations and passes took turns one at a
  time: 15,600 of 100,000 upgraded sockets were ever registered, and the load
  generator — then counting a completed handshake as "connected" — reported
  100,000. The pass now copies the subscriber list under the lock and offers
  after releasing it, and the generator counts a subscriber only once it has
  been served (`TestRegistrationIsNotBlockedByAFanoutInProgress`).
- **The feed waited for the fanout.** `Publish` fanned out inline, so at 100k a
  pass took over a second, the feed's messages queued in the socket, and they
  were stamped only when finally read — the latency left out exactly the delay
  it should have shown. Publishing now goes through the inbox described above
  (`TestPublishNeverWaitsForAFanoutAndKeepsTheNewest`).

## Layout

```
hub/        the hub: upstream, fanout, mailboxes, drops, the HTTP upgrader
loadgen/    the subscriber swarm and its latency histogram
results/    summary.json (embedded by package results), SUMMARY.md, raw/*.json
cmd/hub     the hub as a server: /ws and /stats
cmd/loadgen one run, written as JSON
cmd/summarize   raw/*.json -> summary.json and SUMMARY.md
scripts/scale.sh   the levels, each against a fresh hub
```

`go test -race ./...` runs everything offline: the feed in the tests is a local
fake.

## License

MIT. Market data is Hyperliquid's; this repository reads its public API and
redistributes nothing but what a run printed.
