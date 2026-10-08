#!/bin/sh
# Run the scale levels given as arguments (subscriber counts) against a fresh
# hub each, then write results/summary.json.
#
#   ./scripts/scale.sh 10000 50000 100000
#   DURATION=30s ./scripts/scale.sh 1000
#
# Each level restarts the hub so its peak memory belongs to that level alone,
# and waits until the hub reports the upstream feed up before measuring. If
# the feed never comes up the script stops: there is nothing real to fan out.
set -eu
cd "$(dirname "$0")/.."

DURATION=${DURATION:-60s}
URLS=ws://hub:8080/ws,ws://hub:8081/ws,ws://hub:8082/ws,ws://hub:8083/ws
export HOST_UID="$(id -u)"
mkdir -p results/raw

docker compose build
for n in "$@"; do
	docker compose up -d --force-recreate hub
	tries=0
	until curl -fsS http://127.0.0.1:8080/stats | grep -q '"up":true'; do
		tries=$((tries + 1))
		if [ "$tries" -gt 60 ]; then
			echo "scale: the hub's upstream feed did not come up in 60 s; stopping" >&2
			exit 1
		fi
		sleep 1
	done
	echo "scale: $n subscribers for $DURATION"
	docker compose run --rm loadgen \
		-clients "$n" -duration "$DURATION" \
		-urls "$URLS" -stats http://hub:8080/stats \
		-label "$n subscribers" -out "/results/raw/$n.json"
done
docker compose down
