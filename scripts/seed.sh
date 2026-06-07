#!/usr/bin/env bash
#
# seed.sh — populate acheron with randomized events for manual testing.
#
# Usage:
#   scripts/seed.sh [COUNT] [BASE_URL]
#   COUNT=200 BASE_URL=http://127.0.0.1:8000 scripts/seed.sh
#
# Defaults: COUNT=50, BASE_URL=http://127.0.0.1:8000
# Requires: curl. (jq is only suggested for the verify commands at the end.)
set -uo pipefail

COUNT="${1:-${COUNT:-50}}"
BASE_URL="${2:-${BASE_URL:-http://127.0.0.1:8000}}"

EVENT_TYPES=(page_view click conversion signup checkout add_to_cart)
USERS=(alice bob carol dave erin frank grace heidi)
DEVICES=(mobile desktop tablet)
BROWSERS=(chrome firefox safari edge)
CAMPAIGNS=(spring-sale black-friday newsletter referral organic)
PATHS=(/ /home /pricing /cart /checkout /product/42 /blog/post-1)

pick() { local arr=("$@"); printf '%s' "${arr[RANDOM % ${#arr[@]}]}"; }

# Preflight: make sure the app is actually up before firing a batch at it.
if ! curl -fsS -m 3 "$BASE_URL/health" >/dev/null 2>&1; then
  echo "✗ $BASE_URL is not responding. Start the stack first:" >&2
  echo "    docker compose up --build" >&2
  exit 1
fi

echo "Seeding $COUNT events → $BASE_URL"
ok=0
fail=0
for _ in $(seq 1 "$COUNT"); do
  et=$(pick "${EVENT_TYPES[@]}")
  user=$(pick "${USERS[@]}")
  path=$(pick "${PATHS[@]}")
  campaign=$(pick "${CAMPAIGNS[@]}")
  device=$(pick "${DEVICES[@]}")
  browser=$(pick "${BROWSERS[@]}")
  # Vary the metadata shape across events (string / number / nested) to exercise
  # the schemaless `flattened` ES mapping and the metadata_text full-text mirror.
  case $((RANDOM % 3)) in
    0) meta="{\"device\":\"$device\",\"browser\":\"$browser\",\"campaign\":\"$campaign\"}" ;;
    1) meta="{\"device\":\"$device\",\"cart_value\":$((RANDOM % 500)),\"items\":$((RANDOM % 9 + 1))}" ;;
    2) meta="{\"campaign\":\"$campaign\",\"experiment\":{\"variant\":\"v$((RANDOM % 3))\",\"bucket\":$((RANDOM % 100))}}" ;;
  esac
  body="{\"event_type\":\"$et\",\"user_id\":\"$user\",\"source_url\":\"https://shop.test$path\",\"metadata\":$meta}"
  http=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/events" \
           -H 'Content-Type: application/json' -d "$body" || echo "000")
  if [ "$http" = "202" ]; then
    ok=$((ok + 1))
  else
    fail=$((fail + 1))
    echo "  ! POST returned $http for: $body" >&2
  fi
done

echo "Done: $ok accepted, $fail failed."
echo
echo "Verify (events are processed asynchronously by the worker):"
echo "  curl -s $BASE_URL/metrics | jq '.worker'"
echo "  curl -s '$BASE_URL/events?with_total=true' | jq '.total'"
echo "  curl -s '$BASE_URL/events/stats' | jq"
echo "  curl -s '$BASE_URL/events/search?q=spring-sale' | jq '.total'   # after ~5s (ES indexing)"
