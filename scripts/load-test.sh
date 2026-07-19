#!/usr/bin/env bash

set -u

COUNT="${1:-}"
DELAY="${2:-}"
API_URL="${API_URL:-http://localhost:3000}"

if [[ ! "$COUNT" =~ ^[1-9][0-9]*$ ]] || [[ -z "$DELAY" ]] || [[ ! "$DELAY" =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]]; then
  echo "Usage: $0 <message-count> <delay-seconds>"
  echo "Example: $0 100 0.1"
  exit 1
fi

if ! curl --silent --fail --max-time 3 "$API_URL/api/status" >/dev/null; then
  echo "Error: Backend is not available at $API_URL"
  echo "Start it with: cd backend && npm run dev"
  exit 1
fi

START_TIME=$SECONDS
SUCCESS=0
FAILED=0

echo "Sending $COUNT messages with a ${DELAY}s delay..."

for ((i = 1; i <= COUNT; i += 1)); do
  STATUS=$(curl --silent --output /dev/null --write-out "%{http_code}" \
    --max-time 10 \
    --request POST "$API_URL/api/messages" \
    --header "Content-Type: application/json" \
    --data "{\"content\":\"Load test message $i\"}" || true)

  if [[ "$STATUS" == "201" ]]; then
    ((SUCCESS += 1))
  else
    ((FAILED += 1))
    echo "Message $i failed (HTTP ${STATUS:-connection error})"
  fi

  if [[ "$DELAY" != "0" && "$i" -lt "$COUNT" ]]; then
    sleep "$DELAY"
  fi
done

echo
echo "Load test complete"
echo "Successful: $SUCCESS"
echo "Failed:     $FAILED"
echo "Duration:   $((SECONDS - START_TIME)) seconds"

[[ "$FAILED" -eq 0 ]]
