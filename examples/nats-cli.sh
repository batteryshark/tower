#!/bin/sh
# The same thing with the official `nats` CLI (brew install nats), if you have it.
# The token comes from the file through $NATS_TOKEN, so it never appears on a command line.
set -eu
export NATS_URL="${TOWER_NATS_URL:-nats://127.0.0.1:4222}"
NATS_TOKEN=$(cat "${TOWER_NATS_TOKEN_FILE:-$HOME/.tower/secrets/nats.token}")
export NATS_TOKEN

case "${1:-help}" in
  pub)    # publish a CloudEvent; the header lets JetStream drop duplicates
    id=$(uuidgen | tr 'A-Z' 'a-z')
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    nats pub -H "Nats-Msg-Id:$id" ev.example.greeting.sent \
      "{\"specversion\":\"1.0\",\"id\":\"$id\",\"source\":\"tower/examples/nats-cli\",\"type\":\"example.greeting.sent\",\"time\":\"$now\",\"datacontenttype\":\"application/json\",\"data\":{\"text\":\"hello from the nats CLI\"}}"
    ;;
  sub)    nats sub "${2:-ev.>}" ;;                          # live only
  watch)  nats sub --stream TOWER --all "${2:-ev.>}" ;;     # replay the stream, then tail
  streams) nats stream ls && nats stream info TOWER ;;
  *) echo "usage: $0 pub|sub [filter]|watch [filter]|streams" ;;
esac
