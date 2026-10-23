#!/bin/sh
# Install the Tower nats-server config under ~/.tower (outside the repo).
# Creates a random token at ~/.tower/secrets/nats.token if missing (0600) and
# writes ~/.tower/nats/nats.conf plus ~/.tower/nats/auth.conf (0600, holds the token).
# Never prints the token. Safe to re-run.
set -eu
umask 077
here=$(cd "$(dirname "$0")/.." && pwd)
tower_home="${TOWER_HOME:-$HOME/.tower}"
secrets="$tower_home/secrets"
natsdir="$tower_home/nats"
token_file="$secrets/nats.token"

mkdir -p "$secrets" "$natsdir/jetstream"
chmod 700 "$tower_home" "$secrets" "$natsdir"

if [ ! -s "$token_file" ]; then
  openssl rand -hex 32 > "$token_file"
  echo "created $token_file"
fi
chmod 600 "$token_file"

token=$(tr -d '\n' < "$token_file")
printf 'authorization {\n  token: "%s"\n}\n' "$token" > "$natsdir/auth.conf"
chmod 600 "$natsdir/auth.conf"

sed "s|@STORE_DIR@|$natsdir/jetstream|" "$here/deploy/nats.conf.template" > "$natsdir/nats.conf"
chmod 600 "$natsdir/nats.conf"
echo "wrote $natsdir/nats.conf and $natsdir/auth.conf"
