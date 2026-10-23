# Operating the Tower bus

## Layout on this Mac

| What | Where |
|---|---|
| nats-server | `/opt/homebrew/bin/nats-server` (Homebrew `nats-server`); `nats` CLI from Homebrew `nats` |
| Config | `~/.tower/nats/nats.conf` (0600), generated from `deploy/nats.conf.template` |
| Token | `~/.tower/secrets/nats.token` (0600, dir 0700); copied into `~/.tower/nats/auth.conf` (0600) |
| JetStream data | `~/.tower/nats/jetstream` |
| Service | Fleet `nats` (LaunchAgent `local.nats`, group `tower`) |
| Log | `~/Library/Logs/tower-nats.log` |
| Client port | `127.0.0.1:4222` (token auth) |
| Monitoring | `http://127.0.0.1:8222` (`/healthz`, `/varz`, `/jsz`; no auth, loopback only) |
| Tailnet | `tailscale serve --tcp 4222` -> `tcp://127.0.0.1:4222`, tailnet only. Nothing listens on the LAN. |
| Handler host | Fleet `tower-host` (LaunchAgent `local.tower-host`, group `tower`): `.venv/bin/python -m tower host` in the project dir |
| Host health | `http://127.0.0.1:8796/health` (loopback only) |
| Live viewer | `http://127.0.0.1:8796/tower/`; tailnet `https://<your-mac>.<tailnet>.ts.net:9443/tower/` via `tailscale serve --set-path=/tower` ([VIEWER.md](VIEWER.md)) |
| Host log | `~/Library/Logs/tower-host.log` (JSON lines) |
| Host state | `~/.tower/state/` (waker ledger, nod-watcher ledger, github state); reviews in `~/.tower/reviews/` |
| Host env | `TOWER_NOD_URL=<Nod base URL>`, `TOWER_NOD_TOKEN_FILE=<path to a Nod issuer token>` (read by path; the Nod watcher is disabled without both), `PYTHONUNBUFFERED=1` |

## Install (what was done)

```sh
brew install nats-server nats
./scripts/setup-nats.sh                  # token (if missing) + config under ~/.tower; never prints the token
fleet add nats --port 8222 --probe /healthz --group tower --log ~/Library/Logs/tower-nats.log \
  -- /opt/homebrew/bin/nats-server -c ~/.tower/nats/nats.conf
fleet up nats
uv run tower streams --ensure            # create/update the TOWER and TOWER_DLQ streams
tailscale serve --bg --tcp 4222 tcp://127.0.0.1:4222
```

Handler host (Fleet refuses a service named like its group, so the service is `tower-host` in group `tower`):

```sh
uv sync
fleet add tower-host --port 8796 --probe /health --group tower --log ~/Library/Logs/tower-host.log \
  --cwd <path-to-tower> --env TOWER_NOD_URL=<Nod base URL> \
  --env TOWER_NOD_TOKEN_FILE=<path to a Nod issuer token> \
  --env PYTHONUNBUFFERED=1 -- <path-to-tower>/.venv/bin/python -m tower host
fleet up tower-host
tailscale serve --bg --https=9443 --set-path=/tower http://127.0.0.1:8796/tower   # live viewer, tailnet only
```

`fleet up tower` / `fleet down tower` act on both services. After changing code or handler files:
`fleet restart tower-host`.

## Day to day

```sh
fleet status nats
fleet logs nats -f
curl -s http://127.0.0.1:8222/healthz
uv run tower streams
tailscale serve status
```

Rotate the token: delete `~/.tower/secrets/nats.token`, run `./scripts/setup-nats.sh`, `fleet restart nats`, then
restart every client that holds the old token (handler host, publishers, remote machines).

Config change: edit `deploy/nats.conf.template`, re-run `./scripts/setup-nats.sh`, check with
`nats-server -c ~/.tower/nats/nats.conf -t`, then `fleet restart nats`.

## Rollback

In reverse order of install. The installed CLI (callers that shell out to `tower` stop working):

```sh
uv tool uninstall tower                  # removes ~/.local/bin/tower
```

Then the handler host:

```sh
tailscale serve --https=9443 --set-path=/tower off   # viewer route only; NOT `--https=9443 off` (drops every 9443 route)
fleet down tower-host                    # stop handlers and watchers
fleet remove tower-host                  # drop it from Fleet and delete its plist
# optional: delete the durable consumers (nats consumer rm TOWER <name>) and ~/.tower/state, ~/.tower/reviews
```

Then the bus:

```sh
tailscale serve --tcp=4222 off           # remove the tailnet TCP route only; https routes stay
fleet down nats                          # stop it (stays off across logins)
fleet remove nats                        # drop it from Fleet and delete its generated plist
brew uninstall nats-server nats
rm -rf ~/.tower/nats                     # config + JetStream data (all stored events)
rm -f ~/.tower/secrets/nats.token        # only if nothing else should keep the token
```

Check after: `tailscale serve status` shows no `tcp://...:4222`, `fleet` no longer lists `nats`,
`lsof -nP -iTCP:4222 -sTCP:LISTEN` is empty.
