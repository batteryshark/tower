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
| Host state | `~/.tower/state/` (waker ledger, nod-watcher ledger, github state, handler ledger `ledger.sqlite`; `TOWER_STATE_DIR` moves it); reviews in `~/.tower/reviews/` |
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

## Nod shadow install (Step 0)

The handler host runs `nod-answer-recorded` (shadow: logs the wake it would send, never calls the waker) and
`nod-catchup` (off: logs `disabled`, no Nod call) with the env unchanged: `TOWER_NOD_MODE` unset means shadow,
`TOWER_NOD_CATCHUP` unset means off. Install: fast-forward the main checkout to the pushed sha
(`git pull --ff-only`), `uv sync` if `uv.lock` changed, `fleet restart tower-host`. Check `curl -s
http://127.0.0.1:8796/health`, `tower handlers`, `tower nod status`, and that the log shows no `woke origin`.

Rollback: never `git reset --hard` the main checkout without the lead. Instead, in a worktree from origin/main,
`git revert --no-edit <first>^..<last>` (the Nod commits), push that to main, then in the main checkout
`git pull --ff-only` and `fleet restart tower-host`. The durable consumer `nod-answer-recorded` stays on the stream
after a revert; remove it with `nats consumer rm TOWER nod-answer-recorded` (or leave it: nothing reads it).
`~/.tower/state/ledger.sqlite` can stay; nothing else uses it.

## Per-producer users (PLAN, not applied)

Status: a plan for the Nod cutover. The live `nats.conf` and `auth.conf` still use the single shared token, and no
password files exist yet. Nothing below has been run.

Why: with one shared token any publisher can publish anything, including a forged `source`. Per-producer users with
ACLs bound what each producer can publish. NATS refuses a token alongside users (checked on 2.15.1), so the switch is all
at once: every client moves off the token in the same step.

| User | Publish | Subscribe | Used by |
|---|---|---|---|
| `tower` | everything (`>`) | everything (`>`) | the handler host, the `tower` CLI, the viewer, the GitHub watcher, remote tailnet tools |
| `docket` | `ev.docket.>`, `ev.lead.>` | `_INBOX_docket.>` (PubAck replies) | Docket's publisher; it must set inbox prefix `_INBOX_docket` |
| `nod` | `ev.nod.>` | `_INBOX_nod.>` (PubAck replies) | Nod's outbox publisher; inbox prefix `_INBOX_nod` |

Clients: Tower's own clients connect as a user with `TOWER_NATS_USER=tower` and
`TOWER_NATS_PASSWORD_FILE=~/.tower/secrets/nats-tower.password` (`tower.bus.connect`; the token stays the default when
`TOWER_NATS_USER` is unset). Each password lives in a 0600 file under `~/.tower/secrets/` (Docket's and Nod's copies
go to their owners by path); `auth.conf` holds only bcrypt hashes (`nats server passwd`).

Planned `auth.conf` (generated by `setup-nats.sh`, never committed):

```
authorization {
  users = [
    {user: tower,  password: "<bcrypt>"}
    {user: docket, password: "<bcrypt>", permissions: {publish: ["ev.docket.>", "ev.lead.>"], subscribe: ["_INBOX_docket.>"]}}
    {user: nod,    password: "<bcrypt>", permissions: {publish: ["ev.nod.>"], subscribe: ["_INBOX_nod.>"]}}
  ]
}
```

Migration, at cutover, with the Docket and Nod leads ready:

1. Back up `~/.tower/nats/auth.conf` to `auth.conf.token-backup` (0600).
2. Create the three password files (0600) and the hashed `auth.conf`; check with `nats-server -c ~/.tower/nats/nats.conf -t`.
3. Docket ships its user/password support and inbox prefix first, still on the token (inert until step 5).
4. Add `--env TOWER_NATS_USER=tower --env TOWER_NATS_PASSWORD_FILE=...` to Fleet `tower-host` (not restarted yet).
5. `fleet restart nats`, then `fleet restart tower-host`, then Docket and Nod restart with their users.
6. Check: `tower handlers` connects; a Docket fire and a Nod event land in TOWER; as `docket`, a publish to `ev.nod.x`
   is refused (permissions violation in the nats log); an old token client is refused.

Rollback: restore `auth.conf.token-backup` over `auth.conf`, `fleet restart nats`, drop the two `TOWER_NATS_*` env
entries from `tower-host` and restart it; Docket and Nod go back to their token settings. Then delete the password
files. No stored events are affected either way.

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
