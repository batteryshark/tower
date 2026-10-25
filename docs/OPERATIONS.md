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
| Host state | `~/.tower/state/` (waker ledger, the retired nod-watcher's ledger kept for audit, github state, handler ledger `ledger.sqlite`; `TOWER_STATE_DIR` moves it); reviews in `~/.tower/reviews/` |
| Host env | `TOWER_NOD_URL=<Nod base URL>`, `TOWER_NOD_TOKEN_FILE=<path to a Nod issuer token>` (read by path; `nod-answer-recorded` and `nod-catchup` need both), `PYTHONUNBUFFERED=1` |

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

## S7: Nod watcher retired (2026-10-25)

`nod-watcher` (polled Nod for requests announced on `ev.nod.request.asked`) and `nod-answer-wake` (woke on
`ev.nod.answer.received`) are deleted; `nod-answer-recorded` on Nod's `ev.nod.answer.recorded` is the only Nod path.
Their durable consumers are removed by the operator (`nats consumer rm TOWER nod-watcher`,
`nats consumer rm TOWER nod-answer-wake`). `~/.tower/state/nod-watcher/requests.json` stays for audit;
`tower nod pending` still reads it.

Rollback: the old path needs the Nod issuer OFF first, so the old path and the new one never both wake. Then
revert the S7 commit in a worktree from origin/main (`git revert --no-edit <S7 sha>`), push to main, fast-forward the
main checkout (`git pull --ff-only`) and `fleet restart tower-host`. The old handlers recreate their consumers (they
start at new events only).

## Per-producer users (runbook for the window)

Status: tooling ready, NOT applied. The live `nats.conf` and `auth.conf` still use the shared token, and none of the
password files below exist. Only the window runs `apply --execute`.

Why: with one shared token any publisher can publish anything, including a forged `source`. NATS refuses a token
alongside users (checked on 2.15.1), so the switch is all at once: after it, every token connection is refused.

| User | Publish | Subscribe | Password file (0600, created only by `apply`) | Used by |
|---|---|---|---|---|
| `tower` | all | all | `~/.tower/secrets/tower.password` | handler host, viewer, GitHub watcher, the plain `tower` CLI |
| `docket` | `ev.docket.>`, `ev.lead.>` | `_INBOX_docket.>` | `~/.tower/secrets/docket.password` | Docket's publisher, inbox prefix `_INBOX_docket` |
| `nod` | `ev.nod.>` | `_INBOX_nod.>` | `~/.tower/secrets/nod-publisher.password` | Nod's publisher (from S2), inbox prefix `_INBOX_nod` |

How Tower clients choose auth (`tower.bus.connect`, used by every Tower path): explicit arguments >
`TOWER_NATS_USER` + `TOWER_NATS_PASSWORD_FILE` > the pointer `~/.tower/nats/client.json`
(`TOWER_NATS_CLIENT_CONFIG` moves it) > the token. A missing pointer means the token, which is today's behaviour.
The pointer holds no secret: `{"mode": "user", "user": "tower", "password_file": "..."}` or `{"mode": "token"}`.
`tower auth` prints the choice without connecting. `auth.conf` holds only bcrypt hashes (`nats server passwd`).

The tool, dry run unless `--execute`; it never restarts anything and prints the restart order:

```sh
tower admin nats-users plan                       # paths, users, ACLs; no values
tower admin nats-users apply [--execute]          # backup-<ts>/, missing password files, auth.conf (not client.json)
tower admin nats-users switch-clients [--execute] # client.json mode user; only after the nats restart
tower admin nats-users rollback [--execute]       # newest token auth.conf backup back, client.json mode token
tower admin nats-users verify [--stage s1|s2]     # the checks, in order
```

`apply` is idempotent: a second run changes nothing. It copies `nats.conf` and `auth.conf` into
`~/.tower/nats/backup-<ts>/` and leaves `nats.conf` and `~/.tower/secrets/nats.token` untouched, so rollback always
has the token config.

### S1: users on, with Docket (Nod still off)

Run the checks in this order. If any check fails, run the rollback once, in its order below, then HOLD: no
automatic repeat.

a. Preconditions: Docket's publisher can use user `docket`, a password file by path and inbox prefix
   `_INBOX_docket` (inert until e); Nod's publisher is off. `tower admin nats-users plan` shows the expected changes.
b. Nothing yet: `apply` and the restarts all happen in c, so Tower clients are never pointed at users before the
   server has them.
c. In one go, in this order:
   1. `tower admin nats-users apply --execute` (backs up, creates the password files, writes `auth.conf`; client.json
      is untouched, so Tower clients keep using the token for now).
   2. `nats-server -c ~/.tower/nats/nats.conf -t` (must pass).
   3. `fleet restart nats`. All token clients drop and are refused from now on.
   4. `tower admin nats-users switch-clients --execute` (client.json mode user).
d. `fleet restart tower-host`; it reconnects as `tower` through client.json. The gap where Tower clients cannot
   connect is c3 to c4 for the CLI and c3 to d for the host.
e. Docket (owned by the Docket lead): remove `DOCKET_TOWER_TOKEN_FILE` from Docket's env (Docket drops its user
   mode if `DOCKET_TOWER_TOKEN_FILE` exists, even empty), add `DOCKET_TOWER_NATS_USER=docket` and
   `DOCKET_TOWER_PASSWORD_FILE=~/.tower/secrets/docket.password`, then restart Docket so it loads Docket `eef870c`.
f. Verify:
   1. `tower admin nats-users verify --stage s1` prints `ok` for, in order: configured users are exactly
      tower, docket and nod; the token is refused; tower connects through client.json and reads the stream; among
      active connections `tower` is present, `nod` is NOT (before S2), nothing else and none without a user. Docket
      may be absent: its publisher connects only for each publish.
   2. Docket's proof: one Docket fire lands on TOWER after the switch: `tower watch 'ev.docket.>' --since 30m`
      shows a fire newer than the nats restart.
   3. `curl -s http://127.0.0.1:8796/health` is 200 and `tower handlers` lists every handler running.

Rollback (once, then HOLD):

1. `tower admin nats-users rollback --execute` (restores the newest token `auth.conf` from `backup-<ts>/`, keeps the
   per-user one in `rollback-<ts>/`, sets client.json to `{"mode": "token"}`; password files stay).
2. `nats-server -c ~/.tower/nats/nats.conf -t`, then `fleet restart nats`.
3. `fleet restart tower-host`.
4. Docket (Docket lead): restore `DOCKET_TOWER_TOKEN_FILE`, remove `DOCKET_TOWER_NATS_USER` and
   `DOCKET_TOWER_PASSWORD_FILE`, restart Docket.
5. Check: health 200, `tower handlers`, `tower auth` says `token`.

### S2: Nod's publisher on

The Nod lead configures Nod's publisher with user `nod`, the `nod-publisher.password` file and inbox prefix
`_INBOX_nod`; it publishes with `Nats-Expected-Stream: TOWER`. Then `tower admin nats-users verify --stage s2`. Nod may publish only `ev.nod.>` and subscribe only `_INBOX_nod.>`.

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
