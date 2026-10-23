# Tower

Tower is an event bus for the tools and agents on this machine and tailnet. Anything can publish an event to a
channel; anything can subscribe to channels with wildcards, live or with replay. It is
[NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) plus a small Python kit:

- `tower.events`: the event format (CloudEvents 1.0 JSON), see [docs/EVENTS.md](docs/EVENTS.md).
- `tower.bus`: connect with the token, set up the streams, publish with duplicate protection.
- `tower` CLI: `pub`, `sub`, `watch`, `streams`, `host`, `handlers`, `dlq`.
- The handler host: small handler and watcher files with durable consumers, retries and dead letters, see
  [docs/HANDLERS.md](docs/HANDLERS.md). Waking agents: [docs/WAKER.md](docs/WAKER.md).
- The live viewer: watch events as they happen at http://127.0.0.1:8796/tower/ or, on the tailnet (phones too),
  https://<your-mac>.<tailnet>.ts.net:9443/tower/, see [docs/VIEWER.md](docs/VIEWER.md).

Tower is plumbing only. Real code publishes events (Docket fires, Nod answers, a GitHub watcher); handlers are code
that subscribe to a channel and act, for example by waking an agent. There is no central router or rules engine.

## Quick start

Requires the bus to be running (see [docs/OPERATIONS.md](docs/OPERATIONS.md)) and [uv](https://docs.astral.sh/uv/).

```sh
uv sync
uv run tower streams                                   # bus reachable? streams there?
uv run tower pub demo.hello.sent --data '{"text":"hi"}'
uv run tower watch --all                                # replay everything, then follow (Ctrl-C)
uv run tower watch 'ev.github.>' --since 2h             # one domain, last two hours
uv run tower sub 'ev.>'                                 # live only, no replay
uv run tower pub github.pr.opened -q batteryshark -q tower --data '{"number":1}'
                                                        # -> ev.github.pr.opened.batteryshark.tower
uv run tower watch 'dlq.>'                              # dead letters
```

In Python:

```python
from tower import bus, events

nc = await bus.connect()
js = nc.jetstream()
await bus.publish(js, events.build("nod.answer.received", {"request_id": "r1"}, source="nod"))
await nc.close()
```

More in [examples/](examples/): `publish.py`, `subscribe.py` (durable consumer, the handler pattern), `watch.py`,
and `nats-cli.sh` for the official `nats` CLI.

## Install the CLI

```sh
uv tool install --editable <path-to-tower>   # puts `tower` in ~/.local/bin
command -v tower                             # ~/.local/bin/tower
```

Editable, so it tracks this checkout; no reinstall after code changes (reinstall only when dependencies change).
Contract for other tools: `tower` is on PATH; callers accept a configured CLI path that defaults to `tower`
(e.g. `tower pub demo.cli.check --data '{}'`). Uninstall: `uv tool uninstall tower`.

## Connecting

| Variable | Default |
|---|---|
| `TOWER_NATS_URL` | `nats://127.0.0.1:4222` |
| `TOWER_NATS_TOKEN_FILE` | `~/.tower/secrets/nats.token` |

The server listens on loopback only and requires the token. Clients without the token are refused.

From another machine on the tailnet: copy the token into a 0600 file there (never into a repo or a command line) and
point the client at the tailnet name:

```sh
export TOWER_NATS_URL=nats://<your-mac>.<tailnet>.ts.net:4222
export TOWER_NATS_TOKEN_FILE=~/.tower/secrets/nats.token
uv run tower watch
```

The `nats` CLI takes the same values as `NATS_URL` and `NATS_TOKEN` (see `examples/nats-cli.sh`).

## Development

```sh
uv run pytest                                  # unit tests
TOWER_INTEGRATION=1 uv run pytest tests/test_integration.py   # against the live bus
```

Layout: `src/tower/` (library and CLI), `tests/`, `docs/`, `examples/`, `deploy/` (server config template),
`scripts/setup-nats.sh` (installs the config and token under `~/.tower`).
