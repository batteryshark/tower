<img src="assets/tower.png" alt="A control tower passing messages between robots" width="320">

# Tower

Tower is a self-hosted event bus for local tools and automations. It combines NATS JetStream with a Python library, command-line client, handler host and read-only web viewer, so applications can publish structured events, replay missed messages and process them with retries and dead-letter queues.

## Requirements

- Python 3.11+ and [uv](https://docs.astral.sh/uv/).
- A running [NATS server](https://docs.nats.io/running-a-nats-service/introduction/installation) with JetStream enabled.
- OpenSSL for the included server-configuration script.
- Optional: an authenticated `gh` CLI for GitHub events; a local OpenCode server for session notifications.

## Install

```sh
git clone https://github.com/batteryshark/tower.git
cd tower
uv sync
uv tool install .
```

The `tower` command is installed in uv's tool binary directory. You can also run it from the checkout with `uv run tower`.

## Quick start

For a new local NATS installation:

```sh
./scripts/setup-nats.sh
nats-server -c "$HOME/.tower/nats/nats.conf"
```

Leave the server running. In another terminal:

```sh
tower streams --ensure
tower pub demo.hello.sent --data '{"text":"hello"}'
tower watch 'ev.demo.>' --all
```

`watch` replays matching messages and then follows new ones; Ctrl-C stops it. To run the viewer with only your own handlers, create an empty handler directory and start the host:

```sh
mkdir -p "$HOME/.tower/handlers"
tower host --no-builtin --dir "$HOME/.tower/handlers"
```

Open <http://127.0.0.1:8796/tower/>. Built-in integrations are documented in [Handlers](docs/HANDLERS.md); inspect and configure them before starting the host without `--no-builtin`.

## Configuration

| Variable | Default / purpose |
| --- | --- |
| `TOWER_NATS_URL` | `nats://127.0.0.1:4222` |
| `TOWER_NATS_TOKEN_FILE` | `~/.tower/secrets/nats.token` |
| `TOWER_NATS_USER`, `TOWER_NATS_PASSWORD_FILE` | Optional user/password-file authentication |
| `TOWER_NATS_CLIENT_CONFIG` | `~/.tower/nats/client.json`; authentication selection |
| `TOWER_CONFIG` | Handler settings; otherwise `~/.tower/config.toml` or `config.json` |
| `TOWER_HANDLERS_DIR` | Custom handler directory, default `~/.tower/handlers` |
| `TOWER_STATE_DIR` | Persistent handler state, default `~/.tower/state` |

Keep credential files private. `tower auth` reports the selected authentication mode without displaying credentials. See [Operations](docs/OPERATIONS.md) for setup and authentication details.

## Key commands

```sh
tower --help
tower streams                         # inspect streams
tower sub 'ev.>'                       # new events only
tower watch 'ev.github.>' --since 2h    # replay, then follow
tower handlers                        # host status
tower dlq                             # inspect dead letters
```

## Documentation

- [Event format and subjects](docs/EVENTS.md)
- [Handlers, retries and integrations](docs/HANDLERS.md)
- [Server operations](docs/OPERATIONS.md)
- [Web viewer](docs/VIEWER.md) and [project pages](docs/PROJECT-PAGES.md)
- [GitHub publisher](docs/GITHUB.md) and [OpenCode session notifications](docs/WAKER.md)
- [Python examples](examples/)

## License

[MIT](LICENSE).
