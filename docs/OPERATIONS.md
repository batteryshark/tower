# Operating Tower

## Local server

Install NATS using its platform packages (on macOS: `brew install nats-server nats`). From the Tower checkout:

```sh
./scripts/setup-nats.sh
nats-server -c "$HOME/.tower/nats/nats.conf" -t
nats-server -c "$HOME/.tower/nats/nats.conf"
```

The setup script creates a random token if absent and writes the configuration. It does not start a service. It uses `~/.tower` unless `TOWER_HOME` is set. Do not rerun it over a server already configured for per-user authentication: it writes token authentication.

| Path or address | Purpose |
| --- | --- |
| `~/.tower/nats/nats.conf` | Server configuration |
| `~/.tower/nats/auth.conf` | Private authentication configuration |
| `~/.tower/secrets/nats.token` | Client token, mode 0600 |
| `~/.tower/nats/jetstream` | Persistent event storage |
| `127.0.0.1:4222` | Authenticated client port |
| `127.0.0.1:8222` | Unauthenticated local monitoring |

With the server running, create the streams once:

```sh
tower streams --ensure
curl -fsS http://127.0.0.1:8222/healthz
```

Use your operating system's service manager to keep NATS and the handler host running. Start custom handlers with `tower host --no-builtin --dir /absolute/path/to/handlers`. The host serves health at <http://127.0.0.1:8796/health> and the viewer at <http://127.0.0.1:8796/tower/>. Restart the host after changing handler files.

## Authentication

Client selection order is explicit connection arguments, `TOWER_NATS_USER` plus `TOWER_NATS_PASSWORD_FILE`, the client pointer file, then token authentication. The pointer defaults to `~/.tower/nats/client.json`; `TOWER_NATS_CLIENT_CONFIG` changes it. It contains either `{"mode":"token"}` or `{"mode":"user","user":"tower","password_file":"/absolute/path/to/password"}`. It contains no password value.

```sh
tower auth
tower admin nats-users plan
```

The optional `nats-users` tool manages a fixed integration profile: `tower` has full access; `docket` publishes `ev.docket.>` and `ev.lead.>` and subscribes to `_INBOX_docket.>`; `nod` publishes `ev.nod.>` and subscribes to `_INBOX_nod.>`. Use ordinary NATS configuration for other profiles.

`apply`, `switch-clients` and `rollback` are dry runs unless given `--execute`. `apply` backs up server configuration and creates missing private password files and bcrypt authentication hashes; it does not restart services or change client selection. Switching from a token to users disconnects token clients. Apply the configuration, validate it with `nats-server -t`, restart NATS, then switch and restart clients. `rollback` restores the newest token configuration backup and the token client pointer; restart the server and clients afterward. Keep the original token until migration and rollback are no longer needed.

For token rotation, replace the token file with a fresh private value, regenerate token-mode configuration and restart NATS and all clients. Never put credentials in a repository or command arguments.

## Remote access

The supplied server configuration binds loopback. A private Tailscale route can expose the authenticated client port:

```sh
tailscale serve --bg --tcp 4222 tcp://127.0.0.1:4222
```

Remote clients need their own private copy of the appropriate credential file and `TOWER_NATS_URL=nats://<host>.<tailnet>.ts.net:4222`. The [viewer guide](VIEWER.md) covers its separate HTTP route. Restrict access with tailnet ACLs.

## State and recovery

Handler state defaults to `~/.tower/state`; review output defaults to `~/.tower/reviews`. Back up these directories and JetStream storage according to your retention needs. Handler delivery is at least once; handlers must make effects idempotent. See [Handlers](HANDLERS.md) for retries, dead letters and the optional Nod journal recovery integration.

Stop the host and server with your service manager before removing an installation. `uv tool uninstall tower` removes only the CLI. Removing JetStream data deletes retained events; removing handler state loses deduplication and recovery information.
