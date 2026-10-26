# The live viewer

A read-only page that shows bus events as they happen. The handler host serves it, so it is up when `tower-host` is.

| Where | URL |
|---|---|
| Local browser | http://127.0.0.1:8796/tower/ |
| Any device on the tailnet, including a phone | https://<your-mac>.<tailnet>.ts.net:9443/tower/ |

On a phone, open the tailnet URL in Safari and use Add to Home Screen. The page reconnects by itself after the phone
sleeps and continues from the last event it showed.

## The page

- Events newest first: subject, time, source, stream sequence, and a one-line data preview. Tap an event for the full
  JSON (the stream, sequence, subject, receive time and the whole envelope).
- Filter: a NATS subject inside `ev.` (`ev.>`, `ev.github.>`, `ev.*.answer.*`). Tick "dead letters" to add
  `dlq.>` (dead-letter rows have a red border). Apply clears the list and replays.
- Pause holds new events (the button counts them); Resume shows them. Clear empties the list. At most 500 rows.
- Status strip, refreshed every 10 s: bus connection, each handler and watcher with its state, handled count and
  dead letters, and the TOWER_DLQ message count.
- Bookmarkable: `?filter=ev.github.>&dlq=1&since=2h` in the page URL sets the stream query.

Event data is untrusted and is only ever written as text. The page has a strict CSP (no external resources;
connections only to its own origin).

## Endpoints

All are GET, served by `tower.viewer` on the host's aiohttp app (127.0.0.1:8796). There is no publish route, and the
bus token stays in the host.

| Path | What |
|---|---|
| `/tower/` | the page (`src/tower/viewer.html`); `/tower` redirects here |
| `/tower/events` | `text/event-stream` (below) |
| `/tower/status` | JSON for the status strip: `nats`, `handlers` (state and counters, no file paths), `dead_letters`, `load_errors`, `streams` |
| `/tower/health` | the same as `/health` (200, or 503 when NATS is disconnected) |

`/tower/events` query:
- `filter`: a subject inside `ev.`, default `ev.>`. Anything else (including `dlq.>`) is 400.
- `since`: either a count of the last N **stream** messages (default 200, max 1000; the filter applies after, so
  fewer may match) or a duration `30s`, `15m`, `2h`, `1d` (max 30 days).
- `dlq=1`: also stream `dlq.>` from TOWER_DLQ.

Stream format:
- Each event is `id: <seq>` (with `dlq=1`: `<tower seq>.<dlq seq>`), `event: tower`, and
  `data: {"stream","seq","subject","received","event"}`. A message that is not valid JSON comes as `event: {"raw": …}`.
- The stream opens with `retry: 3000`, and a `: ping` comment is sent every 15 s.
- On reconnect the browser sends `Last-Event-ID`. The stream then starts right after those sequences (`since` is
  ignored), so nothing is skipped or repeated.
- Each connection is an ephemeral ordered JetStream consumer (no acks, in memory, removed 30 s after the client goes).
- At most 20 streams at once (503 after that). A client that falls 2000 events behind is disconnected and resumes by
  `Last-Event-ID`.
- When dead letters are on, the replay delivers the two streams as they arrive, so the replayed rows are not strictly
  in time order across `ev` and `dlq`. Live rows are.

Try it from a shell:

```sh
curl -N 'http://127.0.0.1:8796/tower/events?filter=ev.demo.>&since=5'
curl -N -H 'Last-Event-ID: 16' 'http://127.0.0.1:8796/tower/events'
tower pub demo.viewer.check --data '{"note":"hello viewer"}'
```

## Tailnet route

```sh
tailscale serve --bg --https=9443 --set-path=/tower http://127.0.0.1:8796/tower
```

Tailnet only (Serve, not Funnel). Any tailnet device the ACLs allow can read the stream;
publishers must keep secrets out of events. To remove only this path:

```sh
tailscale serve --https=9443 --set-path=/tower off
```

Do **not** use `tailscale serve --https=9443 off`, which the serve command suggests: it removes every 9443 route.
After either command, check `tailscale serve status` to confirm any other routes are unchanged.

## Tests

`tests/test_viewer.py` runs against a throwaway nats-server and covers: input parsing, the page and its CSP,
status, the filter and live delivery, bounded replay, resume by `Last-Event-ID`, the dead-letter toggle with the
combined id, and refusals (bad filter or since, write methods, the stream limit).
