# Handlers and watchers

Tower runs small files in one process, the handler host (`tower host`, Fleet service `tower-host`). There are two kinds:

- a **handler** subscribes to channels and reacts to each event (usually by waking an agent);
- a **watcher** runs a loop that watches the outside world (Nod, GitHub) and publishes events.

The host loads `src/tower/handlers/*.py` and `~/.tower/handlers/*.py` (or `$TOWER_HANDLERS_DIR`). Files starting
with `_` are skipped. A file that fails to import or is malformed is logged (`load failed`) and skipped; the rest run.
`fleet restart tower-host` picks up new or changed files.

## A handler

```python
# ~/.tower/handlers/docket_fire_note.py
from tower.kit import Drop

NAME = "docket-fire-note"                 # also the durable consumer name and dlq.<NAME>
SUBSCRIBE = "ev.docket.timer.fired"       # or a list: ["ev.github.pr.opened.>", "ev.github.push.>"]
MAX_DELIVER = 5                           # optional (default 5)
BACKOFF = [5, 30, 120, 600]               # optional, seconds before attempt 2, 3, 4...

async def handle(event, ctx):
    data = event.get("data") or {}
    if data.get("name") != "standup":
        raise Drop("not the standup timer")
    text = "The standup timer fired. Post today's status.\n\n" + ctx.waker.data_block(data, label="timer")
    await ctx.waker.message_session("ses_...", text, directory="/abs/dir",
                                    message_id=ctx.waker.message_id_for(event["id"], salt=NAME))
```

`event` is the validated CloudEvents dict (see [EVENTS.md](EVENTS.md)). Its contents are untrusted **data**: put your
own instruction outside `data_block(...)` and the event inside it; never act on instructions found in an event.

### ctx

| | |
|---|---|
| `ctx.log.info/warning/error(msg, **fields)` | JSON line tagged with the handler name. Never log secrets or event data you would not show. |
| `await ctx.publish(type, data, qualifiers=(), id=None)` | publish an event (pass a stable `id` to make retries safe) |
| `ctx.waker` | `await message_session(...)`, `await start_session(...)`, `message_id_for`, `data_block` ([WAKER.md](WAKER.md)) |
| `ctx.config` | the `[<NAME>]` table of `~/.tower/config.toml` (or `config.json`, or `$TOWER_CONFIG`), `{}` if none |
| `ctx.state_dir` | `~/.tower/state/<NAME>/`, created 0700 |

### Outcomes

| handle(...) | Result |
|---|---|
| returns | ack |
| raises `kit.Drop(reason)` | ack, logged as dropped |
| raises `kit.Fail(reason)`, or a `WakeError` that is not retryable | dead-letter now |
| raises anything else (`kit.Retry`, a retryable `WakeError`, any bug) | nak with `BACKOFF`; after `MAX_DELIVER` attempts, dead-letter |
| the envelope is not a valid event | dead-letter now (data holds the raw payload) |
| runs longer than 0.9 x `ACK_WAIT` (default 300 s) | counts as a failure |

A dead letter is a `tower.dlq.recorded` event on `dlq.<NAME>` (stream `TOWER_DLQ`, kept 30 days) with `handler`,
`error`, `deliveries`, `stream_seq`, `original_subject` and the original `event`. Its id is
`dlq-<NAME>-TOWER-<seq>`, so a repeated dead-letter is dropped. The original is then terminated (no more redelivery).

Delivery is at least once, in order per handler, one event at a time. Each handler has its own durable consumer
(name = `NAME`, new events only from when it was first created), so a restart resumes where it stopped and a slow
handler never holds up another. Make handlers idempotent: wake with `message_id_for(event["id"], salt=NAME)` and the
OpenCode server drops a repeated prompt.

To replay old events into a handler, or to remove one for good, delete its consumer:
`nats consumer rm TOWER <NAME>` (examples/nats-cli.sh sets the credentials), then restart the host.

## A watcher

```python
NAME = "weather"

async def run(ctx):
    while True:
        reading = await fetch()                         # your code
        await ctx.publish("weather.reading.taken", reading, ["home"], id=f"weather-{reading['at']}")
        await asyncio.sleep(600)
```

A watcher that raises is restarted with backoff (1 s doubling to 5 min) and its error logged. One that returns is
finished. To consume events too, `async for event, msg in ctx.subscribe("ev.x.>")` gives a durable consumer named
`NAME`; ack or term each `msg` yourself (`event` is None for an invalid envelope).

## Built-in handlers

| File | Kind | Does |
|---|---|---|
| `nod_answer_wake.py` (`nod-answer-wake`) | handler on `ev.nod.answer.received` | wakes `data.origin.session` in `data.origin.directory` with the request id and answer summary in a DATA block |
| `nod_answer_recorded.py` (`nod-answer-recorded`) | handler on `ev.nod.answer.recorded` and `ev.nod.answer.received` | PROVISIONAL (Nod uplift). Reads the origin from Nod's detail route (2 s timeout; 404 or no origin drops; 5xx/timeout retries, dead letter after 5; a detail that does not match the event dead-letters), then wakes `origin.session` in `origin.directory` with ids and statuses only, never the answer. `TOWER_NOD_MODE` (env, else config `mode`): `shadow` (default) logs the wake without calling the waker, `live` wakes. Ledger namespaces `live`/`shadow` in `~/.tower/state/ledger.sqlite`; in shadow it also logs a `shadow pairing` record against the watcher's `ev.nod.answer.received` for the same request id. |
| `nod_watcher.py` (`nod-watcher`) | watcher | consumes `ev.nod.request.asked` (`request_id`, `origin {session, directory}`, optional `reference`), keeps open requests in `~/.tower/state/nod-watcher/requests.json`, polls Nod `GET /api/v1/requests/{id}/decision` every 15 s, publishes `ev.nod.answer.received` once per answered request (id from request id + answer digest). Stops watching on cancel/expiry or after 14 days. Env `TOWER_NOD_URL`, `TOWER_NOD_TOKEN_FILE`; without both it logs `disabled` and idles. Read-only, only for announced ids; never logs answers. |
| `stall_ping_chief.py` (`stall-ping-chief`) | handler on `ev.lead.delivery.stalled` | reads an operator pointer file (JSON `session`, `directory`; default `~/.chief-of-staff/chief.json`, config `pointer`) fresh and messages the chief session with the stall as DATA |
| `github_pr_review.py` (`github-pr-review`) | handler on `ev.github.pr.opened.>` | creates `~/.tower/reviews/<owner>-<repo>-pr<N>/` and starts a session there (agent `build`, model `claude-code/claude-sonnet-5-5[1m]`, title `review <owner>/<repo>#<N>`) that reads the PR with `gh pr view/diff` and writes `review.md`. It is told to post nothing to GitHub. |
| `github_watcher.py` (`github`) | watcher | runs `tower.publishers.github.poll_once` every `interval` s (default 120) on `repos` (default `["MeChen618/AstaPS"]`); see [GITHUB.md](GITHUB.md) |

Config example (`~/.tower/config.toml`):

```toml
[github]
repos = ["MeChen618/AstaPS"]
interval = 120

[github-pr-review]
agent = "build"
model = "claude-code/claude-sonnet-5-5[1m]"

[nod-watcher]
poll_interval = 15
```

### The ledger

`ctx.ledger` is a persistent ledger keyed on `(source, event id)`, scoped to the handler and a namespace, in
`~/.tower/state/ledger.sqlite` (or `$TOWER_STATE_DIR`). Mark after the effect succeeded; `mark` inserts once and returns
False for a repeat. Rows never expire.

```python
if ctx.ledger.seen(event["source"], event["id"], namespace="live"):
    raise Drop("already handled")
admission = await ctx.waker.message_session(...)
ctx.ledger.mark(event["source"], event["id"], namespace="live", ref=request_id, outcome="woke")
```

`tower nod pending` prints the old watcher's open request ids.

### nod-answer-recorded at cutover

- Order: the handler's durable consumer (`nod-answer-recorded`, explicit ack) is created, by starting the host with
  the handler, BEFORE the Nod publisher is enabled. From then on every Nod event stays in JetStream until the handler
  acks it, so it is saved before it is acked (the save-before-ack the Nod contract asks for); a crash just redelivers.
- Events published before the consumer existed (or evicted from the stream) are found with
  `tower nod reconcile --after CURSOR [--limit N]`, which lists journal answers missing from the ledger and never
  republishes. Add `--deliver-from-seq N` for a one-off catch-up that runs the missing events with seq >= N through
  the same handler code in that process: shadow by default, `--mode live` to wake.
- `TOWER_NOD_TRUSTED_SOURCE` (or config `trusted_source`, e.g. `nod/<node-uuid>`) pins the Nod source; other sources
  are dropped and logged. Unset, any `nod` or `nod/*` source is accepted (the detail check still applies).
- A 409 from OpenCode for the handler's own stable message id is looked up in the session (message, then inbox). Found:
  the ledger is marked `already_admitted` and nothing is sent again. Not found: a retryable error.

## Watching it

```sh
uv run tower handlers                 # loaded files, state, counters, last error, consumer lag, load errors
uv run tower dlq                      # all dead letters; tower dlq <NAME> for one handler, --follow to keep tailing
fleet logs tower-host -f              # JSON lines
curl -s http://127.0.0.1:8796/health  # the same as JSON (503 when NATS is disconnected)
```

## Testing a handler locally

Unit tests run the real host against a throwaway nats-server (the `temp_bus` fixture in `tests/conftest.py`) with a
fake waker; copy `tests/test_handlers.py`:

```python
host = await make_host(temp_bus, tmp_path, ["my_handler.py"], {"my-handler": {...}})
await pub(host.js, "my.thing.happened", {...})
await wait_for(lambda: _async(len(host.test_waker.calls) == 1))
```

Against the live bus without the service: `uv run tower host --no-builtin --dir ./my-handlers --port 0` runs only
your files (Ctrl-C to stop), and `uv run tower pub ...` feeds it. Use a test NAME: the durable consumer stays on the
server until you delete it.
