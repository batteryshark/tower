# Spike: watching Tower's live event stream in OpenChamber

Date: 2026-10-23. Question: what is the cheapest way to watch the live stream in OpenChamber?

## Recommendation

Build option (a): a read-only SSE page served by the Tower handler host on a loopback port and exposed tailnet-only
at `https://<your-mac>.<tailnet>.ts.net:9443/tower/` with one new `tailscale serve` path route. The
principal opens that URL in OpenChamber's browser panel on the Mac, or in the phone's browser over the tailnet. No
OpenChamber extension, no Canvas view, no NATS credential in any browser.

It is the only option that worked live in every place the principal looks (desktop browser panel and a phone-sized
viewport through tailscale serve). It is also the cheapest: about half a day, and nothing to install in OpenChamber.

## Evidence

Probe: a 25-line Python SSE server on a loopback port. It served a page whose
`EventSource('events')` showed one JSON tick per second. Every probe process was stopped by PID afterwards.

| Option | Result | Evidence |
| --- | --- | --- |
| (a) web page + SSE, browser panel | **Worked** | `browser.open` on the probe's loopback URL in the OpenChamber browser panel: the snapshot showed `ticks 4`, rising by one per second. |
| (a) same, through tailscale serve, phone width | **Worked** | A temporary foreground `tailscale serve --https=<spare port> --set-path=/tower http://127.0.0.1:<probe port>/tower` (self-removing; the production serve port untouched). `curl -N` on `https://…ts.net:<spare port>/tower/events` streamed ticks 1 s apart (no proxy buffering). The browser panel at the 390×844 mobile viewport showed `ticks 3`, then `ticks 8` 5 s later. The relative `events` URL worked under the `/tower` prefix. After Ctrl-C, `tailscale serve status --json` was byte-identical to before. |
| (b) Agent Canvas HTML view | **Did not work (by design)** | `canvas_show` warned at save time: "Canvas pages have no network access". Rendered, the console showed `Connecting to 'http://127.0.0.1:<probe port>/events' violates … "connect-src 'none'"`, and the same for `fetch`. The probe server received no request from the frame. A view changes only when an agent saves a new revision, so "live" would mean an agent re-posting every few seconds: costly and not live. A pinned view is reopened from the Canvas sidebar's "Project views" list (or its link), but it shows a snapshot only. |
| (c) OpenChamber extension (panel or Work Status section) | **Not tried: costs more and does less** | Read `@openchamber/sdk` 2.3.2 docs (`GUEST_SERVICES.md`, `DOCUMENTATION.md`) and Docket's extension (`docket/openchamber`). Panels run in `sandbox="allow-scripts"` with `connect-src` limited to the guest's own `/api/guests/<id>/`, or to up to 8 declared **https** origins (`contributes.origins`, approved by the principal). A service can relay requests, but "streaming from a service to the panel … is deferred; it needs a streaming call on the SDK first", so a relay means polling (Docket's panel polls). Extensions are installed by the principal in Settings → Extensions (not from an agent session), need an approval dialog, and **don't load on mobile**. The only live route would be an EventSource to the https tailnet origin of option (a), with CORS, so (c) is at most a later wrapper around (a). |
| (d) NATS websocket from the browser | **Not tried: ruled out on security** | Needs a `websocket {}` listener in nats.conf (owned by bus-core; the current template has none) and a NATS credential in page JavaScript, where anyone who opens the page can read it. A subscribe-only user would limit the damage, but it still hands out a bus credential and the nats.ws client bundle, and it is not cheaper than SSE. It also fails in Canvas and extension frames for the same CSP reasons as (b) and (c). |

Other findings:
- Other local services already share one `tailscale serve` HTTPS port through path routes, the pattern recommended here.
- The probe above shows SSE streams through `tailscale serve` without proxy buffering.

## Route to build

Served by the handler host (asyncio, nats-py). Use aiohttp for the HTTP side (one dependency; it does async streaming
next to nats-py; stdlib `http.server` is threaded and doesn't fit asyncio). Bind `127.0.0.1:8796` by default, and put the port in config.

Endpoints (all GET, read-only; there is no publish path):
- `/tower/`: one self-contained HTML page with inline CSS and JS and relative URLs, so it works at the loopback root and
  under the tailnet prefix. Mobile-first: one column, newest event at the top, a subject filter box, pause/resume,
  a connection badge, and a tap to expand an event's JSON. It renders event fields with `textContent` only, never
  `innerHTML`, because event data is untrusted.
- `/tower/events?subject=ev.>&last=100`: `text/event-stream`.
  - Server side: an ephemeral **ordered consumer** (no acks, no durable state) on stream `TOWER` filtered by
    `subject`. It starts from the last `last` messages (cap 500), or from `Last-Event-ID + 1` when the browser
    reconnects, so a reconnect has no gaps.
  - Each message is sent as `id: <stream seq>` / `event: tower` / `data: {"seq":…,"subject":…,"event":<CloudEvents envelope>}`.
    `subject=dlq.>` reads `TOWER_DLQ` the same way.
  - A `: ping` comment every 15 s keeps proxies and phone browsers from dropping the connection. Headers:
    `Cache-Control: no-cache`, `X-Accel-Buffering: no`.
  - Validate `subject` against `^(ev|dlq)(\.[A-Za-z0-9_-]+|\.\*)*(\.>)?$` and refuse anything else with 400.
  - Limit to 20 concurrent streams. Delete the consumer when the client disconnects.
- `/tower/health`: `{"ok":true,"nats":"connected"}`.

The NATS token stays in the handler host (read from `~/.tower/secrets/`). The browser never sees a credential.

Exposure (a shared live change, so it needs the lead's go-ahead when built):
```
tailscale serve --bg --https=9443 --set-path=/tower http://127.0.0.1:8796/tower
```
Rollback: `tailscale serve --https=9443 --set-path=/tower off`. Check: `tailscale serve status` lists `/tower` and the
other routes are unchanged.
Access control is tailnet-only, the same as the other path routes: any tailnet device allowed by the ACLs
can read the stream. If an event's data must not be shown that way (for example Nod answer text), decide per
channel. The simplest rule: the page shows `data` and publishers keep secrets out of events, as the event-format
doc already requires.

How the principal opens it:
- Mac: OpenChamber browser panel → `https://<your-mac>.<tailnet>.ts.net:9443/tower/` (or
  `http://127.0.0.1:8796/tower/`). An agent can open it for them with `browser.open`.
- Phone: the same https URL in Safari over the tailnet; Add to Home Screen for one-tap access. EventSource reconnects
  on its own after the phone sleeps, and `Last-Event-ID` fills the gap.
- Save the URL as a project Note in OpenChamber Knowledge.

Rough cost: about 120 lines of Python (route, ordered consumer, SSE writer, filter check), about 150 lines of
HTML/JS, and tests (subject check, SSE framing, resume from Last-Event-ID against a test nats-server). Half a day
for one medium worker, plus one serve route with a live check.

Later, optional: an OpenChamber Work Status section extension that opens an EventSource to the https origin through
`contributes.origins` (the endpoint above plus `Access-Control-Allow-Origin` for that frame). That means 1 day or
more, installing by the principal, and no mobile. Only worth it if the principal wants the stream inside the chat
chrome rather than the browser panel.
