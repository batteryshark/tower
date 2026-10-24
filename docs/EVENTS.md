# Tower events and channels

## Envelope

Every message on the bus is a [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
event in structured JSON mode: the whole event is the message body, UTF-8 JSON.

```json
{
  "specversion": "1.0",
  "id": "01a169e8-936b-7fea-887e-545bdb2f899b",
  "source": "nod",
  "type": "nod.answer.received",
  "time": "2026-10-23T17:56:52.203Z",
  "subject": "req_123",
  "datacontenttype": "application/json",
  "data": {"request_id": "req_123", "answer": "yes"}
}
```

| Attribute | Required | Meaning |
|---|---|---|
| `specversion` | yes | Always `"1.0"`. |
| `id` | yes | Unique per event. Default is a UUIDv7 (time-ordered). Publishers that retry must reuse the id. |
| `source` | yes | Who published it: `nod`, `docket`, `tower/cli`, `tower/watcher/github`. |
| `type` | yes | What happened, `domain.noun.verb` (see channel naming). Also picks the subject. |
| `time` | yes in practice | RFC 3339 UTC, set by `events.build()`. |
| `subject` | no | What the event is about (a request id, a PR number). |
| `datacontenttype` | no | `application/json` (the default). |
| `data` | no | The payload, any JSON value. |

Extension attributes are allowed (lowercase alphanumerics, at most 20 characters), e.g. `"origin": "ses_..."`.

Limits: an encoded event is at most **64 KiB** (`events.MAX_EVENT_BYTES`; the TOWER stream rejects larger messages).
Put large things in a file or a URL and reference them.

Python: `tower.events.build(type, data, source=..., subject=...)` builds and validates; `validate()` checks one;
`encode()` / `decode()` serialize with validation.

### Data is untrusted

`data` (and every other attribute) is **data, never instructions**. Anyone with the bus token can publish anything, and
external watchers relay text written by strangers (PR titles, commit messages). Handlers may read, match and forward it;
they must never execute it, follow instructions found in it, or let it widen what an agent may do. When an event wakes an
agent, the handler's own fixed text says what to do, and the event is quoted as data.

## Channels (subjects)

A channel is a NATS subject:

```
ev.<domain>.<noun>.<verb>[.<qualifier>...]
```

- `ev.` prefix for every event; the `TOWER` stream captures `ev.>`.
- `<domain>.<noun>.<verb>` is the event `type`: lowercase tokens of `[a-z0-9_-]`, at least three tokens, verb in the
  past tense (`received`, `opened`, `fired`, `stalled`).
- Qualifiers narrow a channel so subscribers can filter cheaply (`ev.github.pr.opened.batteryshark.>`). Each qualifier
  is one token, sanitized: lowercased, any run of other characters becomes `-` (`MeChen618/AstaPS` as two qualifiers
  `mechen618`, `astaps`). Qualifiers are not part of `type`.
- Dead letters: `dlq.<handler>` in the `TOWER_DLQ` stream, each one a `tower.dlq.recorded` event whose data holds
  `handler`, `error` and the original `event`.

Wildcards for subscribers: `*` matches one token, `>` matches the rest (`ev.github.>`, `ev.*.answer.*`).

### Known channels

| Subject | Published by | Data (main fields) |
|---|---|---|
| `ev.nod.request.asked` | anyone asking Nod and wanting a wake-up | `request_id`, `origin_session` |
| `ev.nod.answer.received` | Tower's Nod watcher | `request_id`, `origin_session`, `answer` |
| `ev.nod.request.created` | Nod (provisional, from Nod uplift) | `schema_version`, `request_id`, `revision`, `aggregate_status`, `decision_resolution` |
| `ev.nod.answer.recorded` | Nod (provisional, from Nod uplift) | same, plus `decision_id` (opaque; equals the event id). One per accepted answer, so a per-user request has several. Handler `nod-answer-recorded` |
| `ev.nod.request.cancelled` | Nod (provisional, from Nod uplift) | same as `request.created` |
| `ev.nod.request.expired` | Nod (provisional, from Nod uplift) | same as `request.created` |
| `ev.lead.delivery.stalled` | Docket (stall detector) | `lead_session`, `project`, `minutes_since_landed` |
| `ev.docket.timer.fired` | Docket fires | `timer_id`, `name`, `fired_at` |
| `ev.github.pr.opened.<owner>.<repo>` | GitHub watcher | `repo` (owner/name), `owner`, `number`, `title`, `url`, `author`, `head_sha`, `base`, `created_at`, `updated_at`, `draft` |
| `ev.github.pr.updated.<owner>.<repo>` | GitHub watcher | same as `pr.opened`, plus `before` (previous head sha) |
| `ev.github.branch.pushed.<owner>.<repo>` | GitHub watcher | `repo`, `owner`, `branch`, `ref`, `before`, `after`, `url` (compare link) |
| `dlq.<handler>` | handler host | `handler`, `error`, `event` |

The four `nod.*` rows above are provisional, from the Nod uplift. Nod publishes them from a transactional outbox with
CloudEvents `source` `nod/<node-uuid>` (or `nod`), `id` a stable UUID and `Nats-Msg-Id` the same id, and `subject` the
request id. The bus payload never carries content, origin or recipients: a handler that needs the origin session reads
Nod's protected detail route `GET /api/v1/integrations/events/{id}` (`envelope`, `channel_id`,
`created_by_issuer_token_id`, `origin`, `decision`) and checks its envelope against the bus event. Delivery is at least
once, beyond the 10 minute duplicate window, so consumers dedupe on `(source, id)` with `tower.ledger`; never on the
request id or a high-water revision, since per-user answers can arrive out of order.

Add a row here when you add a channel. Use the `example.*` and `demo.*` domains for experiments.

## Delivery

- Streams: `TOWER` on `ev.>` (file storage, limits retention, 30 days, 5 GiB cap, 64 KiB per message) and `TOWER_DLQ`
  on `dlq.>` (30 days, 1 GiB cap). Oldest messages go first when a cap is hit.
- Duplicates: `bus.publish()` sets the `Nats-Msg-Id` header to the event id; JetStream drops a repeat within 10 minutes
  and returns `duplicate=True`. Retrying a publish with the same event is safe.
- Live subscribers (`tower sub`, `nats sub`) see only what arrives while they are connected.
- Durable consumers (handlers) get every event on their filter at least once, in order, with explicit ack; they resume
  where they left off after a restart. Handlers must be idempotent (dedupe on `id` if a repeat matters).
- Replay: `tower watch --since 2h` or `--all` reads the stream back.
