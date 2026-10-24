# Second review source slice

Project pages allow only enumerated `file` IDs (including `knowledge`). The empty
selection shows a Choose a file prompt. The same-origin static script submits the
GET form once on a nonempty selection; selected content is immediately visible.
Without scripting, a noscript Read file submit remains usable. Only project HTML
adds `script-src 'self'`; the event viewer policy is unchanged.

Overview GET values: `tab=flight|else` (default flight), `q` (up to 200 characters),
`page` (1–9999; out-of-range pages clamp). Pages contain at most 30 whole-row links.
The overview accepts up to 1000 manifest projects, with unchanged file/content
bounds. Recent activity renders at most 50 genuine events, not status baselines.

Projection additions, all identity-bound by enclosing project/outcome:

- `status_observation`: `id,text,at,reference,provenance`. The refresh writer owns
  this baseline. Only subsequent valid Status text changes emit `kind=status`
  activity. The exact sanitized short status is the label; time is observation
  time, not source mtime. Detail/byte-hash edits do not emit status events.
- Optional `runtime`: `state,at,reference,provenance`, where state is running,
  idle or unknown. No runtime producer/detector is implemented. Absent means
  unknown. Parent must supply actual runtime evidence and freshness semantics;
  roster-active must never be mapped to running.
- Optional `file_update`: `at,reference,provenance`. Parent must supply the real
  source timestamp (not snapshot generation or observation time). No discovery
  or timestamp producer is included in this slice.

Last activity is the newest validated activity event, actual acceptance answer,
or optional file update. The first status baseline is intentionally not activity.
Knowledge age and real Docket marker times are unchanged. Running or activity
within 48 hours selects In flight; missing evidence selects Everything else.

Still gated: automatic authoritative registered-lead inventory, exact approved
root/header enumeration, runtime evidence producer and actual Nod-answer source.
Keep generated per-project HTTP file allowlists. Do not scrape transcripts or
private Docket state, infer project association, or introduce a scanner/daemon.
