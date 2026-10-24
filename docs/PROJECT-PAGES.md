# Project pages adapter v1

Additive GET-only routes: `/tower/projects/`, `/tower/projects/<opaque-id>` and
`/tower/projects/<opaque-id>/images/<opaque-image-id>`. Existing event viewer stays intact.
Set `TOWER_PROJECT_MANIFEST` to an operator-owned JSON file. Absent configuration is
visibly unavailable, never populated from fixtures or automatic OpenChamber discovery.
No dependency, listener, credential, DB access, network fetch or write is introduced.

## Manifest (operator-owned, private)

```json
{"version":1,"projects":[{
  "id":"p_demo","title":"Example","root":"/approved/canonical/project",
  "outcome_id":"outcome-2","status":"active","header_file":"lead",
  "files":[{"id":"lead","label":"LEAD.md","path":"LEAD.md"},
    {"id":"plan","label":"Native plan","root":"/approved/canonical/plans","path":"approved-plan.md"}],
  "images":[{"id":"diagram","path":"images/diagram.png"}],
  "export":{"root":"/approved/canonical/exports","path":"p_demo.json"}
}]}
```

IDs are opaque stable operator assignments, ASCII alphanumeric/underscore/hyphen,
1–80 chars. All projects (including held ones if desired) must be explicitly listed;
no implicit inclusion of personal roots. `files` is an exact Markdown allowlist.
An optional per-file root permits exactly one approved external native plan, not browsing.
Images are an exact raster allowlist under the project root. Relative references resolve
from the selected Markdown directory, including `../images/example.png` ONLY when the
computed reference maps to an already approved project-root image ID. The reference is
never used as a filesystem read path; unlisted/escaping references never load. External
images/SVG never load. Raster endpoints check magic bytes and return matching MIME/nosniff.
Roots must be canonical, without symlink components. Files/dirs beneath roots are opened
with no-follow descriptors; files with multiple hardlinks are refused. Root ancestors must
remain operator-controlled: no universal TOCTOU claim is made against a hostile root owner.
Exact-file privacy scans remain mandatory; filename checks cannot detect secret content.
Hidden, secret, token, credential, auth, password and key paths,
percent encoding, backslashes and parent traversal are rejected even when configured.
Limits: 100 projects, 100 files/images each, 512KiB per text/JSON, 2MiB per raster/response.

## Concrete kit export contract (serialized next slice)

Kit atomically replaces each sanitized projection using supported Knowledge interfaces;
Tower only reads it. No private database/auth store scrape. Missing API means
`knowledge.state: "unavailable"`, not replacement content. Export is trusted operator
input: strip secrets/private operational receipts before writing. Include only allowlisted
project records. No runtime manifest or actual private data is shipped here.

```json
{
  "version":1,"project_id":"p_demo","outcome_id":"outcome-2",
  "generated_at":"2026-10-23T18:00:00Z",
  "summary":{"observed_at":"2026-10-23T17:59:00Z",
    "provenance":"current owner status note","reference":"native-note-id",
    "usable now":"Source available","left":"Integration","waiting on":"Review","handed to":"Kit builder"},
  "knowledge":{"state":"available","provenance":"OpenChamber Knowledge API",
    "items":[
      {"id":"note-native-id","kind":"note","title":"Decision","body":"Markdown text"},
      {"id":"todo-native-id","kind":"todo","title":"Next","body":"Details","completed":false},
      {"id":"plan-native-id","kind":"plan","title":"Plan","body":"# Plan\n- [ ] Work"}
    ]},
  "acceptance":{"project_id":"p_demo","outcome_id":"outcome-2",
    "state":"accepted","request_state":"answered",
    "request_id":"nod-request-id","requested_at":"2026-10-23T17:00:00Z",
    "reference":"sanitized outcome reference","provenance":"chief-only Nod answer projection",
    "answer":{"type":"nod.answer","id":"durable-answer-id","request_id":"nod-request-id",
      "project_id":"p_demo","outcome_id":"outcome-2","value":"Accept",
      "answered_at":"2026-10-23T18:00:00Z","reference":"durable answer reference",
      "provenance":"actual Nod response"}},
  "activity":[{"id":"stable-event-id","project_id":"p_demo","outcome_id":"outcome-2",
    "kind":"landing","at":"2026-10-23T16:00:00Z","label":"Source landed",
    "reference":"commit abc123","provenance":"Git commit"}]
}
```

All nested objects/arrays/strings are strictly typed; unknown fields invalidate the relevant
manifest entry or projection. Version must be integer1, never boolean. JSON nesting max12
(parser RecursionError is also handled). Manifest file/version/array corruption produces503;
malformed entries become explicit unavailable rows, preserving all other entries. Malformed
per-project exports become unavailable without blanking the overview or exposing partial evidence.
Required top-level identity/version/timestamp mismatch invalidates the projection.
Optional export sections: summary, knowledge, acceptance, activity. No null sections.
Manifest requires id/outcome_id/title/status/root; files/images default empty, header_file/export
optional. The examples enumerate all permitted keys. Request fields are absent when request_state
is none; answer is absent or null unless answered. Answered state requires a complete actual answer.
Knowledge state is available/unavailable (unavailable may not carry items); provenance is required,
items defaults empty. Each note/todo/plan requires id/kind/title/body; completed is required on todo
and, whenever present, must be an actual boolean (0/1/strings refused). Duplicate IDs refused.
IDs match `[A-Za-z0-9_-]{1,80}`. Titles/labels max200, status max80, header/summary fields max2000,
body max50000, reference/provenance/activity label max500, paths max4096 chars. Nonempty text is
required except knowledge body; control characters other than newline/tab/CR are refused.
Times must be explicit RFC3339-like timezone-bearing ISO strings, max40 chars, year2000–2100,
not future. Summary observed_at and activity at cannot exceed generated_at.
Acceptance states: `unfinished`, `finished-awaiting-acceptance`, `accepted`, `not-yet`.
Request states: `none`, `pending`, `expired`, `cancelled`, `answered`. Pending/expired/
cancelled are displayed as request provenance, never answers. Accepted/not-yet require
answered request state, request ID, actual answer ID/time, provenance/reference on both request
and answer, exact answer.type `nod.answer` and `Accept` / `Not yet` value. Answer.request_id
must equal acceptance.request_id; request and answer project_id/outcome_id must each match
the projection and manifest. requested_at <= answered_at <= generated_at is mandatory.
Authenticity and chief-only issuance remain exporter obligations; Tower validates bindings,
not cryptographic authenticity. No fake historical Nod receipt may be manufactured;
the exporter must not carry an earlier outcome's answer into a new outcome.
An unavailable projection omits acceptance. LEAD text never grants acceptance. Acceptance is
displayed only for explicit finished/accepted/not-yet state in a valid outcome-bound projection.
Keep native note/todo/plan IDs and todo boolean completed state; max 200 displayed items.
Native plan may instead be the explicitly mapped Markdown file above.

Activity kinds: `landing`, `acceptance`, `decision`, `handoff`; each requires factual
time, stable ID, project/outcome identity, concise label, safe text reference and provenance.
Max 500 entries consumed per project. Six-hour feed filters future/old times and dedupes IDs.
Last landed is the newest actual landing for this outcome, never file mtime/header text.
Unknown history remains unknown. Header uses only first 30 lines before first `##`:
Usable now / Left / Waiting on / Handed to. Status is operator-manifest scope, not inference.
Optional summary snapshot is a migration seam for legacy LEADs, not a new ledger. Its exact
fields are shown above; observed_at/provenance/reference are mandatory. Optional `needs_you`
is strictly boolean; absent/false means no Needs you marker. Only an explicit principal action
should set it, never a substring match against prose. This is the only schema extension.
Explicit sourced summary text takes priority. A missing field may fall back to a local header
only if <=100 characters without obvious hashes, IDs or paths. This conservative filter is not
a plain-language translator: the parent owns templates and source language. Missing values are
omitted. Full local header and provenance remain under Project details and the selected document.
Overview has one full-row project link: name, short manifest status, explicit Needs you, and
relative actual last landing if known. Detail caps the three main sentences at word boundaries
around100 characters. Updated uses summary.observed_at, never generated_at or file mtime;
absolute same-host local time is secondary in time tooltips (no script/CSP change).
Selected files, including approved-outcome text, are behind a named Read disclosure by default;
the GET file selection remains in the URL. Feed shows three recent factual events, with remaining
events behind More activity; technical evidence is collapsed. Empty feed is omitted.

## Local validation / rollout

Run `PYTHONPATH=src <python> -m pytest tests/test_projects.py tests/test_project_validation.py tests/test_viewer.py`.
Tests create synthetic temporary manifests only. No production fallback fixture exists.
The layout reuses viewer theme tokens, wraps long text and uses a 390px-compatible grid;
Markdown supports safe headings, paragraphs, emphasis/strong/inline code, ordered/unordered/task
lists, tables, fenced code, HTTP(S) links and approved images, not arbitrary HTML or full CommonMark
extensions. Task checkboxes are disabled, not a write UI. CSP forbids scripts/external images.
Parent owns manifest/export integration, install/rollback and real tailnet phone screenshots;
source tests are not evidence of installed acceptance. No live deployment or push here.

## Supported Knowledge export source (kit-owned, not Tower runtime)

Parent established the maintained OpenChamber agent-tool transport. In an actual inherited
agent context, read OPENCHAMBER_AGENT_TOOL_URL and OPENCHAMBER_AGENT_TOOL_TOKEN in memory only.
Validate loopback URL, refuse redirects/remote origin, use bounded timeout/response and POST
with Bearer token and JSON `{input:{action:"knowledge.list",directory:<explicit approved root>},
sessionID:<actual calling native session>,tool:"openchamber_knowledge"}`. Validate response
schemaVersion1/ok:true/action knowledge.list before projecting only approved notes/todos/plans.
Never forge another session or persist credentials. Only this read action is permitted.
Plans return paths, not bodies: individually approve those exact Markdown paths. Missing inherited
auth/API means knowledge unavailable; never scrape DB/private stores. Initial/manual refresh is
supported with generated_at visible; no unattended freshness guarantee is made. Tower does not
call this transport or retain its token. Docket idle_question events and existing viewer/handlers
are unchanged: idle_minutes is not minutes_since_landed, a landing, or a fault.
