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
from the selected Markdown directory, without `..`; external images/SVG never load.
Roots must be canonical, without symlink components. Files/dirs beneath roots are opened
with no-follow descriptors. Hidden, secret, token, credential, auth, password and key paths,
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
  "knowledge":{"state":"available","provenance":"OpenChamber Knowledge API",
    "items":[
      {"id":"note-native-id","kind":"note","title":"Decision","body":"Markdown text"},
      {"id":"todo-native-id","kind":"todo","title":"Next","body":"Details","completed":false},
      {"id":"plan-native-id","kind":"plan","title":"Plan","body":"# Plan\n- [ ] Work"}
    ]},
  "acceptance":{"state":"accepted","request_state":"answered",
    "request_id":"nod-request-id","requested_at":"2026-10-23T17:00:00Z",
    "reference":"sanitized outcome reference","provenance":"chief-only Nod answer projection",
    "answer":{"id":"durable-answer-id","value":"Accept","answered_at":"2026-10-23T18:00:00Z"}},
  "activity":[{"id":"stable-event-id","project_id":"p_demo","outcome_id":"outcome-2",
    "kind":"landing","at":"2026-10-23T16:00:00Z","label":"Source landed",
    "reference":"commit abc123","provenance":"Git commit"}]
}
```

Required top-level identity/version/timestamp mismatch invalidates the projection.
Acceptance states: `unfinished`, `finished-awaiting-acceptance`, `accepted`, `not-yet`.
Request states: `none`, `pending`, `expired`, `cancelled`, `answered`. Pending/expired/
cancelled are displayed as request provenance, never answers. Accepted/not-yet require
answered request state, request ID, actual answer ID/time, provenance and exact `Accept` /
`Not yet` value. Request/outcome binding and chief-only issuance are exporter obligations;
the exporter must not carry an earlier outcome's answer into a new outcome.
An unavailable projection yields unreported acceptance. LEAD text never grants acceptance.
Keep native note/todo/plan IDs and todo boolean completed state; max 200 displayed items.
Native plan may instead be the explicitly mapped Markdown file above.

Activity kinds: `landing`, `acceptance`, `decision`, `handoff`; each requires factual
time, stable ID, project/outcome identity, concise label, safe text reference and provenance.
Max 500 entries consumed per project. Six-hour feed filters future/old times and dedupes IDs.
Last landed is the newest actual landing for this outcome, never file mtime/header text.
Unknown history remains unknown. Header uses only first 30 lines before first `##`:
Usable now / Left / Waiting on / Handed to. Status is operator-manifest scope, not inference.
Header source ID, outcome and export timestamp remain visible for freshness/provenance.

## Local validation / rollout

Run `PYTHONPATH=src <python> -m pytest tests/test_projects.py tests/test_viewer.py`.
Tests create synthetic temporary manifests only. No production fallback fixture exists.
The layout reuses viewer theme tokens, wraps long text and uses a 390px-compatible grid;
Markdown supports safe headings, paragraphs, bullets, fenced code, HTTP(S) links and approved
images, not arbitrary HTML or full CommonMark extensions. CSP forbids scripts/external images.
Parent owns manifest/export integration, install/rollback and real tailnet phone screenshots;
source tests are not evidence of installed acceptance. No live deployment or push here.
