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
- Optional `runtime`: `state,at,reference,provenance`, where state is running
  or unknown, never inferred idle. Positive evidence requires the exact normal
  Docket index basis `docket.running_sample`, session reference and original
  sample time. At EACH PAGE READ, running expires after 120 seconds (inclusive
  boundary) and future samples are unknown. The 900-second refresh cannot renew
  that clock. Roster-active, a header adjective or recent text is not busy.
- `file_updates`: at most 200 `id,at,reference,provenance` rows (100 files plus
  100 images). Evidence IDs separate file/image namespaces. Actual admitted
  descriptor-fstat modification times classify recency; initial inventory is
  not a feed flood. Subsequent byte changes emit `kind=file` at source time,
  never a landing. Older optional singular `file_update` remains readable.

Last activity is the newest validated activity event, actual acceptance answer,
or optional file update. The first status baseline is intentionally not activity.
Knowledge age and real Docket marker times are unchanged. Running or activity
within 48 hours selects In flight; missing evidence selects Everything else.

## Discovery and safe availability

The existing bounded refresh optionally walks explicit operator-approved canonical
roots to depth four, stops at real headers/repositories, skips worktree `.git`
files and tool/fixture/dependency directories, and caps grouping directories at
2000 and projects at 1000. Traversal uses descriptor-relative no-follow opens.
Only safe conventional LEAD.md/PROJECT.md copies are generated for new projects.
Stable IDs hash canonical paths, not inode or mutable content. Legacy exact
directory overrides retain their IDs/outcomes/selected extras and genuine cache.
No implicit media, report or native plan discovery is introduced.

Generated output must be a scoped directory beneath the configured manifest root.
The 512 KiB manifest/content limits are unchanged: realistic 500-project fixtures
fit; oversized publication is refused, never silently truncated as complete.
Root authorization permits these conventional documents on the existing tailnet,
not public publication; private-pattern checks are not a prose classifier. The
operator must review actual emitted content before installation.

Before fallible discovery or producer reads, the refresh atomically revokes old
file/image and cached-content access in a guarded manifest. A crash/partial write
leaves safe historical rows explicitly unavailable rather than serving stale
copies. A successful refresh re-admits only currently checked files. Known
incomplete inventory retains historical rows visibly stale; revoked scopes are
not re-admitted. Corrupt binding state is visibly incomplete and blocks new Nod
imports until operator repair, rather than silently resetting outcome pins.
This is not a multi-file transaction: temporary file unavailability during a
refresh is deliberate. Operator-owned writable export ancestry remains required.
Direct legacy Markdown/raster sources are also privacy-checked at page read.

## Real decisions, bounds and clocks

The supplemental index is only normal `project-index --json`: exact version1,
complete=true, at most20 leads/64KiB, original UTC6Z samples and scoped directories.
No private roster/native status read is used. Unknown source state stays unknown.

Optional private Nod configuration binds at most eight exact UUIDs to admitted
canonical directories and safe operator labels. Only normal `decision-json UUID`
is invoked. Initial project/outcome pins (or explicit outcome IDs) prevent old
answers transferring to a new outcome. Bounded opaque pins survive binding removal
and re-add, with a 1000-pin ceiling that refuses further unpinned imports.
Real per-user decisions take precedence over the top-level alias, max64 wrappers.
Event IDs retain the exact original up-to-nine-digit UTC answer string. Only the
operator label, source time and opaque domain-separated digests persist; no raw
response, actor, recipient or chosen answer does. A decision is NOT acceptance.
Unavailable reads preserve prior verified events at their original times.

CLI reads have live stdout bounds (64KiB, legacy acceptance32KiB), restricted
environment, discarded stderr and child-only kill/reap on error. Nod children
get at most3 seconds each. One shared 50-second monotonic budget bounds composed
producer reads and checked publication, reserving time under the existing60-second
host timeout; no immediate retries. The host result IPC cap alone is84KiB to fit
1000 maximum-length stale IDs. No general source/content byte cap was raised.

Knowledge remains genuine-context only, with original read age. Unread new
Knowledge is unavailable, not empty/fresh. No background native authentication,
dynamic plan enrollment, bus/watcher/history change, new service or scheduler.
Runtime installation, actual source/content scan, live tailnet screenshots and
chief acceptance remain integration-owner gates, not source-test outcomes.
