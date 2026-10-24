# Source-only interface audit — second review

Scope: the project selection, scalable overview and truthful unavailable/runtime
changes, using the explicitly selected installed Norman and Norman-audit pack.
No redesign, independent model panel or live acceptance is implied.

## Frequent tasks and source paths

Inferred from the approved project-page brief:

1. See current projects: In flight is the default route. Everything else is one
   visible navigation link; name search is a labeled GET field with a submit.
   Tab/search/page remain in the URL and pagination links. Browser Back/bookmark
   resumes that URL; global All projects deliberately returns the default view.
2. Open a project/document: one whole-row project link, then one file-selection
   change submits GET and directly renders the article. There is no additional
   normal Open/Read disclosure action. No-script fallback is one labeled submit.
3. Inspect what changed: recent activity is visible, labeled separately as file
   update/status observation/landing/decision/acceptance. Optional event provenance
   is one named native disclosure, not primary prose or an invented answer.

These are source transition/control counts, not measured device taps or latency.

## Collector plus manual walkthrough

Ran the installed `norman-audit src/tower --json` collector once. It reports no
literal small-target findings or undocumented flags. Its HTML form facts concern
the event viewer only: it cannot parse Python-generated project markup. Manual
walkthrough therefore traced the GET routes, escaped search, selected option,
auto-submit script, article, pagination and native disclosures in `projects.py`.

For the changed availability/unknown/empty cues, the five Norman questions:

- Earned place: withheld content must not silently look current. Existing rows
  carry a short unavailable state and the detail explains refresh recovery.
- Findable: cues sit on the existing overview/detail path; no new settings/control.
- Cost: no new interaction; file change still navigates directly. Technical sample
  provenance and real sample age remain secondary.
- Feedback: an empty view says so; incomplete inventory is visibly stale; an
  aborted refresh says files unavailable. Unknown never claims idle/running.
- Resumption: file selection and overview filters/pages remain bookmarkable GET
  state. An interrupted source refresh fails closed, then successful refresh
  restores only currently admitted content; it never asks the viewer to repair
  permissions or manufacture a source observation.

Literal focus outlines and minimum target rules are retained. Synthetic regressions
cover direct selection/URL state/no-script/CSP, search/pagination, unknown and
120/121-second busy boundaries. This does not prove rendered target geometry,
clipping, actual keyboard/focus order, timing or fresh phone appearance: those
**need live check** by the integration owner at the tailnet address.

No source-scope findings remain. Source-only verdict (not install/acceptance):
**No Norman doors. Ship.**
