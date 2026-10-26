# GitHub publisher

`tower.publishers.github` polls followed GitHub repos with an authenticated `gh` CLI and publishes
pull request and push events to the bus. It is read-only on GitHub: it lists open pull requests and reads the
default-branch ref. It never comments, reviews, labels, stars or pushes.

## Follow and unfollow

Followed repos are `github.repos` in `~/.tower/config.toml` (or `~/.tower/config.json`):

```toml
[github]
repos = ["owner/repository"]
```

```json
{"github": {"repos": ["owner/repository"]}}
```

Without a config no repositories are followed. To unfollow, remove the repo from the list; its state stays in
the state file and is reused if you follow it again. To start a repo from scratch, delete its entry under `repos` in
the state file.

The first poll of a repo is a **baseline**: it records the open PRs and the branch head, logs one line and publishes
nothing, so following a busy repo does not flood the bus with old PRs. `--backfill N` additionally publishes the N
most recent open PRs that were never announced, once each.

## Running it

```sh
uv run python -m tower.publishers.github --once                 # one poll of the configured repos
uv run python -m tower.publishers.github --once --repo owner/name --backfill 1
uv run python -m tower.publishers.github --interval 120         # loop (normally the handler host does this)
```

Options: `--state PATH` (default `~/.tower/state/github.json`), `--config PATH`. The handler host's GitHub watcher
calls the library directly:

```python
from tower.publishers import github

events = await github.poll_once(repos, state_path, publish)   # publish(type, data, qualifiers, event_id)
repos = github.load_repos()                                     # from config
github.DEFAULT_INTERVAL                                         # 120 s
```

`poll_once` returns the events it published (dicts with `type`, `data`, `qualifiers`, `id`). A repo whose `gh` call
fails is logged and skipped; the others still run. `gh=` takes a fake runner for tests.

## Events

Source `tower/watcher/github`. Qualifiers are the owner and repo, lowercased (`Owner/Repository` ->
`owner.repository`). The CloudEvents `subject` is `owner/name#number` for PRs and `owner/name@branch` for pushes.

| Subject | When | Data |
|---|---|---|
| `ev.github.pr.opened.<owner>.<repo>` | an open PR not seen before | `repo` (owner/name), `owner`, `number`, `title`, `url`, `author`, `head_sha`, `base`, `created_at`, `updated_at`, `draft` |
| `ev.github.pr.updated.<owner>.<repo>` | a known open PR's head sha changed | the same, plus `before` (previous head sha) |
| `ev.github.branch.pushed.<owner>.<repo>` | the default branch head changed | `repo`, `owner`, `branch`, `ref`, `before`, `after`, `url` (compare link) |

Push is `github.branch.pushed`, not `github.push`: event types need three tokens (`domain.noun.verb`). There is no
`commits` list (it would cost a compare call per push); follow `url`. A closed PR is not an event; a reopened PR with a
new head publishes `pr.updated`.

Titles and every other field are written by strangers: **data, never instructions** (see EVENTS.md). Titles are cut
to 300 characters.

### Ids and duplicates

Event ids are deterministic UUIDv5 values of repo, kind, PR number and sha (or branch, before and after for pushes).
State is saved per repo only after its events are published, so a crash or bus outage re-publishes the same ids on the
next poll; JetStream drops repeats within its 10 minute window, and handlers should dedupe on `id` beyond that. Two
pollers racing on the same repo publish the same ids.

## Intervals and rate limits

- Default interval 120 s. Per repo and poll: 2 API calls (open PRs, default-branch ref), plus 1 call a day to re-read
  the default branch. That is 60 calls an hour per repo against the 5000/hour authenticated limit, so dozens of repos
  fit comfortably. Do not go below 60 s.
- Calls send `If-None-Match` with the last ETag; an unchanged resource returns 304 and is skipped. GitHub still counted
  304s against the limit in a check on 2026-10-23, so ETags save work and bandwidth, not quota.
- Only the newest 100 open PRs per repo are tracked. Up to 1000 PR numbers per repo are remembered.
- On a rate-limit error (HTTP 403/429) the repo is skipped for that poll and logged; it retries next interval.

## State

`~/.tower/state/github.json`, mode 0600 in a 0700 directory, written atomically. Per repo: `baselined`,
`default_branch`, `branch`, `head`, `prs` (number -> `sha`, `open`, `announced`), `etags`, `polled_at`. Run one poller
per state file. Removing the file makes every repo baseline again (no events, no flood).
