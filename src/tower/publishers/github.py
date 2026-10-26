"""GitHub publisher: poll followed repos with the `gh` CLI and publish PR and push events.

Read-only on GitHub: it lists open pull requests and reads the default-branch ref, nothing else.

Events (qualifiers are owner and repo, sanitized by tower.bus.subject_for):
    ev.github.pr.opened.<owner>.<repo>      a PR not seen before
    ev.github.pr.updated.<owner>.<repo>     a known open PR's head sha changed
    ev.github.branch.pushed.<owner>.<repo>  the default branch head changed

The first poll of a repo is a baseline: it records what exists and publishes nothing. Event ids are
deterministic (uuid5 of repo, kind, number and sha), so a restart or a race never publishes twice.

Used by the handler host's GitHub watcher through poll_once(repos, state_path, publish), and standalone:

    python -m tower.publishers.github --once [--repo owner/name ...] [--backfill N]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import tomllib
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable

log = logging.getLogger("tower.github")

DEFAULT_REPOS = []
DEFAULT_INTERVAL = 120.0
DEFAULT_STATE_PATH = "~/.tower/state/github.json"
CONFIG_PATHS = ("~/.tower/config.toml", "~/.tower/config.json")
SOURCE = "tower/watcher/github"

PR_OPENED = "github.pr.opened"
PR_UPDATED = "github.pr.updated"
BRANCH_PUSHED = "github.branch.pushed"

# Fixed namespace so event ids are the same on every machine and every run.
ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/batteryshark/tower#publishers.github")
DEFAULT_BRANCH_TTL = 24 * 3600  # re-read the repo's default branch once a day
MAX_KNOWN_PRS = 1000  # per repo; oldest numbers are dropped beyond this
TITLE_MAX = 300

# publish(type, data, qualifiers, event_id) -> awaitable
Publish = Callable[[str, dict, list[str], str], Awaitable[Any]]


class GitHubError(RuntimeError):
    """A gh call failed (not found, auth, rate limit, network)."""


@dataclass
class Response:
    status: int
    etag: str | None
    data: Any  # parsed JSON; None on 304


# gh(path, etag) -> Response. The default runs `gh api -i`; tests pass a fake.
Gh = Callable[[str, str | None], Awaitable[Response]]


async def gh_api(path: str, etag: str | None = None, *, timeout: float = 30.0) -> Response:
    """GET one API path with `gh api -i`, sending If-None-Match when an ETag is known."""
    args = ["gh", "api", "-i", "-H", "Accept: application/vnd.github+json"]
    if etag:
        args += ["-H", f"If-None-Match: {etag}"]
    args.append(path)
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise GitHubError(f"gh api {path}: timed out after {timeout:.0f}s") from None
    text = out.decode("utf-8", "replace")
    head, sep, body = text.replace("\r\n", "\n").partition("\n\n")
    lines = head.split("\n")
    if not sep or not lines[0].startswith("HTTP/"):
        raise GitHubError(f"gh api {path}: {err.decode('utf-8', 'replace').strip()[:300] or 'no response'}")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    if status == 304:
        return Response(304, headers.get("etag") or etag, None)
    if status >= 400:
        raise GitHubError(f"gh api {path}: HTTP {status} {body.strip()[:300]}")
    return Response(status, headers.get("etag"), json.loads(body) if body.strip() else None)


def load_repos(path: str | None = None) -> list[str]:
    """Followed repos from github.repos in ~/.tower/config.toml (or .json); DEFAULT_REPOS if unset."""
    for candidate in [path] if path else CONFIG_PATHS:
        file = Path(os.path.expanduser(candidate))
        if not file.exists():
            continue
        raw = file.read_bytes()
        config = tomllib.loads(raw.decode()) if file.suffix == ".toml" else json.loads(raw)
        repos = (config.get("github") or {}).get("repos")
        if repos is not None:
            return [check_repo(r) for r in repos]
    return list(DEFAULT_REPOS)


def check_repo(repo: str) -> str:
    owner, _, name = str(repo).strip().partition("/")
    if not owner or not name or "/" in name:
        raise ValueError(f"repo {repo!r}: use owner/name")
    return f"{owner}/{name}"


def event_id(repo: str, kind: str, *parts: Any) -> str:
    return str(uuid.uuid5(ID_NAMESPACE, "|".join([repo.lower(), kind, *map(str, parts)])))


def load_state(path: str | Path) -> dict:
    file = Path(os.path.expanduser(str(path)))
    try:
        state = json.loads(file.read_text())
    except FileNotFoundError:
        return {"repos": {}}
    state.setdefault("repos", {})
    return state


def save_state(path: str | Path, state: dict) -> None:
    """Atomic write, file 0600 in a 0700 directory."""
    file = Path(os.path.expanduser(str(path)))
    file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = file.with_name(f".{file.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, file)


def _pr_data(repo: str, pr: dict) -> dict:
    return {
        "repo": repo,
        "owner": repo.split("/")[0],
        "number": pr["number"],
        "title": (pr.get("title") or "")[:TITLE_MAX],
        "url": pr.get("html_url"),
        "author": (pr.get("user") or {}).get("login"),
        "head_sha": pr["head"]["sha"],
        "base": pr["base"]["ref"],
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "draft": bool(pr.get("draft")),
    }


async def _poll_repo(repo: str, rs: dict, gh: Gh, *, backfill: int, now: float) -> list[dict]:
    """Poll one repo, updating its state dict in place; return the events to publish."""
    owner, name = repo.split("/")
    out: list[dict] = []
    baseline = not rs.get("baselined")
    etags = rs.setdefault("etags", {})

    def emit(kind: str, data: dict, eid: str) -> None:
        out.append({"type": kind, "data": data, "qualifiers": [owner, name], "id": eid})

    # Default branch, cached for a day.
    if not rs.get("default_branch") or now - rs.get("default_branch_at", 0) > DEFAULT_BRANCH_TTL:
        info = await gh(f"repos/{repo}", None)
        rs["default_branch"] = info.data["default_branch"]
        rs["default_branch_at"] = now

    # Open PRs, newest first. More than 100 open PRs: only the newest 100 are tracked.
    path = f"repos/{repo}/pulls?state=open&sort=created&direction=desc&per_page=100"
    # Backfill needs the list itself, so it skips the ETag.
    res = await gh(path, None if backfill else etags.get("pulls"))
    known: dict[str, dict] = rs.setdefault("prs", {})
    if res.status != 304:
        for pr in res.data:
            key, sha = str(pr["number"]), pr["head"]["sha"]
            entry = known.get(key)
            if entry is None:
                known[key] = {"sha": sha, "announced": not baseline}
                if not baseline:
                    emit(PR_OPENED, _pr_data(repo, pr), event_id(repo, "pr.opened", key, sha))
            elif entry["sha"] != sha:
                if not baseline:
                    data = _pr_data(repo, pr) | {"before": entry["sha"]}
                    emit(PR_UPDATED, data, event_id(repo, "pr.updated", key, sha))
                entry["sha"] = sha
        open_now = {str(pr["number"]) for pr in res.data}
        for key, entry in known.items():
            entry["open"] = key in open_now
        # Backfill: the N most recent open PRs not yet announced, once each.
        for pr in res.data[:backfill]:
            entry = known[str(pr["number"])]
            if not entry["announced"]:
                emit(PR_OPENED, _pr_data(repo, pr), event_id(repo, "pr.opened", pr["number"], pr["head"]["sha"]))
                entry["announced"] = True
        if len(known) > MAX_KNOWN_PRS:
            for key in sorted(known, key=int)[: len(known) - MAX_KNOWN_PRS]:
                del known[key]
        etags["pulls"] = res.etag

    # Default-branch head.
    branch = rs["default_branch"]
    ref_path = f"repos/{repo}/git/ref/heads/{branch}"
    res = await gh(ref_path, etags.get("ref") if rs.get("branch") == branch else None)
    if res.status != 304:
        after = res.data["object"]["sha"]
        before = rs.get("head") if rs.get("branch") == branch else None
        if before and before != after and not baseline:
            emit(
                BRANCH_PUSHED,
                {
                    "repo": repo,
                    "owner": owner,
                    "branch": branch,
                    "ref": f"refs/heads/{branch}",
                    "before": before,
                    "after": after,
                    "url": f"https://github.com/{repo}/compare/{before}...{after}",
                },
                event_id(repo, "push", branch, before, after),
            )
        rs["branch"], rs["head"] = branch, after
        etags["ref"] = res.etag

    if baseline:
        rs["baselined"] = True
        log.info(
            "github %s: baseline, %d open PRs, %s at %s; nothing published",
            repo, sum(1 for e in known.values() if e.get("open")), branch, (rs.get("head") or "?")[:12],
        )
    rs["polled_at"] = now
    return out


async def poll_once(
    repos: Iterable[str],
    state_path: str | Path = DEFAULT_STATE_PATH,
    publish: Publish | None = None,
    *,
    gh: Gh = gh_api,
    backfill: int = 0,
) -> list[dict]:
    """Poll each repo once, publish new events, save state. Returns the events (dicts: type, data, qualifiers, id).

    State for a repo is saved only after its events are published, so a failed publish is retried next poll
    with the same ids. A repo that fails (gh error) is logged and skipped; the others still run.
    """
    state = load_state(state_path)
    published: list[dict] = []
    for repo in map(check_repo, repos):
        key = repo.lower()
        rs = json.loads(json.dumps(state["repos"].get(key, {})))  # work on a copy until published
        try:
            evs = await _poll_repo(repo, rs, gh, backfill=backfill, now=time.time())
        except (GitHubError, KeyError, TypeError, ValueError) as exc:
            log.warning("github %s: poll failed: %s", repo, exc)
            continue
        for ev in evs:
            if publish is not None:
                await publish(ev["type"], ev["data"], ev["qualifiers"], ev["id"])
            log.info("github %s: %s #%s id=%s", repo, ev["type"], ev["data"].get("number", "-"), ev["id"])
        published += evs
        state["repos"][key] = rs
        save_state(state_path, state)
    return published


def bus_publisher(js) -> Publish:
    """A publish callable that builds the CloudEvent and publishes it on the bus."""
    from .. import bus, events

    async def publish(type: str, data: dict, qualifiers: list[str], eid: str):
        subject = f"{data['repo']}#{data['number']}" if "number" in data else f"{data['repo']}@{data.get('branch')}"
        event = events.build(type, data, source=SOURCE, subject=subject, id=eid)
        ack = await bus.publish(js, event, qualifiers)
        if getattr(ack, "duplicate", False):
            log.info("github: %s already on the bus (duplicate dropped)", eid)
        return ack

    return publish


async def _main(args: argparse.Namespace) -> int:
    from .. import bus

    repos = [check_repo(r) for r in args.repo] if args.repo else load_repos(args.config)
    nc = await bus.connect()
    try:
        publish = bus_publisher(nc.jetstream())
        backfill = args.backfill
        while True:
            await poll_once(repos, args.state, publish, backfill=backfill)
            backfill = 0
            if args.once:
                return 0
            await asyncio.sleep(args.interval)
    finally:
        await nc.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m tower.publishers.github", description=__doc__.split("\n")[0])
    p.add_argument("--once", action="store_true", help="poll once and exit")
    p.add_argument("--repo", action="append", default=[], help="owner/name, repeatable (default: config github.repos)")
    p.add_argument("--backfill", type=int, default=0, help="also publish the N most recent open PRs not yet announced")
    p.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="seconds between polls (default 120)")
    p.add_argument("--state", default=DEFAULT_STATE_PATH, help=f"state file (default {DEFAULT_STATE_PATH})")
    p.add_argument("--config", help="config file (default ~/.tower/config.toml or config.json)")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stderr)
    try:
        return asyncio.run(_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
