"""GitHub publisher tests with a fake gh runner. No network, no bus."""

from __future__ import annotations

import json
import os
import stat

import pytest

from tower import bus, events
from tower.publishers import github as G

REPO = "Owner/Repository"


def pr(number, sha, title="t", created="2026-10-23T00:00:00Z"):
    return {
        "number": number,
        "title": title,
        "html_url": f"https://github.com/{REPO}/pull/{number}",
        "user": {"login": "someone"},
        "head": {"sha": sha},
        "base": {"ref": "main"},
        "created_at": created,
        "updated_at": created,
        "draft": False,
    }


class FakeGh:
    """Serves repo info, the open PR list and the main ref; honors ETags like GitHub."""

    def __init__(self, prs=(), head="a" * 40):
        self.prs = list(prs)
        self.head = head
        self.calls: list[tuple[str, str | None]] = []
        self.fail = False

    def _body(self, path):
        if path == f"repos/{REPO}":
            return {"default_branch": "main"}
        if path.startswith(f"repos/{REPO}/pulls"):
            return self.prs
        if path == f"repos/{REPO}/git/ref/heads/main":
            return {"object": {"sha": self.head}}
        raise G.GitHubError(f"HTTP 404 {path}")

    async def __call__(self, path, etag):
        self.calls.append((path, etag))
        if self.fail:
            raise G.GitHubError("HTTP 502")
        body = self._body(path)
        tag = '"%x"' % (hash(json.dumps(body, sort_keys=True)) & 0xFFFFFFFF)
        if etag == tag:
            return G.Response(304, tag, None)
        return G.Response(200, tag, body)


class Recorder:
    def __init__(self):
        self.events = []

    async def __call__(self, type, data, qualifiers, eid):
        # Same checks the bus applies, so a bad type or subject fails here.
        bus.subject_for(type, qualifiers)
        events.build(type, data, source=G.SOURCE, id=eid)
        self.events.append((type, data, qualifiers, eid))


@pytest.fixture
def state_path(tmp_path):
    return tmp_path / "state" / "github.json"


async def test_baseline_publishes_nothing(state_path):
    gh, pub = FakeGh([pr(1, "s1"), pr(2, "s2")]), Recorder()
    assert await G.poll_once([REPO], state_path, pub, gh=gh) == []
    assert pub.events == []
    state = json.loads(state_path.read_text())["repos"]["owner/repository"]
    assert state["baselined"] and state["head"] == "a" * 40
    assert set(state["prs"]) == {"1", "2"}
    assert stat.S_IMODE(os.stat(state_path).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(state_path.parent).st_mode) == 0o700


async def test_new_pr_sha_change_and_push(state_path):
    gh, pub = FakeGh([pr(1, "s1")]), Recorder()
    await G.poll_once([REPO], state_path, pub, gh=gh)

    gh.prs = [pr(2, "s2", title="new"), pr(1, "s1")]
    out = await G.poll_once([REPO], state_path, pub, gh=gh)
    assert [(e["type"], e["data"]["number"]) for e in out] == [(G.PR_OPENED, 2)]
    t, data, quals, eid = pub.events[0]
    assert bus.subject_for(t, quals) == "ev.github.pr.opened.owner.repository"
    assert data["head_sha"] == "s2" and data["repo"] == REPO and data["base"] == "main"
    assert set(data) >= {"repo", "number", "title", "url", "author", "head_sha", "base", "created_at", "draft"}

    gh.prs = [pr(2, "s2"), pr(1, "s1b")]
    out = await G.poll_once([REPO], state_path, pub, gh=gh)
    assert [(e["type"], e["data"]["number"], e["data"]["before"]) for e in out] == [(G.PR_UPDATED, 1, "s1")]

    gh.head = "b" * 40
    out = await G.poll_once([REPO], state_path, pub, gh=gh)
    assert len(out) == 1 and out[0]["type"] == G.BRANCH_PUSHED
    push = out[0]["data"]
    assert (push["branch"], push["before"], push["after"]) == ("main", "a" * 40, "b" * 40)
    assert bus.subject_for(out[0]["type"], out[0]["qualifiers"]) == "ev.github.branch.pushed.owner.repository"


async def test_unchanged_poll_uses_etags_and_publishes_nothing(state_path):
    gh, pub = FakeGh([pr(1, "s1")]), Recorder()
    await G.poll_once([REPO], state_path, pub, gh=gh)
    gh.calls.clear()
    assert await G.poll_once([REPO], state_path, pub, gh=gh) == []
    # Default branch is cached; both remaining calls carry an ETag.
    assert len(gh.calls) == 2 and all(etag for _, etag in gh.calls)


async def test_restart_is_idempotent_and_ids_deterministic(state_path, tmp_path):
    gh = FakeGh([pr(1, "s1")])
    await G.poll_once([REPO], state_path, Recorder(), gh=gh)
    gh.prs.insert(0, pr(2, "s2"))

    # A publish that dies mid-poll leaves state unsaved: the next run publishes the same id once.
    async def broken(*a):
        raise ConnectionError("bus down")

    with pytest.raises(ConnectionError):
        await G.poll_once([REPO], state_path, broken, gh=gh)
    pub = Recorder()
    first = await G.poll_once([REPO], state_path, pub, gh=gh)
    again = await G.poll_once([REPO], state_path, pub, gh=gh)
    assert len(first) == 1 and again == [] and len(pub.events) == 1

    # Same inputs elsewhere give the same id, so JetStream dedupe covers two pollers racing.
    other = tmp_path / "other.json"
    gh2 = FakeGh([pr(1, "s1")])
    await G.poll_once([REPO], other, Recorder(), gh=gh2)
    gh2.prs.insert(0, pr(2, "s2"))
    assert (await G.poll_once([REPO], other, Recorder(), gh=gh2))[0]["id"] == first[0]["id"]


async def test_backfill_publishes_newest_once(state_path):
    gh = FakeGh([pr(3, "s3"), pr(2, "s2"), pr(1, "s1")])
    await G.poll_once([REPO], state_path, Recorder(), gh=gh)
    pub = Recorder()
    out = await G.poll_once([REPO], state_path, pub, gh=gh, backfill=1)
    assert [(e["type"], e["data"]["number"]) for e in out] == [(G.PR_OPENED, 3)]
    assert await G.poll_once([REPO], state_path, pub, gh=gh, backfill=1) == []
    out = await G.poll_once([REPO], state_path, pub, gh=gh, backfill=2)
    assert [e["data"]["number"] for e in out] == [2]


async def test_backfill_on_first_run(state_path):
    out = await G.poll_once([REPO], state_path, Recorder(), gh=FakeGh([pr(2, "s2"), pr(1, "s1")]), backfill=1)
    assert [e["data"]["number"] for e in out] == [2]


async def test_failed_repo_is_skipped(state_path):
    gh = FakeGh([pr(1, "s1")])
    gh.fail = True
    assert await G.poll_once([REPO], state_path, Recorder(), gh=gh) == []
    assert not state_path.exists()


def test_load_repos(tmp_path):
    toml = tmp_path / "config.toml"
    toml.write_text('[github]\nrepos = ["a/b", "c/d"]\n')
    assert G.load_repos(str(toml)) == ["a/b", "c/d"]
    js = tmp_path / "config.json"
    js.write_text(json.dumps({"github": {"repos": ["e/f"]}}))
    assert G.load_repos(str(js)) == ["e/f"]
    assert G.load_repos(str(tmp_path / "missing.toml")) == G.DEFAULT_REPOS
    with pytest.raises(ValueError):
        G.check_repo("nope")
