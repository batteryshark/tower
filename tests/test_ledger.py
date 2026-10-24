"""The persistent (source, event id) ledger."""

import os
import stat

from tower import kit
from tower.ledger import Ledger


def test_mark_once_and_survive_reopen(tmp_path):
    ledger = Ledger.under(tmp_path)
    book = ledger.bind("h")
    assert not book.seen("nod/a", "e1", namespace="live")
    assert book.mark("nod/a", "e1", namespace="live", ref="r1", outcome="woke", info={"revision": 2})
    assert not book.mark("nod/a", "e1", namespace="live", outcome="again")
    ledger.close()

    reopened = Ledger.under(tmp_path).bind("h")
    row = reopened.get("nod/a", "e1", namespace="live")
    assert row["outcome"] == "woke" and row["info"] == {"revision": 2}
    assert reopened.find("r1", namespace="live")[0]["event_id"] == "e1"
    assert stat.S_IMODE(os.stat(tmp_path / "ledger.sqlite").st_mode) == 0o600


def test_keys_are_scoped(tmp_path):
    ledger = Ledger.under(tmp_path)
    a, b = ledger.bind("a"), ledger.bind("b")
    a.mark("nod/x", "e1", namespace="shadow")
    assert not a.seen("nod/x", "e1", namespace="live")
    assert not a.seen("nod/y", "e1", namespace="shadow")
    assert not b.seen("nod/x", "e1", namespace="shadow")
    assert ledger.event_ids("a", ("live", "shadow")) == {("nod/x", "e1")}


def test_context_exposes_ledger(tmp_path):
    ctx = kit.Context(name="h", js=None, log=kit.Log("t"), state_root=tmp_path)
    ctx.ledger.mark("src", "e1")
    assert kit.Context(name="h", js=None, log=kit.Log("t"), state_root=tmp_path).ledger.seen("src", "e1")
    assert not kit.Context(name="other", js=None, log=kit.Log("t"), state_root=tmp_path).ledger.seen("src", "e1")
