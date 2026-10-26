import pytest

from tower import bus, cli, events


def test_subject_for_plain_type():
    assert bus.subject_for("nod.answer.received") == "ev.nod.answer.received"


def test_subject_for_with_qualifiers_sanitized():
    assert (
        bus.subject_for("github.pr.opened", ["Owner", "Repository"])
        == "ev.github.pr.opened.owner.repository"
    )
    assert bus.subject_for("a.b.c", ["Foo/Bar", "x.y", " spaced out ", "ok_1-2"]) == "ev.a.b.c.foo-bar.x-y.spaced-out.ok_1-2"


@pytest.mark.parametrize("bad", ["", "...", "*", ">", "  "])
def test_empty_or_wildcard_qualifiers_rejected(bad):
    with pytest.raises(ValueError):
        bus.subject_for("a.b.c", [bad])


def test_bad_type_rejected():
    with pytest.raises(events.EventError):
        bus.subject_for("ev.>")


def test_dlq_subject():
    assert bus.dlq_subject("Nod-Answer") == "dlq.nod-answer"


def test_stream_configs():
    tower, dlq = bus.STREAM_CONFIGS
    assert tower.name == "TOWER" and tower.subjects == ["ev.>"]
    assert tower.max_age == 30 * 86400 and tower.duplicate_window == 600
    assert tower.max_bytes == 5 * 1024**3
    assert dlq.name == "TOWER_DLQ" and dlq.subjects == ["dlq.>"] and dlq.max_age == 30 * 86400


def test_cli_helpers():
    assert cli.parse_duration("10m").total_seconds() == 600
    assert cli.parse_duration("2d").total_seconds() == 172800
    assert cli.stream_for("dlq.>") == "TOWER_DLQ"
    assert cli.stream_for("ev.github.>") == "TOWER"
    with pytest.raises(Exception):
        cli.parse_duration("ten minutes")


def test_read_token_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("TOWER_NATS_TOKEN_FILE", str(tmp_path / "nope"))
    assert bus.read_token() is None
    (tmp_path / "t").write_text("abc\n")
    assert bus.read_token(str(tmp_path / "t")) == "abc"
