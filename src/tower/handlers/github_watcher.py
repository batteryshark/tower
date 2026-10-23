"""Poll followed GitHub repos and publish ev.github.* events (tower.publishers.github).

Config ([github] in ~/.tower/config.toml): repos (default ["MeChen618/AstaPS"]), interval seconds (default 120).
Read-only on GitHub. State: ~/.tower/state/github.json (shared with `python -m tower.publishers.github`).
"""

import asyncio
from pathlib import Path

NAME = "github"
STATE = Path("~/.tower/state/github.json").expanduser()


async def run(ctx):
    try:
        from tower.publishers.github import poll_once
    except ImportError:
        ctx.log.warning("disabled", reason="tower.publishers.github is not installed")
        await asyncio.Event().wait()
    repos = list(ctx.config.get("repos") or ["MeChen618/AstaPS"])
    interval = max(30, int(ctx.config.get("interval") or 120))
    ctx.log.info("watching", repos=repos, interval=interval)

    async def publish(type, data, qualifiers=(), event_id=None):
        return await ctx.publish(type, data, qualifiers, event_id, source="tower/watcher/github")

    while True:
        try:
            published = await poll_once(repos, STATE, publish)
            if published:
                ctx.log.info("poll", published=len(published))
        except Exception as exc:  # noqa: BLE001 - keep polling; the host logs crashes only
            ctx.log.warning("poll failed", error=f"{exc.__class__.__name__}: {exc}"[:300])
        await asyncio.sleep(interval)
