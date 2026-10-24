"""One-shot startup reconciliation on existing host; deliberately no loop."""
from tower.snapshot_refresh import invoke
from tower.kit import Fail

NAME = 'project-snapshot-startup'


async def run(ctx):
    if not ctx.config:
        ctx.log.info('snapshot startup disabled'); return
    try:
        result = await invoke(ctx.config, {'version':1,'kind':'startup'})
        ctx.log.info('startup snapshots refreshed',refreshed=result['refreshed'],stale_sources=len(result['stale_sources']))
    except Fail:
        ctx.log.error('Snapshot lease expired; owner must renew through normal Docket client.')
    except Exception:
        # Host restarts crashed watchers: catch once here to keep this hook truly one-shot.
        ctx.log.error('startup refresh failed; owner must check lease/config or next event')
