"""Nod journal catch-up (PROVISIONAL, from the Nod uplift): see tower.nod.run_catchup.

On start, on bus reconnect and every catchup_interval (600 s), pages Nod's journal
GET /api/v1/integrations/events from a durable cursor (bounded pages per run, up to the fixed high water) and runs
every nod.answer.recorded not yet terminal in the ledger through the same path as the bus handler. Off unless
TOWER_NOD_CATCHUP=on: then it logs "disabled" and makes no Nod call. A 404/501 from the journal backs off quietly.
Config table [nod-catchup]: catchup_interval, catchup_limit, catchup_pages, catchup_start, plus the TOWER_NOD_* keys.
"""

from tower import nod

NAME = nod.CATCHUP


async def run(ctx):
    await nod.catchup_loop(ctx)
