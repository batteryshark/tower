"""Wake the session that asked a Nod question when Nod records an answer (ev.nod.answer.recorded).

PROVISIONAL, from the Nod uplift. The work is tower.nod.process, shared with the nod-catchup watcher and the
`tower nod` commands: check the source, fetch Nod's protected detail for the origin, check it against the event, then
wake (TOWER_NOD_MODE=live) or log the wake it would send (shadow, the default). Every outcome is a ledger row.

Here: 404 or no origin is a logged drop; 5xx, timeouts and a busy claim retry, then dead-letter after MAX_DELIVER;
other 4xx and a detail that does not match dead-letter at once, and on_dead_letter keeps a durable ledger row.
The old watcher path (ev.nod.request.asked / ev.nod.answer.received) is retired (S7, 2026-10-25).
"""

from tower import nod
from tower.kit import Drop

NAME = nod.HANDLER
SUBSCRIBE = "ev.nod.answer.recorded"
MAX_DELIVER = nod.MAX_ATTEMPTS
BACKOFF = [5, 30, 120, 600]


async def handle(event, ctx):
    env = nod.Env.from_ctx(ctx)
    outcome = await nod.process(event, env, via="bus")
    if outcome.skipped:
        raise Drop(f"already {outcome.state}")
    if outcome.state.startswith("dropped:"):
        raise Drop(outcome.state[len("dropped:"):])


async def on_dead_letter(event, ctx, error):
    if event.get("type") == nod.ANSWER_TYPE:
        nod.record_dead_letter(event, nod.Env.from_ctx(ctx), error)
