"""Refresh exact approved project snapshots; never wake an assistant."""
from tower.snapshot_refresh import handle

NAME = 'project-snapshot-refresh'
SUBSCRIBE = ['ev.docket.snapshot.refresh_due', 'ev.lead.milestone.recorded']
