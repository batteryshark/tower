"""Offline invalidation authority checks; no services or real Docket calls."""
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from pathlib import Path
import sys
import json

import pytest
from tower import kit,snapshot_refresh as refresh

T='2026-01-01T00:00:00Z'


def periodic():
    return {'id':'occ_fixture','source':'docket','type':'docket.snapshot.refresh_due','time':T,
            'data':{'version':1,'purpose':'project_pages','item_id':'dkt_tower_refresh','occurrence_id':'occ_fixture',
                    'scheduled_at':T,'fired_at':T,'coalesced':0,'schedule_until':T,'provenance':'docket_recurring_clock'}}


def test_periodic_only_sanitized_notice():
    assert refresh.notice(periodic(),'dkt_tower_refresh')=={'version':1,'kind':'periodic'}
    for change in (lambda e:e.update(source='foreign'),lambda e:e['data'].update(command='evil'),
                   lambda e:e['data'].update(item_id='foreign'),lambda e:e['data'].update(coalesced=True),
                   lambda e:e['data'].update(occurrence_id='other')):
        event=periodic(); change(event)
        with pytest.raises(ValueError): refresh.notice(event,'dkt_tower_refresh')


def test_milestone_source_time_returned_id_no_prose_effect():
    event={'id':'mil_'+'a'*64,'source':'docket','type':'lead.milestone.recorded','time':T,
           'data':{'version':1,'lead_session':'ses_fixture','directory':'/fixture/project','milestone_at':T,
                   'milestone_ref':'Source marker only','observed_at':T,'roster_state':'done',
                   'provenance':'recorded_marker_observation'}}
    result=refresh.notice(event,'dkt_tower_refresh')
    assert result['marker']['id']==event['id'] and result['marker']['milestone_at']==T
    event['data']['argv']=['evil']
    with pytest.raises(ValueError): refresh.notice(event,'dkt_tower_refresh')


async def test_missing_and_invalid_inert_fixed_invocation(monkeypatch,tmp_path):
    invoke=AsyncMock(return_value={'version':1,'refreshed':1,'stale_sources':[]})
    monkeypatch.setattr(refresh,'invoke',invoke)
    ctx=SimpleNamespace(config={},log=SimpleNamespace(info=lambda *a,**kw:None))
    with pytest.raises(kit.Drop): await refresh.handle(periodic(),ctx)
    invoke.assert_not_called()
    ctx.config={'python':str(tmp_path/'python'),'script':str(tmp_path/'refresh.py'),
                'config':str(tmp_path/'refresh.json'),'item_id':'dkt_tower_refresh'}
    await refresh.handle(periodic(),ctx)
    assert invoke.call_args.args==(ctx.config,{'version':1,'kind':'periodic'})
    invalid=periodic(); invalid['data']['directory']='/event/path'
    with pytest.raises(kit.Drop): await refresh.handle(invalid,ctx)
    assert invoke.call_count==1


async def test_fixed_subprocess_bounds_and_scrubbed_context(tmp_path,monkeypatch):
    script=tmp_path/'refresh.py'; config=tmp_path/'config.json'
    config.write_text('{}')
    options={'python':str(Path(sys.executable).resolve()),'script':str(script),
             'config':str(config),'item_id':'dkt_tower_refresh'}
    monkeypatch.setenv('OPENCHAMBER_AGENT_TOOL_TOKEN','never-forward')
    monkeypatch.setenv('OPENCODE_SESSION_ID','ses_never_forward')
    script.write_text('import os,sys,json\nassert "OPENCHAMBER_AGENT_TOOL_TOKEN" not in os.environ\nassert "OPENCODE_SESSION_ID" not in os.environ\nassert json.load(sys.stdin)=={"version":1,"kind":"periodic"}\nprint(json.dumps({"version":1,"refreshed":1,"stale_sources":[]}))\n')
    assert (await refresh.invoke(options,{'version':1,'kind':'periodic'}))['refreshed']==1
    script.write_text('print("x"*40000)\n')
    with pytest.raises(ValueError,match='output bound'):
        await refresh.invoke(options,{'version':1,'kind':'periodic'})


@pytest.mark.parametrize('count',[0,2,True,-1,'2'])
async def test_safe_optional_redaction_counts(tmp_path,count):
    script=tmp_path/'refresh.py'
    result={'version':1,'refreshed':1,'stale_sources':[],'redactions':count}
    script.write_text('import sys\nsys.stdin.read()\nprint('+repr(json.dumps(result))+')\n')
    options={'python':str(Path(sys.executable).resolve()),'script':str(script),
             'config':str(tmp_path/'config.json'),'item_id':'dkt_tower_refresh'}
    if type(count) is int and count>=0:
        assert (await refresh.invoke(options,{'version':1,'kind':'periodic'}))['redactions']==count
    else:
        with pytest.raises(ValueError,match='redaction count invalid'):
            await refresh.invoke(options,{'version':1,'kind':'periodic'})
