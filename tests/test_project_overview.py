"""Synthetic-only scale and navigation; no project discovery or live runtime reads."""
import json
import subprocess
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tower.projects import Projects
from tower import project_schema as schema


async def test_500_partition_order_search_pages(tmp_path):
    now = datetime.now(timezone.utc)
    pages = Projects()
    rows = []
    for i in range(500):
        p = dict(id=f'p{i}', outcome_id='o', title=f'Project {i:03}', status='Working',
                 root=str(tmp_path), files=[])
        at = (now-timedelta(hours=i)).isoformat()
        data = dict(version=1, project_id=p['id'], outcome_id='o', generated_at=now.isoformat(),
                    file_update=dict(at=at, reference='Approved file', provenance='Source timestamp'))
        if i == 499:
            data['runtime'] = dict(state='running', at=now.isoformat(), reference='ses_fixture', provenance='docket.running_sample')
        schema.projection(data, p)
        rows.append((p, data, ''))
    flight = pages.overview(rows, {}, now)
    assert len(flight[0]) == 30 and flight[4] == 2
    assert flight[0][0][0]['id'] == 'p0'
    second = pages.overview(rows, {'page':'2'}, now)[0]
    assert [r[0]['id'] for r in second] == [f'p{i}' for i in range(30,49)] + ['p499']
    others = pages.overview(rows, {'tab':'else'}, now)
    assert others[0][0][0]['id'] == 'p49' and others[4] == 15
    assert pages.overview(rows, {'tab':'else', 'q':'PROJECT 490'}, now)[0][0][0]['id'] == 'p490'
    assert pages.overview(rows, {'tab':'else', 'page':'9999'}, now)[3] == 15
    # Unknown runtime and no source activity must not become active from a roster adjective.
    unknown = [(dict(rows[0][0], status='active'), {}, '')]
    assert pages.overview(unknown, {}, now)[0] == []
    assert len(pages.overview(unknown, {'tab':'else'}, now)[0]) == 1
    manifest=tmp_path/'manifest.json'
    manifest.write_text(json.dumps(dict(version=1, projects=[r[0] for r in rows])))
    assert len(Projects(str(manifest)).load()[0]) == 500
    pages.load = lambda: ([r[0] for r in rows], '')
    index = {r[0]['id']: r[1] for r in rows}
    pages.projection = lambda p: (index[p['id']], '')
    app = web.Application(); pages.install(app)
    async with TestClient(TestServer(app)) as client:
        body = await (await client.get('/tower/projects/?tab=else&q=Project&page=2')).text()
        assert body.count('class="project-row"') == 30
        assert 'tab=else&amp;q=Project&amp;page=3' in body
        for query in ('tab=../foo','page=0','page=-1','page=10000','q='+'x'*201):
            assert (await client.get('/tower/projects/?'+query)).status == 400


async def test_direct_selection_script_and_unknown_file(tmp_path):
    from test_projects import fixture
    pages, p, data = fixture.__wrapped__(tmp_path)
    app = web.Application(); pages.install(app)
    async with TestClient(TestServer(app)) as client:
        initial = await (await client.get('/tower/projects/p_demo')).text()
        assert 'Choose a file' in initial and '<article>' not in initial
        response = await client.get('/tower/projects/p_demo?file=lead')
        body = await response.text()
        assert '<article><h2>LEAD.md</h2>' in body and '<summary>Read ' not in body
        assert '<option value="lead" selected>' in body
        assert '<noscript><button>Read file</button></noscript>' in body
        assert 'onchange=' not in body and "script-src 'self'" in response.headers['Content-Security-Policy']
        script = await client.get('/tower/projects-select.js')
        assert script.content_type == 'application/javascript'
        source = await script.text()
        assert 'requestSubmit()' in source
        # Execute the served handler with a minimal DOM contract: one change, one GET form submit.
        subprocess.run(['node', '-e', "let listener, count=0; const select={value:'lead',form:{requestSubmit(){count++}},addEventListener(event,fn){if(event!=='change')throw Error(event);listener=fn}}; global.document={querySelectorAll(){return [select]}};" + source + ";listener();if(count!==1)throw Error('expected one submission');select.value='';listener();if(count!==1)throw Error('empty selection submitted');"], check=True, capture_output=True)
        assert 'script-src' not in script.headers['Content-Security-Policy']
        assert (await client.get('/tower/projects/p_demo?file=made-up')).status == 404


@pytest.mark.parametrize('name,value', [('runtime', dict(state=True)), ('file_update', dict(at='tomorrow'))])
def test_optional_inputs_fail_closed(name, value):
    now=datetime.now(timezone.utc).isoformat()
    data=dict(version=1, project_id='p', outcome_id='o', generated_at=now)
    data[name]=dict(at=now, reference='Source', provenance='Owner', **value) if 'at' not in value else value
    with pytest.raises(ValueError): schema.projection(data, dict(id='p',outcome_id='o'))


async def test_page_read_busy_exact_expiry_and_future_unknown(tmp_path):
    from test_projects import fixture
    pages,p,data=fixture.__wrapped__(tmp_path)
    now=datetime(2026,1,4,tzinfo=timezone.utc)
    class PageClock(datetime):
        @classmethod
        def now(cls,tz=None): return now
    data['file_update']['at']='2026-01-01T00:00:00Z'
    app=web.Application(); pages.install(app)
    async with TestClient(TestServer(app)) as client:
        for age,expected in ((0,'running'),(120,'running'),(121,'unknown'),(-1,'unknown')):
            sample=(now-timedelta(seconds=age)).isoformat()
            data['runtime']=dict(state='running',at=sample,reference='ses_fixture',provenance='docket.running_sample')
            (tmp_path/'projection.json').write_text(json.dumps(data))
            with patch('tower.projects.datetime',PageClock):
                overview=await (await client.get('/tower/projects/')).text()
                detail=await (await client.get('/tower/projects/p_demo')).text()
                elsewhere=await (await client.get('/tower/projects/?tab=else')).text()
            assert ('class="project-link"' in overview)==(expected=='running')
            assert ('class="project-link"' in elsewhere)==(expected=='unknown')
            assert 'Lead runtime: '+expected in detail
            assert data['runtime']['at']==sample # No timestamp renewal at page read.
        data['runtime']['state']='unknown'
        (tmp_path/'projection.json').write_text(json.dumps(data))
        with patch('tower.projects.datetime',PageClock):
            assert 'class="project-link"' not in await (await client.get('/tower/projects/')).text()


async def test_guarded_manifest_and_current_private_content_unavailable(tmp_path):
    from test_projects import fixture
    pages,p,data=fixture.__wrapped__(tmp_path)
    p['content_available']=False
    data['runtime']=dict(state='running',at=data['generated_at'],reference='ses_fixture',provenance='docket.running_sample')
    (tmp_path/'projection.json').write_text(json.dumps(data))
    (tmp_path/'manifest.json').write_text(json.dumps(dict(version=1,projects=[p],inventory=dict(complete=False,observed_at=data['generated_at'],provenance='Refresh unavailable'))))
    app=web.Application(); pages.install(app)
    async with TestClient(TestServer(app)) as client:
        body=await (await client.get('/tower/projects/p_demo?file=knowledge')).text()
        assert 'Project files unavailable' in body and 'todo-1' not in body
        assert 'Lead runtime: unknown' in body
        assert 'inventory incomplete' in body
        p['content_available']=True
        (tmp_path/'manifest.json').write_text(json.dumps(dict(version=1,projects=[p])))
        (tmp_path/'LEAD.md').write_text('access_token=private-fixture-canary')
        response=await client.get('/tower/projects/p_demo?file=lead')
        body=await response.text()
        assert 'private-fixture-canary' not in body and 'Selected file unavailable' in body
        policy=response.headers['Content-Security-Policy']
        for value in ("default-src 'none'","script-src 'self'","base-uri 'none'","form-action 'self'"):
            assert value in policy
        assert response.headers['X-Content-Type-Options']=='nosniff'


def test_precise_answer_time_parser_preserves_original_receipt():
    at='2026-01-01T00:00:00.123456789Z'
    assert schema.stamp(at).microsecond==123456
    data=dict(version=1,project_id='p',outcome_id='o',generated_at='2026-01-02T00:00:00Z',activity=[dict(id='nod_receipt',project_id='p',outcome_id='o',kind='decision',at=at,label='Design choice answered',reference='Opaque receipt',provenance='Normal Nod decision-json read')])
    schema.projection(data,dict(id='p',outcome_id='o'))
    assert data['activity'][0]['at']==at
