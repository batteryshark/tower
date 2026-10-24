import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tower.projects import Projects, header, markdown, read_file, compact, relative_time
from tower import project_schema


@pytest.fixture
def fixture(tmp_path):
    (tmp_path / 'LEAD.md').write_text('# Demo\nUsable now: source\nLeft: integrate\nWaiting on: review\nAcceptance: accepted\n## History\nUsable now: stale')
    p = {'id': 'p_demo', 'title': 'Demo', 'root': str(tmp_path), 'outcome_id': 'o2',
         'status': 'held', 'header_file': 'lead', 'files': [{'id': 'lead', 'label': 'LEAD.md', 'path': 'LEAD.md'}],
         'export': {'root': str(tmp_path), 'path': 'projection.json'}}
    data = {'version': 1, 'project_id': 'p_demo', 'outcome_id': 'o2',
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'knowledge': {'state': 'available', 'provenance': 'supported API', 'items': [{'id': 'todo-1', 'kind': 'todo', 'title': 'Task', 'body': 'Details', 'completed': False}]}}
    (tmp_path / 'projection.json').write_text(json.dumps(data))
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'version': 1, 'projects': [p]}))
    return Projects(str(manifest)), p, data


def test_header_and_outcome(fixture):
    pages, p, data = fixture
    assert header('Usable now: new\n## History\nLeft: old') == {'usable now': 'new'}
    assert pages.summary(p, data)['acceptance'] == 'unreported'
    data['acceptance'] = {'state': 'accepted', 'request_state': 'pending'}
    assert pages.summary(p, data)['acceptance'] == 'unreported'
    data['acceptance'].update(project_id='p_demo', outcome_id='o2', reference='outcome', request_state='answered', requested_at=data['generated_at'], request_id='r', provenance='Nod', answer={'id': 'a', 'type': 'nod.answer', 'project_id': 'p_demo', 'outcome_id': 'o2', 'request_id': 'r', 'reference': 'answer', 'provenance': 'Nod', 'value': 'Accept', 'answered_at': data['generated_at']})
    assert pages.summary(p, data)['acceptance'] == 'accepted'
    p['outcome_id'] = 'o3'
    assert pages.projection(p)[0] == {}


@pytest.mark.parametrize('path', ['../LEAD.md', '%2e%2e/LEAD.md', '.env.md', 'secret.md', 'keys/key.md', 'token.md', 'auth.md', '/LEAD.md', 'x\\LEAD.md'])
def test_denied(tmp_path, path):
    with pytest.raises((ValueError, OSError)):
        read_file(tmp_path, path, {'.md'})


def test_symlink_bounds(tmp_path):
    (tmp_path / 'real.md').write_text('x' * 100)
    (tmp_path / 'link.md').symlink_to(tmp_path / 'real.md')
    (tmp_path / 'dir').symlink_to(tmp_path, target_is_directory=True)
    for path in ('link.md', 'dir/real.md'):
        with pytest.raises((ValueError, OSError)):
            read_file(tmp_path, path, {'.md'})
    with pytest.raises(ValueError):
        read_file(tmp_path, 'real.md', {'.md'}, 10)


def test_markdown_security():
    result = markdown('<script>alert(1)</script>\n![x](https://evil/a.png)\n[x](javascript:alert)\n![ok](images/a.png)\n# Title\n```\n<b>x</b>\n```', {'images/a.png': '/tower/projects/p/images/i'})
    assert '<script>' not in result and 'javascript:' not in result and 'https://evil' not in result
    assert 'src="/tower/projects/p/images/i"' in result
    assert '<h1>Title</h1>' in result and '&lt;b&gt;' in result


async def test_routes_phone_readonly(fixture):
    pages, p, data = fixture
    app = web.Application()
    pages.install(app)
    async with TestClient(TestServer(app)) as client:
        for route in ('/tower/projects/', '/tower/projects/p_demo', '/tower/projects/p_demo?file=knowledge'):
            response = await client.get(route)
            body = await response.text()
            assert response.status == 200
            assert 'width=device-width' in body and 'max-width:420px' in body
            assert 'script-src' not in response.headers['Content-Security-Policy']
            for method in ('POST', 'PUT', 'DELETE', 'PATCH', 'HEAD'):
                assert (await client.request(method, route)).status == 405
        body = await (await client.get('/tower/projects/p_demo?file=knowledge')).text()
        assert 'todo-1' in body and 'completed: False' in body
        assert (await client.get('/tower/projects/p_demo?file=../LEAD.md')).status == 404
        assert (await client.get('/tower/projects/unknown')).status == 404
        assert (await client.get('/tower/projects/p_demo/images/unknown')).status == 404


async def test_unconfigured():
    app = web.Application()
    Projects().install(app)
    async with TestClient(TestServer(app)) as client:
        assert 'manifest unavailable' in await (await client.get('/tower/projects/')).text()


async def test_snapshot_controls_and_selection_source(fixture):
    """Markup/CSS contracts only: not rendered phone or keyboard proof."""
    pages, p, data = fixture
    app = web.Application()
    pages.install(app)
    async with TestClient(TestServer(app)) as client:
        overview = await (await client.get('/tower/projects/')).text()
        assert 'Shared snapshots' not in overview
        assert 'Unreported' not in overview
        assert '<a class="project-link" href="/tower/projects/p_demo"><strong>Demo</strong>' in overview
        assert 'Usable now' not in overview and 'No reported semantic activity' not in overview
        for query, label in [('', 'LEAD.md'), ('?file=knowledge', 'Knowledge notes / todos / plans')]:
            body = await (await client.get('/tower/projects/p_demo' + query)).text()
            assert '<title>Demo</title>' in body
            assert '<h1>Demo</h1>' in body
            assert f'<details><summary>Read {label}</summary><article>' in body
            assert 'method="get"' in body and '<button>Open</button>' in body
            assert 'min-height:44px' in body and 'min-width:44px' in body
            assert 'a:focus-visible,select:focus-visible,button:focus-visible,summary:focus-visible' in body
            assert 'outline:3px solid var(--accent)' in body
            assert 'flex-wrap:wrap' in body and 'min-width:0' in body
            assert '<h2>Acceptance</h2>' not in body
            assert 'No answer reported' not in body
        p_root = Path(p['root'])
        p_root.joinpath('projection.json').unlink()
        body = await (await client.get('/tower/projects/')).text()
        assert 'Unreported' not in body
        assert 'missing, unreadable or invalid' in body and 'project owner must refresh or configure' in body
        body = await (await client.get('/tower/projects/p_demo?file=knowledge')).text()
        assert 'Knowledge unavailable:' in body
        p_root.joinpath('LEAD.md').unlink()
        body = await (await client.get('/tower/projects/p_demo')).text()
        assert 'project owner must restore or configure the shared document' in body


@pytest.mark.parametrize('state,value,label', [('accepted', 'Accept', 'Accepted'), ('not-yet', 'Not yet', 'Not yet accepted')])
async def test_plain_acceptance_evidence(fixture, state, value, label):
    pages, p, data = fixture
    data['acceptance'] = dict(project_id=p['id'], outcome_id=p['outcome_id'], state=state,
        request_state='answered', request_id='r', requested_at=data['generated_at'], provenance='Nod', reference='request',
        answer=dict(type='nod.answer', id='a', project_id=p['id'], outcome_id=p['outcome_id'], request_id='r',
                    value=value, answered_at=data['generated_at'], provenance='Nod answer', reference='answer'))
    Path(p['root'], 'projection.json').write_text(json.dumps(data))
    app = web.Application()
    pages.install(app)
    async with TestClient(TestServer(app)) as client:
        body = await (await client.get('/tower/projects/p_demo')).text()
        assert f'<dt>State</dt><dd>{label}</dd>' in body
        assert f'<dt>Actual answer</dt><dd>{value}</dd>' in body
        assert '<details><summary>Acceptance technical details</summary>' in body
        assert '&quot;requested_at&quot;' in body and '&quot;answered_at&quot;' in body
        assert '&quot;provenance&quot;: &quot;Nod answer&quot;' in body


@pytest.mark.parametrize('needs_you', [True, False, None])
async def test_compact_semantic_summary(fixture, needs_you):
    pages, p, data = fixture
    data['summary'] = {'observed_at': data['generated_at'], 'provenance': 'Lead', 'reference': 'Current outcome',
                       'usable now': 'The phone page is ready.', 'left': 'Finish review.', 'waiting on': 'Your review.'}
    if needs_you is not None:
        data['summary']['needs_you'] = needs_you
    Path(p['root'], 'LEAD.md').write_text('# Lead\nUsable now: MAINe3654d2 /private/source with lots of internal details\nLeft: ' + 'long ' * 50)
    Path(p['root'], 'projection.json').write_text(json.dumps(data))
    app = web.Application()
    pages.install(app)
    async with TestClient(TestServer(app)) as client:
        overview = await (await client.get('/tower/projects/')).text()
        detail = await (await client.get('/tower/projects/p_demo')).text()
        assert ('<b class="needs-you">Needs you</b>' in overview) == (needs_you is True)
        assert 'The phone page is ready.' not in overview
        assert '<dd>The phone page is ready.</dd>' in detail
        assert 'Oldest update just now' in overview and 'Updated just now' in detail
        assert 'Shared snapshots' not in detail and 'Unreported' not in detail
        assert '<h2>Acceptance</h2>' not in detail
        assert 'MAINe3654d2' not in detail.split('<summary>Project details</summary>')[0]
    for invalid in ('true', 1, {}, []):
        data['summary']['needs_you'] = invalid
        with pytest.raises(ValueError):
            project_schema.projection(data, p)


def test_word_boundary_and_time():
    assert compact('A simple sentence.', 100) == 'A simple sentence.'
    assert compact('A small sentence with several words.', 24) == 'A small sentence with…'
    assert relative_time(None) == ''
    assert 'title=' in relative_time(datetime.now(timezone.utc).isoformat(), 'Updated ')


def test_unfinished_acceptance_hidden(fixture):
    pages, p, data = fixture
    data['acceptance'] = dict(project_id=p['id'], outcome_id=p['outcome_id'], state='unfinished',
                              request_state='none', provenance='Lead', reference='Current outcome')
    assert pages.acceptance(data) == ''
    data['acceptance']['state'] = 'finished-awaiting-acceptance'
    assert 'Finished; awaiting acceptance' in pages.acceptance(data)


async def test_knowledge_true_age_and_stale_source(fixture):
    pages,p,data=fixture
    data['knowledge']['observed_at']='2026-01-01T00:00:00Z'
    data['summary']={'observed_at':'2026-01-01T00:00:00Z','reference':'source digest',
                     'provenance':'Reviewed source · Source changed; owner must write short plain headers.'}
    Path(p['root'],'projection.json').write_text(json.dumps(data))
    app=web.Application(); pages.install(app)
    async with TestClient(TestServer(app)) as client:
        body=await (await client.get('/tower/projects/p_demo?file=knowledge')).text()
        assert 'Notes and todos from ' in body
        assert 'Source changed. The owner must shorten its plain progress header.' in body
    for invalid in (False, None, '2099-01-01T00:00:00Z'):
        data['knowledge']['observed_at']=invalid
        with pytest.raises(ValueError): project_schema.projection(data,p)
