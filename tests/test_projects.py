import json
from datetime import datetime, timezone

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tower.projects import Projects, header, markdown, read_file


@pytest.fixture
def fixture(tmp_path):
    (tmp_path / 'LEAD.md').write_text('# Demo\nUsable now: source\nLeft: integrate\nWaiting on: review\nAcceptance: accepted\n## History\nUsable now: stale')
    p = {'id': 'p_demo', 'title': 'Demo', 'root': str(tmp_path), 'outcome_id': 'o2',
         'status': 'held', 'header_file': 'lead', 'files': [{'id': 'lead', 'label': 'LEAD.md', 'path': 'LEAD.md'}],
         'export': {'root': str(tmp_path), 'path': 'projection.json'}}
    data = {'version': 1, 'project_id': 'p_demo', 'outcome_id': 'o2',
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'knowledge': {'state': 'available', 'items': [{'id': 'todo-1', 'kind': 'todo', 'title': 'Task', 'body': 'Details', 'completed': False}]}}
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
    data['acceptance'].update(request_state='answered', request_id='r', provenance='Nod', answer={'id': 'a', 'value': 'Accept', 'answered_at': data['generated_at']})
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
