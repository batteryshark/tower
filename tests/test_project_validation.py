"""Malformed evidence never becomes acceptance or blanks unrelated projects."""
import copy
import json
from datetime import datetime, timedelta, timezone

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tower.projects import Projects, markdown
from tower import project_schema as schema


def sources(tmp_path):
    now = datetime.now(timezone.utc)
    at = (now - timedelta(seconds=2)).isoformat()
    p = {'id': 'p1', 'outcome_id': 'o1', 'title': 'Project One', 'status': 'native-blocked',
         'root': str(tmp_path), 'header_file': 'lead',
         'files': [{'id': 'lead', 'label': 'Lead', 'path': 'lead/report.md'}],
         'images': [{'id': 'raster', 'path': 'images/ok.png'}, {'id': 'escape', 'path': '../outside.png'},
                    {'id': 'svg', 'path': 'images/x.svg'}, {'id': 'link', 'path': 'images/link.png'}],
         'export': {'root': str(tmp_path), 'path': 'export.json'}}
    (tmp_path / 'lead').mkdir()
    (tmp_path / 'images').mkdir()
    (tmp_path / 'lead/report.md').write_text('# Project\nUsable now: Fresh header\n![yes](../images/ok.png)\n![no](../../outside.png)\n![remote](https://evil/x.png)\n![svg](../images/x.svg)\n![not listed](../images/unlisted.png)')
    (tmp_path / 'images/ok.png').write_bytes(b'\x89PNG\r\n\x1a\n' + b'test')
    (tmp_path / 'images/x.svg').write_text('<svg onload="alert(1)"/>')
    (tmp_path / 'images/link.png').symlink_to(tmp_path / 'images/ok.png')
    a = {'project_id': 'p1', 'outcome_id': 'o1', 'state': 'accepted', 'request_state': 'answered',
         'request_id': 'r1', 'requested_at': at, 'provenance': 'chief Nod', 'reference': 'outcome o1',
         'answer': {'type': 'nod.answer', 'id': 'a1', 'request_id': 'r1', 'project_id': 'p1', 'outcome_id': 'o1',
                    'value': 'Accept', 'answered_at': at, 'provenance': 'Nod response', 'reference': 'answer a1'}}
    data = {'version': 1, 'project_id': 'p1', 'outcome_id': 'o1', 'generated_at': at,
            'acceptance': a, 'summary': {'observed_at': at, 'provenance': 'owner note', 'reference': 'note-1',
                                        'usable now': 'Old snapshot', 'left': 'Remaining work'},
            'knowledge': {'state': 'available', 'provenance': 'supported native read', 'items': [
                {'id': 't1', 'kind': 'todo', 'title': 'Todo', 'body': '- [x] Complete', 'completed': True}]},
            'activity': [{'id': 'event1', 'project_id': 'p1', 'outcome_id': 'o1', 'kind': 'landing', 'at': at,
                          'label': 'Actual source landing', 'reference': 'commit abc', 'provenance': 'Git'}]}
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'version': 1, 'projects': [p]}))
    (tmp_path / 'export.json').write_text(json.dumps(data))
    return p, data, manifest


@pytest.mark.parametrize('field,value', [('acceptance', None), ('acceptance', []), ('knowledge', []),
    ('knowledge', {'state': 'available', 'provenance': 'API', 'items': [None]}),
    ('activity', {}), ('activity', [None]), ('summary', []), ('version', True),
    ('generated_at', 'tomorrow'), ('outcome_id', 'other')])
async def test_bad_export_isolated(tmp_path, field, value):
    p, data, manifest = sources(tmp_path)
    data[field] = value
    (tmp_path / 'export.json').write_text(json.dumps(data))
    other = dict(p, id='p2', title='Project Two')
    manifest.write_text(json.dumps({'version': 1, 'projects': [p, other]}))
    app = web.Application()
    Projects(str(manifest)).install(app)
    async with TestClient(TestServer(app)) as client:
        response = await client.get('/tower/projects/')
        body = await response.text()
        assert response.status == 200 and 'Project One' in body and 'Project Two' in body
        assert 'projection unavailable or invalid' in body and '<b>acceptance:</b> accepted' not in body


@pytest.mark.parametrize('key,value', [('request_id', 'wrong'), ('project_id', 'wrong'), ('outcome_id', 'old'),
    ('type', 'prose'), ('value', 'accepted'), ('id', ''), ('provenance', []), ('reference', ''),
    ('answered_at', '2020-01-01T00:00:00Z')])
def test_answer_binding(tmp_path, key, value):
    p, data, _ = sources(tmp_path)
    data['acceptance']['answer'][key] = value
    with pytest.raises(ValueError):
        schema.projection(data, p)


@pytest.mark.parametrize('state', ['unfinished', 'finished-awaiting-acceptance', 'accepted', 'not-yet'])
@pytest.mark.parametrize('value', ['missing', None, 'Accept', 'Not yet'])
def test_answer_requires_terminal_state_and_value(tmp_path, state, value):
    p, data, _ = sources(tmp_path)
    data['acceptance']['state'] = state
    if value == 'missing':
        del data['acceptance']['answer']['value']
    else:
        data['acceptance']['answer']['value'] = value
    if (state, value) in (('accepted', 'Accept'), ('not-yet', 'Not yet')):
        assert schema.projection(data, p) is data
    else:
        with pytest.raises(ValueError, match='answer value/state mismatch'):
            schema.projection(data, p)


async def test_invalid_utf8_export_isolated(tmp_path):
    p, data, manifest = sources(tmp_path)
    (tmp_path / 'export.json').write_bytes(b'{"version":1,"bad":"\xff"}')
    other = dict(p, id='p2', title='Valid Project', export={'root': str(tmp_path), 'path': 'good.json'})
    good = copy.deepcopy(data)
    good['project_id'] = 'p2'
    good.pop('acceptance')
    good['activity'] = []
    good['summary']['left'] = 'GOOD PROJECT CONTENT'
    (tmp_path / 'good.json').write_text(json.dumps(good))
    manifest.write_text(json.dumps({'version': 1, 'projects': [p, other]}))
    app = web.Application()
    Projects(str(manifest)).install(app)
    async with TestClient(TestServer(app)) as client:
        response = await client.get('/tower/projects/')
        body = await response.text()
        assert response.status == 200
        assert 'Project One' in body and 'projection unavailable or invalid' in body
        assert 'Valid Project' in body and 'GOOD PROJECT CONTENT' in body


@pytest.mark.parametrize('value', [0, 1, 'true', None, [], {}])
def test_todo_bool(tmp_path, value):
    p, data, _ = sources(tmp_path)
    data['knowledge']['items'][0]['completed'] = value
    with pytest.raises(ValueError):
        schema.projection(data, p)


async def test_deep_json_and_bad_manifest_entry(tmp_path):
    p, data, manifest = sources(tmp_path)
    (tmp_path / 'export.json').write_text('[' * 2000 + '0' + ']' * 2000)
    manifest.write_text(json.dumps({'version': 1, 'projects': [None, p, dict(p, id='p2', title='Still visible', files=None)]}))
    app = web.Application()
    Projects(str(manifest)).install(app)
    async with TestClient(TestServer(app)) as client:
        response = await client.get('/tower/projects/')
        body = await response.text()
        assert response.status == 200 and body.count('Unavailable project entry') == 2
        assert 'Project One' in body and 'projection unavailable or invalid' in body


async def test_image_routes_summary_and_activity(tmp_path):
    p, data, manifest = sources(tmp_path)
    pages = Projects(str(manifest))
    app = web.Application()
    pages.install(app)
    async with TestClient(TestServer(app)) as client:
        response = await client.get('/tower/projects/p1')
        body = await response.text()
        assert response.status == 200
        assert 'Fresh header' in body and 'Remaining work' in body and 'observed ' in body
        assert 'src="/tower/projects/p1/images/raster"' in body and 'https://evil' not in body
        assert '<img alt="svg"' not in body
        response = await client.get('/tower/projects/p1/images/raster')
        assert response.status == 200 and response.content_type == 'image/png'
        assert response.headers['X-Content-Type-Options'] == 'nosniff'
        for name in ('escape', 'svg', 'link', 'unlisted', '%2e%2e%2foutside.png'):
            assert (await client.get('/tower/projects/p1/images/' + name)).status == 404
        overview = await (await client.get('/tower/projects/')).text()
        assert '<b>acceptance:</b> accepted' in overview and 'Project One · landing' in overview
        assert 'Actual source landing' in overview
    data['summary']['usable now'] = 'x' * 2001
    with pytest.raises(ValueError):
        schema.projection(data, p)


def test_markdown_features():
    result = markdown('**Bold** and *italic*\n1. First\n2. Second\n\n- [x] Done\n- [ ] Next\n\n| Name | State |\n| --- | --- |\n| One | **ready** |\n\n<script>x</script>\n[x](data:text/html,bad)\n```\n**literal**\n```', {})
    for expected in ('<strong>Bold</strong>', '<em>italic</em>', '<ol>', '<ul>', 'disabled checked', '<table>', '<th>Name</th>', '<strong>ready</strong>', '&lt;script&gt;', '**literal**'):
        assert expected in result
    assert '<script>' not in result and 'href="data:' not in result


def test_versions_and_bounded_nested(tmp_path):
    p, data, manifest = sources(tmp_path)
    manifest.write_text(json.dumps({'version': True, 'projects': [p]}))
    with pytest.raises(ValueError):
        Projects(str(manifest)).load()
    data['extra'] = current = {}
    for _ in range(20):
        current['nested'] = {}
        current = current['nested']
    with pytest.raises(ValueError):
        schema.projection(data, p)


async def test_all_rows_not_first_n(tmp_path):
    p, _, manifest = sources(tmp_path)
    rows = [dict(p, id=f'p{i}', title=f'Visible project {i}') for i in range(100)]
    manifest.write_text(json.dumps({'version': 1, 'projects': rows}))
    app = web.Application()
    Projects(str(manifest)).install(app)
    async with TestClient(TestServer(app)) as client:
        body = await (await client.get('/tower/projects/')).text()
        assert body.count('class="project-row"') == 100
        assert 'Visible project 99' in body and 'Visible project 0' in body
