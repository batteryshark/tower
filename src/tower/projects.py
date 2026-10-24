"""Allowlisted, read-only project pages; no discovery, network calls or writes."""
import html
import json
import os
import re
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web

LIMIT = 512 * 1024
ID = re.compile(r'^[A-Za-z0-9_-]{1,80}$')
DENIED = re.compile(r'secret|credential|token|auth|password|(^|[._-])key([._-]|$)', re.I)
HEADERS = {'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
           'Referrer-Policy': 'no-referrer', 'Content-Security-Policy':
           "default-src 'none'; style-src 'unsafe-inline'; img-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'self'"}


def read_file(root, relative, suffixes, limit=LIMIT):
    """No-follow dirfds prevent both symlink escapes and check/open races."""
    path = Path(relative)
    root = Path(root)
    if root.resolve() != root.absolute():
        raise ValueError('root must be canonical')
    if path.is_absolute() or not path.parts or any(
        p in ('.', '..') or p.startswith('.') or '%' in p or '\\' in p or DENIED.search(p)
        for p in path.parts
    ) or path.suffix.lower() not in suffixes:
        raise ValueError('file not permitted')
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        child = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(child, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
                raise ValueError('not bounded regular file')
            data = stream.read(limit + 1)
            if len(data) > limit:
                raise ValueError('file too large')
            return data
    finally:
        os.close(fd)


def stamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed if parsed.tzinfo else None
    except (ValueError, TypeError, AttributeError):
        return None


def esc(value):
    return html.escape(str(value if value is not None else 'Unreported'))


def header(source):
    result = {}
    for line in source.splitlines()[:30]:
        if line.startswith('## '):
            break
        key, sep, value = line.partition(':')
        if sep and key.lower() in ('usable now', 'left', 'waiting on', 'handed to'):
            result.setdefault(key.lower(), value.strip()[:2000])
    return result


def markdown(source, images):
    """Safe Markdown subset: headings, paragraphs, lists, fences, links and raster images.

    Raw HTML is escaped. Local images must match an explicit approved mapping.
    """
    def inline(value):
        parts, cursor = [], 0
        for match in re.finditer(r'(!?)\[([^\]\n]*)\]\(([^\s)]+)\)', value):
            parts.append(esc(value[cursor:match.start()]))
            image, label, target = match.groups()
            url = urlsplit(target)
            if image and target in images:
                parts.append(f'<img alt="{esc(label)}" src="{esc(images[target])}" loading="lazy">')
            elif not image and url.scheme in ('https', 'http') and url.netloc and not url.username:
                parts.append(f'<a rel="noreferrer noopener" href="{esc(target)}">{esc(label)}</a>')
            else:
                parts.append(esc(label))
            cursor = match.end()
        return ''.join(parts) + esc(value[cursor:])
    out, code = [], False
    for line in source.splitlines():
        if line.startswith('```'):
            out.append('</code></pre>' if code else '<pre><code>')
            code = not code
        elif code:
            out.append(esc(line) + '\n')
        elif re.match(r'^#{1,6} ', line):
            level = len(line.split(' ', 1)[0])
            out.append(f'<h{level}>' + inline(line[level+1:]) + f'</h{level}>')
        elif line.strip():
            out.append('<p>' + ('• ' if line.startswith(('- ', '* ')) else '') + inline(line[2:] if line.startswith(('- ', '* ')) else line) + '</p>')
    return ''.join(out) + ('</code></pre>' if code else '')


class Projects:
    def __init__(self, manifest=None):
        self.manifest = manifest or os.environ.get('TOWER_PROJECT_MANIFEST')

    def load(self):
        if not self.manifest:
            return [], 'Project manifest unavailable — operator configuration required.'
        path = Path(self.manifest)
        data = json.loads(read_file(path.parent, path.name, {'.json'}))
        if data['version'] != 1 or len(data['projects']) > 100:
            raise ValueError('invalid manifest')
        ids = set()
        for p in data['projects']:
            if not ID.fullmatch(p['id']) or p['id'] in ids or not Path(p['root']).is_absolute():
                raise ValueError('invalid project')
            ids.add(p['id'])
            for group in ('files', 'images'):
                entries = p.get(group, [])
                if len(entries) > 100 or len({e['id'] for e in entries}) != len(entries) or any(not ID.fullmatch(e['id']) or e['id'] == 'knowledge' for e in entries):
                    raise ValueError('invalid file IDs')
        return data['projects'], ''

    def projection(self, p):
        try:
            e = p['export']
            data = json.loads(read_file(e['root'], e['path'], {'.json'}))
            if data['version'] != 1 or data['project_id'] != p['id'] or data['outcome_id'] != p['outcome_id'] or not stamp(data['generated_at']):
                raise ValueError('projection mismatch')
            return data, ''
        except (KeyError, ValueError, OSError, TypeError):
            return {}, 'Knowledge / acceptance projection unavailable or invalid.'

    def activity(self, p, data):
        return [e for e in data.get('activity', [])[:500] if isinstance(e, dict)
                and e.get('project_id') == p['id'] and e.get('outcome_id') == p['outcome_id']
                and e.get('id') and e.get('kind') in ('landing', 'acceptance', 'decision', 'handoff')
                and stamp(e.get('at')) and e.get('provenance') and e.get('reference')]

    def summary(self, p, data):
        fields = {}
        try:
            f = next(f for f in p.get('files', []) if f['id'] == p.get('header_file'))
            fields = header(read_file(f.get('root', p['root']), f['path'], {'.md'}).decode())
        except (StopIteration, ValueError, OSError, UnicodeError):
            pass
        a = data.get('acceptance', {})
        state, answer = a.get('state'), a.get('answer') or {}
        expected = {'accepted': 'Accept', 'not-yet': 'Not yet'}
        if state in expected and not (a.get('request_state') == 'answered' and answer.get('value') == expected[state] and stamp(answer.get('answered_at')) and answer.get('id') and a.get('request_id') and a.get('provenance')):
            state = None
        fields['acceptance'] = state if state in ('unfinished', 'finished-awaiting-acceptance', 'accepted', 'not-yet') else 'unreported'
        fields['status'] = p.get('status', 'unreported')
        fields['last landed'] = max((e['at'] for e in self.activity(p, data) if e['kind'] == 'landing'), key=stamp, default='Unreported')
        return fields

    def install(self, app):
        app.router.add_get('/tower/projects', self.redirect, allow_head=False)
        app.router.add_get('/tower/projects/', self.page, allow_head=False)
        app.router.add_get('/tower/projects/{project}', self.page, allow_head=False)
        app.router.add_get('/tower/projects/{project}/images/{image}', self.image, allow_head=False)

    async def redirect(self, request):
        raise web.HTTPFound('/tower/projects/')

    def selected(self, request):
        projects, error = self.load()
        pid = request.match_info.get('project')
        if pid and not ID.fullmatch(pid):
            raise web.HTTPNotFound()
        p = next((p for p in projects if p['id'] == pid), None)
        if pid and not p:
            raise web.HTTPNotFound()
        return projects, error, p

    async def image(self, request):
        try:
            _, _, p = self.selected(request)
            image = next(i for i in p.get('images', []) if i['id'] == request.match_info['image'])
            data = read_file(p['root'], image['path'], {'.png', '.jpg', '.jpeg', '.gif', '.webp'}, 2*1024*1024)
            if data.startswith(b'\x89PNG\r\n\x1a\n'):
                mime = 'image/png'
            elif data.startswith(b'\xff\xd8\xff'):
                mime = 'image/jpeg'
            elif data.startswith((b'GIF87a', b'GIF89a')):
                mime = 'image/gif'
            elif data[:4] == b'RIFF' and data[8:12] == b'WEBP':
                mime = 'image/webp'
            else:
                raise ValueError('invalid raster')
            return web.Response(body=data, content_type=mime, headers=HEADERS)
        except (KeyError, StopIteration, ValueError, OSError, TypeError):
            raise web.HTTPNotFound()

    async def page(self, request):
        try:
            projects, error, selected = self.selected(request)
            body, feed = (f'<p>{esc(error)}</p>' if error else ''), []
            for p in ([selected] if selected else projects):
                data, unavailable = self.projection(p)
                fields = self.summary(p, data)
                body += f'<section><h2><a href="/tower/projects/{p["id"]}">{esc(p["title"])}</a></h2><dl>'
                for key in ('status', 'usable now', 'left', 'waiting on', 'acceptance', 'handed to', 'last landed'):
                    body += f'<div><dt>{esc(key)}</dt><dd>{esc(fields.get(key))}</dd></div>'
                body += '</dl><small>Header: ' + esc(p.get('header_file')) + ' · outcome ' + esc(p['outcome_id']) + ' · projection as of ' + esc(data.get('generated_at')) + '</small></section>'
                feed.extend(self.activity(p, data))
                if selected:
                    body += f'<p role="status">{esc(unavailable)}</p>' if unavailable else ''
                    body += self.files(p, data, request.query.get('file'))
                    body += '<h3>Acceptance provenance</h3><pre>' + esc(json.dumps(data.get('acceptance', {}), indent=2)) + '</pre>'
            now = datetime.now(timezone.utc)
            body += '<h2>Recent activity · last 6 hours</h2><p>Factual source timestamps; no file-mtime inference.</p>'
            recent = sorted((e for e in feed if now-timedelta(hours=6) <= stamp(e['at']) <= now), key=lambda e: stamp(e['at']), reverse=True)
            seen = set()
            for e in recent:
                if e['id'] in seen:
                    continue
                seen.add(e['id'])
                body += '<section>' + esc(e['kind']) + ' · ' + esc(e['at']) + '<p>' + esc(e.get('label')) + '</p><small>' + esc(e['reference']) + ' · ' + esc(e['provenance']) + '</small></section>'
            if not recent:
                body += '<p>No reported semantic activity in this window.</p>'
            css = Path(__file__).with_name('viewer.html').read_text().split('<style>')[1].split('</style>')[0]
            css += 'a{color:var(--accent)} section{background:var(--card);padding:12px;border-radius:10px;margin:8px 0} dl{margin:0} dl>div{display:grid;grid-template-columns:9rem 1fr;gap:8px} dt{color:var(--muted)} dd{margin:0} p,dd,small{overflow-wrap:anywhere} img{max-width:100%;height:auto} select{max-width:100%;font:inherit} article{min-width:0} @media(max-width:420px){dl>div{grid-template-columns:6rem 1fr}}'
            document = '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Tower projects</title><style>' + css + '</style><header><h1>Tower projects</h1><nav><a href="/tower/">Events</a> · <a href="/tower/projects/">All projects</a></nav></header><main>' + body + '</main></html>'
            if len(document.encode()) > 2 * 1024 * 1024:
                raise ValueError('response too large')
            return web.Response(text=document, content_type='text/html', headers=HEADERS)
        except (KeyError, ValueError, OSError, TypeError, UnicodeError):
            return web.Response(text='Project configuration unavailable or invalid.', status=503, headers=HEADERS)

    def files(self, p, data, chosen):
        files = p.get('files', [])
        knowledge = data.get('knowledge', {})
        entries = knowledge.get('items', [])[:200] if knowledge.get('state') == 'available' else []
        options = [(f['id'], f['label']) for f in files] + [('knowledge', 'Knowledge notes / todos / plans')]
        chosen = chosen or options[0][0]
        body = '<form method="get"><label>File <select name="file">'
        for fid, label in options:
            body += f'<option value="{esc(fid)}"' + (' selected' if fid == chosen else '') + '>' + esc(label) + '</option>'
        body += '</select></label> <button>Open</button></form><article>'
        if chosen == 'knowledge':
            body += '<h2>Knowledge</h2><p>' + esc(knowledge.get('state', 'unavailable')) + ' · ' + esc(knowledge.get('provenance')) + '</p>'
            for item in entries:
                body += '<h3>' + esc(item.get('title')) + '</h3><small>ID ' + esc(item.get('id')) + ' · ' + esc(item.get('kind')) + ' · completed: ' + esc(item.get('completed')) + '</small>' + markdown(str(item.get('body', ''))[:50000], {})
        else:
            f = next((f for f in files if f['id'] == chosen), None)
            if not f:
                raise web.HTTPNotFound()
            try:
                root, images = f.get('root', p['root']), {}
                if root == p['root']:
                    for image in p.get('images', []):
                        rel = os.path.relpath(image['path'], str(Path(f['path']).parent))
                        if '..' not in Path(rel).parts:
                            images[rel] = f'/tower/projects/{p["id"]}/images/{image["id"]}'
                body += markdown(read_file(root, f['path'], {'.md'}).decode(), images)
            except (ValueError, OSError, UnicodeError):
                body += '<p>Selected file unavailable or not permitted.</p>'
        return body + '</article>'
