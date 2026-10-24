"""Allowlisted, read-only project pages; no discovery, network calls or writes."""
import html
import json
import os
import re
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlencode

from aiohttp import web
from . import project_schema as schema

LIMIT = 512 * 1024
ID = re.compile(r'^[A-Za-z0-9_-]{1,80}$')
DENIED = re.compile(r'secret|credential|token|auth|password|(^|[._-])key([._-]|$)', re.I)
HEADERS = {'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
           'Referrer-Policy': 'no-referrer', 'Content-Security-Policy':
           "default-src 'none'; style-src 'unsafe-inline'; img-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'self'"}


def read_file(root, relative, suffixes, limit=LIMIT):
    """No-follow opens beneath an operator-controlled canonical root.

    Root ancestors must remain trusted; this is not universal TOCTOU protection.
    """
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
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
                raise ValueError('not bounded regular file')
            data = stream.read(limit + 1)
            if len(data) > limit:
                raise ValueError('file too large')
            return data
    finally:
        os.close(fd)


def stamp(value):
    return schema.stamp(value)


def esc(value):
    return html.escape(str(value if value is not None else ''))


def compact(value, limit=100):
    """A display boundary, not a translation of the source language."""
    value = ' '.join(str(value or '').split())
    if len(value) <= limit:
        return value
    end = value.rfind(' ', 0, limit)
    return value[:end if end > limit // 2 else limit].rstrip(' ,;:') + '…'


def principal_sentence(value):
    # Conservative fallback only; the projection writer owns plain-language editing.
    return len(value) <= 100 and not re.search(r'[0-9a-f]{7,}|(?:ses_|msg_|ev\.)\w+|[/\\]|\b[A-Z]{3,}\d+', value, re.I)


def relative_time(value, prefix=''):
    parsed = stamp(value)
    if not parsed:
        return ''
    seconds = max(0, int((datetime.now(timezone.utc) - parsed).total_seconds()))
    age = ('just now' if seconds < 60 else f'{seconds // 60}m ago' if seconds < 3600
           else f'{seconds // 3600}h ago' if seconds < 86400 else f'{seconds // 86400}d ago')
    return f'<time datetime="{esc(value)}" title="{esc(parsed.astimezone().strftime("%c %Z"))}">{esc(prefix + age)}</time>'


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
        def emphasis(raw):
            safe = esc(raw)
            safe = re.sub(r'`([^`\n]+)`', r'<code>\1</code>', safe)
            safe = re.sub(r'\*\*([^*\n]+)\*\*', r'<strong>\1</strong>', safe)
            return re.sub(r'(?<!\*)\*([^*\n]+)\*(?!\*)', r'<em>\1</em>', safe)
        parts, cursor = [], 0
        for match in re.finditer(r'(!?)\[([^\]\n]*)\]\(([^\s)]+)\)', value):
            parts.append(emphasis(value[cursor:match.start()]))
            image, label, target = match.groups()
            try:
                url = urlsplit(target)
            except ValueError:
                url = urlsplit('')
            if image and target in images:
                parts.append(f'<img alt="{esc(label)}" src="{esc(images[target])}" loading="lazy">')
            elif not image and url.scheme in ('https', 'http') and url.netloc and not url.username:
                parts.append(f'<a rel="noreferrer noopener" href="{esc(target)}">{esc(label)}</a>')
            else:
                parts.append(esc(label))
            cursor = match.end()
        return ''.join(parts) + emphasis(value[cursor:])
    out, code, listing = [], False, None
    lines = source.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        index += 1
        match = re.match(r'^\s*(?:(\d+)\. |([-*]) )(.*)$', line)
        if listing and (not match or ('ol' if match[1] else 'ul') != listing):
            out.append(f'</{listing}>')
            listing = None
        if line.startswith('```'):
            out.append('</code></pre>' if code else '<pre><code>')
            code = not code
        elif code:
            out.append(esc(line) + '\n')
        elif '|' in line and index < len(lines) and re.fullmatch(r'\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*', lines[index]):
            def cells(row, tag):
                return '<tr>' + ''.join(f'<{tag}>' + inline(c.strip()) + f'</{tag}>' for c in row.strip().strip('|').split('|')) + '</tr>'
            out.append('<div class="table-scroll"><table><thead>' + cells(line, 'th') + '</thead><tbody>')
            index += 1
            while index < len(lines) and '|' in lines[index] and lines[index].strip():
                out.append(cells(lines[index], 'td'))
                index += 1
            out.append('</tbody></table></div>')
        elif re.match(r'^#{1,6} ', line):
            level = len(line.split(' ', 1)[0])
            out.append(f'<h{level}>' + inline(line[level+1:]) + f'</h{level}>')
        elif match:
            kind = 'ol' if match[1] else 'ul'
            if not listing:
                out.append(f'<{kind}>')
                listing = kind
            content = match[3]
            task = re.match(r'^\[([ xX])\] (.*)$', content)
            if task:
                content = task[2]
            out.append('<li>' + ('<input type="checkbox" disabled' + (' checked' if task[1].lower() == 'x' else '') + '>' if task else '') + inline(content) + '</li>')
        elif line.strip():
            out.append('<p>' + inline(line) + '</p>')
    return ''.join(out) + (f'</{listing}>' if listing else '') + ('</code></pre>' if code else '')


class Projects:
    def __init__(self, manifest=None):
        self.manifest = manifest or os.environ.get('TOWER_PROJECT_MANIFEST')

    def load(self):
        if not self.manifest:
            return [], 'Project manifest unavailable — the project owner must configure its shared snapshot.'
        path = Path(self.manifest)
        data = json.loads(read_file(path.parent, path.name, {'.json'}))
        schema.version(data)
        schema.keys(data, 'version projects inventory')
        error = ''
        if 'inventory' in data:
            inventory = data['inventory']
            schema.keys(inventory, 'complete observed_at provenance answers_available')
            if type(inventory.get('complete')) is not bool: raise ValueError('invalid inventory')
            schema.time(inventory.get('observed_at')); schema.text(inventory.get('provenance'),500)
            if not inventory['complete']: error = 'Project inventory incomplete — retained safe rows may be stale.'
            if 'answers_available' in inventory:
                if type(inventory['answers_available']) is not bool: raise ValueError('invalid answer availability')
                if not inventory['answers_available']: error += ' Answer source unavailable — previous verified activity retained.'
        schema.seq(data.get('projects'), 1000)
        ids = set()
        projects = []
        for p in data['projects']:
            try:
                schema.project(p)
                if p['id'] in ids:
                    raise ValueError('duplicate project ID')
                ids.add(p['id'])
                projects.append(p)
            except (ValueError, TypeError, RecursionError):
                # Retain an explicit unavailable row, but no file/export access.
                projects.append({'id': '', 'title': 'Unavailable project entry', '_invalid': True})
        return projects, error

    def projection(self, p):
        try:
            e = p['export']
            data = json.loads(read_file(e['root'], e['path'], {'.json'}))
            return schema.projection(data, p), ''
        except (KeyError, ValueError, OSError, TypeError, UnicodeError, RecursionError):
            return {}, 'Knowledge / acceptance snapshot unavailable: missing, unreadable or invalid. The project owner must refresh or configure its shared snapshot.'

    def activity(self, p, data):
        return [e for e in data.get('activity', [])[:500] if isinstance(e, dict)
                and e.get('project_id') == p['id'] and e.get('outcome_id') == p['outcome_id']
                and e.get('id') and e.get('kind') in ('landing', 'acceptance', 'decision', 'handoff', 'status', 'file')
                and stamp(e.get('at')) and e.get('provenance') and e.get('reference')]

    def summary(self, p, data):
        try:
            schema.projection(data, p)
        except (KeyError, ValueError, TypeError, RecursionError):
            data = {}
        fields = {k: v for k, v in data.get('summary', {}).items()
                  if k in ('usable now', 'left', 'waiting on', 'handed to')}
        fields['_source'] = ('Snapshot: ' + data['summary']['provenance'] + ' · ' + data['summary']['reference'] + ' · observed ' + data['summary']['observed_at']) if 'summary' in data else ''
        fields['needs_you'] = data.get('summary', {}).get('needs_you') is True
        try:
            f = next(f for f in p.get('files', []) if f['id'] == p.get('header_file'))
            current = header(read_file(f.get('root', p['root']), f['path'], {'.md'}).decode())
            fields['_header'] = current
            for key, value in current.items():
                if key not in fields and principal_sentence(value):
                    fields[key] = value
        except (StopIteration, ValueError, OSError, UnicodeError):
            pass
        a = data.get('acceptance', {})
        state, answer = a.get('state'), a.get('answer') or {}
        expected = {'accepted': 'Accept', 'not-yet': 'Not yet'}
        if state in expected and not (a.get('request_state') == 'answered' and answer.get('value') == expected[state] and stamp(answer.get('answered_at')) and answer.get('id') and a.get('request_id') and a.get('provenance')):
            state = None
        fields['acceptance'] = state if state in ('unfinished', 'finished-awaiting-acceptance', 'accepted', 'not-yet') else 'unreported'
        fields['status'] = p.get('status')
        fields['last landed'] = max((e['at'] for e in self.activity(p, data) if e['kind'] == 'landing'), key=stamp, default=None)
        for key in ('status', 'usable now', 'left', 'waiting on', 'handed to'):
            if str(fields.get(key, '')).strip().lower() == 'unreported':
                fields.pop(key, None)
        return fields

    def acceptance(self, data):
        """Plain evidence from a validated snapshot, never an inferred answer."""
        a = data.get('acceptance', {})
        if a.get('state') not in ('finished-awaiting-acceptance', 'accepted', 'not-yet'):
            return ''
        states = {'unfinished': 'Not finished', 'finished-awaiting-acceptance': 'Finished; awaiting acceptance',
                  'accepted': 'Accepted', 'not-yet': 'Not yet accepted'}
        request_states = {'none': 'Not requested', 'pending': 'Awaiting answer',
                          'expired': 'Expired', 'cancelled': 'Cancelled', 'answered': 'Answered'}
        answer = a.get('answer') or {}
        pairs = [('State', states.get(a.get('state'))),
                 ('Request', request_states.get(a.get('request_state'))),
                 ('Actual answer', answer.get('value'))]
        return '<h2>Acceptance</h2><dl>' + ''.join(
            f'<div><dt>{esc(k)}</dt><dd>{esc(v)}</dd></div>' for k, v in pairs if v
        ) + '</dl><details><summary>Acceptance technical details</summary><pre>' + esc(json.dumps(a, indent=2)) + '</pre></details>'

    def file_selection(self, p, chosen):
        options = [(f['id'], f['label']) for f in p.get('files', [])] + [('knowledge', 'Knowledge notes / todos / plans')]
        if not chosen:
            return options, '', ''
        label = next((label for fid, label in options if fid == chosen), None)
        if label is None:
            raise web.HTTPNotFound()
        return options, chosen, label

    def install(self, app):
        app.router.add_get('/tower/projects-select.js', self.selection_script, allow_head=False)
        app.router.add_get('/tower/projects', self.redirect, allow_head=False)
        app.router.add_get('/tower/projects/', self.page, allow_head=False)
        app.router.add_get('/tower/projects/{project}', self.page, allow_head=False)
        app.router.add_get('/tower/projects/{project}/images/{image}', self.image, allow_head=False)

    async def selection_script(self, request):
        return web.Response(text="document.querySelectorAll('select[data-file-selection]').forEach(select => select.addEventListener('change', () => { if (select.value) select.form.requestSubmit(); }));",
                            content_type='application/javascript', headers=HEADERS)

    def last_activity(self, p, data):
        times = [e['at'] for e in self.activity(p, data)]
        times.extend(f['at'] for f in data.get('file_updates', []))
        if data.get('file_update'):
            times.append(data['file_update']['at'])
        answer = data.get('acceptance', {}).get('answer')
        if answer:
            times.append(answer['answered_at'])
        return max(times, key=stamp, default=None)

    def runtime_state(self, data, now=None):
        now = now or datetime.now(timezone.utc)
        runtime = data.get('runtime', {})
        sample = stamp(runtime.get('at'))
        return 'running' if (runtime.get('state') == 'running' and runtime.get('provenance') == 'docket.running_sample'
                             and sample and 0 <= (now-sample).total_seconds() <= 120) else 'unknown'

    def overview(self, rows, query, now=None):
        """Pure bounded partition; runtime unknown is never interpreted as busy."""
        now = now or datetime.now(timezone.utc)
        tab = query.get('tab', 'flight')
        search = query.get('q', '')
        page = query.get('page', '1')
        if tab not in ('flight', 'else') or len(search) > 200 or any(ord(c) < 32 for c in search) or not re.fullmatch(r'[1-9][0-9]{0,3}', page):
            raise web.HTTPBadRequest(text='Invalid overview selection')
        selected = []
        for p, data, unavailable in rows:
            last = self.last_activity(p, data)
            running = self.runtime_state(data, now) == 'running'
            flight = running or bool(last and now - timedelta(hours=48) <= stamp(last) <= now)
            if flight == (tab == 'flight') and (tab != 'else' or search.casefold() in p['title'].casefold()):
                selected.append((p, data, unavailable))
        selected.sort(key=lambda row: (stamp(self.last_activity(row[0], row[1])) or datetime.min.replace(tzinfo=timezone.utc), row[0]['title'].casefold()), reverse=True)
        pages = max(1, (len(selected) + 29) // 30)
        number = min(int(page), pages)
        return selected[(number-1)*30:number*30], tab, search, number, pages

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
        except (KeyError, StopIteration, ValueError, OSError, TypeError, RecursionError):
            raise web.HTTPNotFound()

    async def page(self, request):
        try:
            projects, error, selected = self.selected(request)
            title = 'Tower projects'
            if selected:
                _, _, label = self.file_selection(selected, request.query.get('file'))
                title = selected['title']
            body, feed, updated = (f'<p>{esc(error)}</p>' if error else ''), [], []
            invalid = sum(bool(p.get('_invalid')) for p in projects)
            if invalid:
                body += f'<p>Project information unavailable: malformed configuration ({invalid} entries). The project owner must configure its shared snapshot.</p>'
            rows = [(p, *self.projection(p)) for p in ([selected] if selected else projects) if not p.get('_invalid')]
            for p, data, _ in rows:
                feed.extend(dict(e, project_title=p['title']) for e in self.activity(p, data))
            if not selected:
                rows, tab, search, number, pages = self.overview(rows, request.query)
                body += '<nav><a href="?tab=flight">In flight</a><a href="?tab=else">Everything else</a></nav>'
                if tab == 'else':
                    body += '<form method="get"><input type="hidden" name="tab" value="else"><label>Project name <input name="q" maxlength="200" value="' + esc(search) + '"></label><button>Search</button></form>'
                body += f'<p>{"In flight" if tab == "flight" else "Everything else"} · Page {number} of {pages}</p><nav>'
                for n, label in ((number-1, 'Previous'), (number+1, 'Next')):
                    if 1 <= n <= pages:
                        body += '<a href="?' + esc(urlencode(dict(tab=tab, q=search, page=n))) + '">' + label + '</a>'
                body += '</nav>'
            for p, data, unavailable in rows:
                if p.get('_invalid'):
                    body += '<section class="project-row">Project information unavailable: malformed configuration. The project owner must configure its shared snapshot.</section>'
                    continue
                fields = self.summary(p, data)
                observed = data.get('summary', {}).get('observed_at')
                if observed:
                    updated.append(observed)
                body += '<section class="project-row">'
                if not selected:
                    body += f'<a class="project-link" href="/tower/projects/{p["id"]}"><strong>{esc(p["title"])}</strong>'
                else:
                    body += '<div class="project-heading">'
                body += '<span class="project-status">' + esc(compact(fields.get('status'), 70)) + '</span>'
                if fields['needs_you']:
                    body += '<b class="needs-you">Needs you</b>'
                last = self.last_activity(p, data)
                if last:
                    body += '<small>' + relative_time(last, 'Activity ') + '</small>'
                body += '</div>' if selected else '</a>'
                if selected:
                    body += '<dl>'
                    for key in ('usable now', 'left', 'waiting on'):
                        value = fields.get(key)
                        if value:
                            body += f'<div><dt>{esc(key.capitalize())}</dt><dd>{esc(compact(value))}</dd></div>'
                if selected:
                    body += '</dl>'
                    body += relative_time(observed, 'Updated ')
                    if data.get('summary', {}).get('provenance', '').endswith(' · Source changed; owner must write short plain headers.'):
                        body += '<p role="status">Source changed. The owner must shorten its plain progress header.</p>'
                    body += '<details><summary>Project details</summary>'
                    body += '<p>' + esc(fields['_source']) + ' · outcome ' + esc(p['outcome_id']) + '</p>'
                    body += '<p>Lead runtime: ' + esc(self.runtime_state(data)) + '</p>'
                    if data.get('runtime',{}).get('provenance')=='docket.running_sample':
                        body += relative_time(data['runtime']['at'], 'Busy sample ') + '<p>docket.running_sample</p>'
                    for key in ('usable now', 'left', 'waiting on', 'handed to'):
                        if fields.get(key):
                            body += '<p><b>' + esc(key.capitalize()) + ':</b> ' + esc(fields[key]) + '</p>'
                    if fields.get('_header'):
                        body += '<h3>Local header</h3><pre>' + esc(json.dumps(fields['_header'], indent=2)) + '</pre>'
                    body += '</details>'
                body += '</section>'
                if unavailable and not selected:
                    body += '<small>' + esc(p['title']) + ': ' + esc(unavailable) + '</small>'
                if selected:
                    body += f'<p role="status">{esc(unavailable)}</p>' if unavailable else ''
                    body += self.files(p, data, request.query.get('file'))
                    body += self.acceptance(data)
            now = datetime.now(timezone.utc)
            recent = sorted((e for e in feed if now-timedelta(hours=6) <= stamp(e['at']) <= now), key=lambda e: stamp(e['at']), reverse=True)
            if not selected:
                if updated:
                    body += relative_time(min(updated, key=stamp), 'Oldest update ')
            if recent:
                body += '<h2>Recent activity · last 6 hours</h2>'
            seen = set()
            more_activity = False
            for e in recent[:50]:
                event_key = (e['project_id'], e['id'])
                if event_key in seen:
                    continue
                seen.add(event_key)
                if len(seen) == 4:
                    body += '<details><summary>More activity</summary>'
                    more_activity = True
                body += '<section class="activity"><b>' + esc(e['project_title']) + '</b> · ' + esc({'landing': 'Landed', 'acceptance': 'Acceptance', 'decision': 'Decision', 'handoff': 'Handoff', 'status': 'Status observed', 'file': 'File update'}[e['kind']]) + ' · ' + relative_time(e['at']) + '<p>' + esc(compact(e.get('label'))) + '</p><details><summary>Event details</summary><p>' + esc(e.get('label')) + '</p><small>' + esc(e['reference']) + ' · ' + esc(e['provenance']) + '</small></details></section>'
            if more_activity:
                body += '</details>'
            css = Path(__file__).with_name('viewer.html').read_text().split('<style>')[1].split('</style>')[0]
            css += 'a{color:var(--accent)} section{background:var(--card);padding:12px;border-radius:10px;margin:8px 0} dl{margin:0} dl>div{display:grid;grid-template-columns:9rem 1fr;gap:8px} dt{color:var(--muted)} dd{margin:0} p,dd,small{overflow-wrap:anywhere} img{max-width:100%;height:auto} select{max-width:100%;font:inherit} article{min-width:0} @media(max-width:420px){dl>div{grid-template-columns:6rem 1fr}}'
            css += '.project-row{display:flex;flex-wrap:wrap;gap:4px 12px;align-items:baseline}.project-name{font-weight:650}.summary-cell{font-size:12px;overflow-wrap:anywhere;min-width:0}.project-row dl,.project-row small{width:100%}.table-scroll{overflow-x:auto}table{border-collapse:collapse}td,th{padding:4px 8px;border:1px solid var(--line)}article p{margin:6px 0}'
            css += 'header,main,section,form,dl,dd{min-width:0} h1,a,label,article{overflow-wrap:anywhere} nav{display:flex;flex-wrap:wrap;gap:8px} nav a,.project-name{display:inline-flex;align-items:center;min-height:44px;min-width:44px;padding:8px;max-width:100%} a:focus-visible,select:focus-visible,button:focus-visible,summary:focus-visible{outline:3px solid var(--accent);outline-offset:2px} form{display:flex;flex-wrap:wrap;align-items:end;gap:8px;margin:12px 0} form label{display:grid;gap:4px;min-width:0;max-width:100%;flex:1 1 12rem} select,button{min-height:44px;min-width:44px} select{width:100%;background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:8px;padding:8px} details>summary{display:list-item;min-height:44px;padding:12px;overflow-wrap:anywhere} .project-row{align-items:center}'
            css += '.project-row{display:block;padding:0}.project-link{display:grid;grid-template-columns:1fr auto;gap:3px 8px;padding:12px;min-height:44px;text-decoration:none;box-sizing:border-box}.project-link strong{grid-column:1/-1;text-decoration:underline}.project-status{color:var(--fg);font-size:13px}.project-link small{width:auto;color:var(--muted);font-size:12px}.needs-you{font-size:12px;color:var(--accent)}.project-heading{display:flex;flex-wrap:wrap;gap:6px 12px}.project-row:has(dl){padding:12px}.project-row dl{margin:8px 0;font-size:13px}.project-row time{font-size:12px;color:var(--muted)}.activity{font-size:13px}.activity p{margin:4px 0}h2{font-size:18px}details{width:100%}'
            css += '.project-link{display:flex;flex-wrap:nowrap;align-items:center}.project-link strong{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.project-link .project-status{max-width:35%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.project-link small{white-space:nowrap}'
            document = '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(title) + '</title><script src="/tower/projects-select.js" defer></script><style>' + css + '</style><header><h1>' + esc(title) + '</h1><nav aria-label="Tower"><a href="/tower/">Events</a><a href="/tower/projects/">All projects</a></nav></header><main>' + body + '</main></html>'
            if len(document.encode()) > 2 * 1024 * 1024:
                raise ValueError('response too large')
            headers = dict(HEADERS)
            headers['Content-Security-Policy'] += "; script-src 'self'"
            return web.Response(text=document, content_type='text/html', headers=headers)
        except (KeyError, ValueError, OSError, TypeError, UnicodeError, RecursionError):
            return web.Response(text='Project information unavailable: configuration missing, unreadable or invalid. The project owner must refresh or configure its shared snapshot.', status=503, headers=HEADERS)

    def files(self, p, data, chosen):
        files = p.get('files', [])
        knowledge = data.get('knowledge', {})
        entries = knowledge.get('items', [])[:200] if knowledge.get('state') == 'available' else []
        options, chosen, label = self.file_selection(p, chosen)
        body = '<form method="get"><label>File <select name="file" data-file-selection><option value="">Choose a file</option>'
        for fid, option_label in options:
            body += f'<option value="{esc(fid)}"' + (' selected' if fid == chosen else '') + '>' + esc(option_label) + '</option>'
        body += '</select></label><noscript><button>Read file</button></noscript></form>'
        if not chosen:
            return body
        body += '<article><h2>' + esc(label) + '</h2>'
        if chosen == 'knowledge':
            body += '<h2>Knowledge</h2><p>' + esc(knowledge.get('state', 'unavailable')) + ' · ' + esc(knowledge.get('provenance')) + '</p>'
            body += relative_time(knowledge.get('observed_at'), 'Notes and todos from ')
            if knowledge.get('state') != 'available':
                body += '<p>Knowledge unavailable: no available knowledge snapshot was reported. The project owner must refresh its shared snapshot.</p>'
            for item in entries:
                body += '<h3>' + esc(item.get('title')) + '</h3><details><summary>Item details</summary><small>ID ' + esc(item.get('id')) + ' · ' + esc(item.get('kind')) + (' · completed: ' + esc(item['completed']) if 'completed' in item else '') + '</small></details>' + markdown(str(item.get('body', ''))[:50000], {})
        else:
            f = next((f for f in files if f['id'] == chosen), None)
            if not f:
                raise web.HTTPNotFound()
            try:
                root, images = f.get('root', p['root']), {}
                if root == p['root']:
                    for image in p.get('images', []):
                        if Path(image['path']).suffix.lower() not in ('.png', '.jpg', '.jpeg', '.gif', '.webp'):
                            continue
                        rel = os.path.relpath(image['path'], str(Path(f['path']).parent))
                        # Relative reference is only a lookup key, never passed to read_file.
                        images[rel] = f'/tower/projects/{p["id"]}/images/{image["id"]}'
                body += markdown(read_file(root, f['path'], {'.md'}).decode(), images)
            except (ValueError, OSError, UnicodeError):
                body += '<p>Selected file unavailable: missing, unreadable or not permitted. The project owner must restore or configure the shared document.</p>'
        return body + '</article>'
