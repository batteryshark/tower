"""Strict v1 operator manifest and sanitized export validation (no I/O)."""
import re
from datetime import datetime, timezone
from pathlib import Path

ID = re.compile(r'^[A-Za-z0-9_-]{1,80}$')


def obj(value):
    if type(value) is not dict:
        raise ValueError('expected object')
    return value


def keys(value, allowed):
    obj(value)
    if set(value) - set(allowed.split()):
        raise ValueError('unknown schema fields')


def seq(value, maximum):
    if type(value) is not list or len(value) > maximum:
        raise ValueError('expected bounded array')
    return value


def text(value, maximum=2000, empty=False):
    if type(value) is not str or len(value) > maximum or (not empty and not value.strip()) or any(ord(c) < 32 and c not in '\n\r\t' for c in value):
        raise ValueError('expected bounded text')
    return value


def ident(value):
    if not ID.fullmatch(text(value, 80)):
        raise ValueError('invalid ID')
    return value


def stamp(value):
    try:
        text(value, 40)
        if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)', value):
            return None
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed if 2000 <= parsed.year <= 2100 else None
    except (ValueError, TypeError):
        return None


def time(value):
    parsed = stamp(value)
    if parsed is None or parsed > datetime.now(timezone.utc):
        raise ValueError('invalid or future time')
    return parsed


def version(data):
    obj(data)
    if type(data.get('version')) is not int or data['version'] != 1:
        raise ValueError('invalid version')


def json_depth(data, depth=0):
    if depth > 12:
        raise ValueError('JSON nesting limit')
    if isinstance(data, dict):
        for value in data.values():
            json_depth(value, depth + 1)
    elif isinstance(data, list):
        for value in data:
            json_depth(value, depth + 1)


def root(value):
    if not Path(text(value, 4096)).is_absolute():
        raise ValueError('root must be absolute')


def evidence(data):
    obj(data)
    for key in ('reference', 'provenance'):
        text(data.get(key), 500)


def project(p):
    json_depth(p)
    obj(p)
    keys(p, 'id outcome_id title status root header_file files images export')
    for key in ('id', 'outcome_id'):
        ident(p.get(key))
    text(p.get('title'), 200)
    text(p.get('status'), 80)
    root(p.get('root'))
    if 'header_file' in p:
        ident(p['header_file'])
    for group in ('files', 'images'):
        ids = set()
        for f in seq(p.get(group, []), 100):
            obj(f)
            keys(f, 'id label path root' if group == 'files' else 'id path')
            fid = ident(f.get('id'))
            if fid in ids or fid == 'knowledge':
                raise ValueError('duplicate/reserved ID')
            ids.add(fid)
            text(f.get('path'), 4096)
            if group == 'files':
                text(f.get('label'), 200)
                if 'root' in f:
                    root(f['root'])
    if 'export' in p:
        obj(p['export'])
        keys(p['export'], 'root path')
        root(p['export'].get('root'))
        text(p['export'].get('path'), 4096)


def bound(data, p):
    for key in ('project_id', 'outcome_id'):
        if ident(data.get(key)) != p['id' if key == 'project_id' else key]:
            raise ValueError('identity mismatch')


def projection(data, p):
    json_depth(data)
    version(data)
    keys(data, 'version project_id outcome_id generated_at summary knowledge acceptance activity status_observation runtime file_update file_updates')
    bound(data, p)
    generated = time(data.get('generated_at'))
    for name in ('status_observation', 'runtime', 'file_update'):
        if name not in data:
            continue
        s = data[name]
        keys(s, 'at reference provenance' + (' text id' if name == 'status_observation' else ' state' if name == 'runtime' else ''))
        evidence(s)
        if time(s.get('at')) > generated:
            raise ValueError('observation after generation')
        if name == 'status_observation':
            ident(s.get('id')); text(s.get('text'), 80)
            if s['text'] != s['text'].strip() or any(ord(c) < 32 for c in s['text']):
                raise ValueError('invalid status')
        if name == 'runtime':
            if s.get('state') not in ('running', 'unknown'):
                raise ValueError('invalid runtime state')
            if s['state']=='running' and (s['provenance']!='docket.running_sample' or not ident(s['reference']).startswith('ses_')):
                raise ValueError('unverified busy sample')
    files=set()
    for f in seq(data.get('file_updates', []), 100):
        keys(f,'id at reference provenance'); fid=ident(f.get('id')); evidence(f)
        if fid in files or time(f.get('at'))>generated:
            raise ValueError('invalid file update')
        files.add(fid)
    if 'summary' in data:
        s = obj(data['summary'])
        if set(s) - {'observed_at', 'provenance', 'reference', 'usable now', 'left', 'waiting on', 'handed to', 'needs_you'}:
            raise ValueError('unknown summary fields')
        if 'needs_you' in s and type(s['needs_you']) is not bool:
            raise ValueError('needs_you must be boolean')
        evidence(s)
        if time(s.get('observed_at')) > generated:
            raise ValueError('summary after export')
        for key in ('usable now', 'left', 'waiting on', 'handed to'):
            if key in s:
                text(s[key])
    if 'knowledge' in data:
        k = obj(data['knowledge'])
        keys(k, 'state provenance items observed_at')
        if 'observed_at' in k and time(k['observed_at']) > generated:
            raise ValueError('knowledge after export')
        text(k.get('provenance'), 500)
        if k.get('state') not in ('available', 'unavailable'):
            raise ValueError('invalid knowledge state')
        ids = set()
        for item in seq(k.get('items', []), 200):
            obj(item)
            keys(item, 'id kind title body completed')
            iid = ident(item.get('id'))
            if iid in ids:
                raise ValueError('duplicate knowledge ID')
            ids.add(iid)
            if item.get('kind') not in ('note', 'todo', 'plan'):
                raise ValueError('invalid knowledge kind')
            text(item.get('title'), 200)
            text(item.get('body'), 50000, empty=True)
            if item['kind'] == 'todo' and type(item.get('completed')) is not bool:
                raise ValueError('todo completion must be boolean')
            if 'completed' in item and type(item['completed']) is not bool:
                raise ValueError('invalid completion')
        if k['state'] == 'unavailable' and k.get('items'):
            raise ValueError('unavailable knowledge with items')
    if 'acceptance' in data:
        a = obj(data['acceptance'])
        keys(a, 'project_id outcome_id state request_state request_id requested_at provenance reference answer')
        bound(a, p)
        evidence(a)
        state = a.get('state')
        if state not in ('unfinished', 'finished-awaiting-acceptance', 'accepted', 'not-yet') or a.get('request_state') not in ('none', 'pending', 'expired', 'cancelled', 'answered'):
            raise ValueError('invalid acceptance state')
        requested = None
        if a['request_state'] != 'none':
            ident(a.get('request_id'))
            requested = time(a.get('requested_at'))
            if requested > generated:
                raise ValueError('request after export')
        answer = a.get('answer')
        if a['request_state'] == 'answered':
            obj(answer)
            keys(answer, 'type id request_id project_id outcome_id value answered_at provenance reference')
            bound(answer, p)
            evidence(answer)
            ident(answer.get('id'))
            if answer.get('type') != 'nod.answer' or ident(answer.get('request_id')) != a['request_id']:
                raise ValueError('answer not bound to request')
            if not requested <= time(answer.get('answered_at')) <= generated:
                raise ValueError('answer time order')
            value = answer.get('value')
            if (state not in ('accepted', 'not-yet') or type(value) is not str
                    or value not in ('Accept', 'Not yet')
                    or {'accepted': 'Accept', 'not-yet': 'Not yet'}[state] != value):
                raise ValueError('answer value/state mismatch')
        elif answer is not None or state in ('accepted', 'not-yet'):
            raise ValueError('no actual answer')
        if a['request_state'] == 'none' and ('request_id' in a or 'requested_at' in a):
            raise ValueError('request fields without request')
    ids = set()
    for e in seq(data.get('activity', []), 500):
        obj(e)
        keys(e, 'id project_id outcome_id kind at label reference provenance')
        bound(e, p)
        evidence(e)
        eid = ident(e.get('id'))
        if eid in ids or e.get('kind') not in ('landing', 'acceptance', 'decision', 'handoff', 'status', 'file'):
            raise ValueError('invalid/duplicate activity')
        ids.add(eid)
        text(e.get('label'), 500)
        if e['kind'] == 'status':
            text(e['label'], 80)
            if e['label'] != e['label'].strip() or any(ord(c) < 32 for c in e['label']):
                raise ValueError('invalid status observation')
        if time(e.get('at')) > generated:
            raise ValueError('activity after export')
    return data
