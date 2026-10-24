"""Fixed local refresh subprocess. Bus data can invalidate, never select an effect."""
import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from . import kit, project_schema as schema


def marker(data):
    schema.keys(data, 'version lead_session directory milestone_at milestone_ref observed_at roster_state provenance')
    if type(data.get('version')) is not int or data['version'] != 1:
        raise ValueError('marker version')
    if not schema.ident(data.get('lead_session')).startswith('ses_'):
        raise ValueError('marker session')
    schema.root(data.get('directory'))
    if schema.time(data.get('milestone_at')) > schema.time(data.get('observed_at')):
        raise ValueError('marker time order')
    schema.text(data.get('milestone_ref'),1000)
    if data.get('roster_state') not in ('active','hold','done'): raise ValueError('marker roster state')
    if data.get('provenance') != 'recorded_marker_observation':
        raise ValueError('marker provenance')
    return data


def notice(event, item_id):
    if event.get('source') != 'docket': raise ValueError('not Docket')
    data = event.get('data')
    schema.ident(event.get('id')); schema.time(event.get('time'))
    if event.get('type') == 'lead.milestone.recorded':
        if not re.fullmatch(r'mil_[0-9a-f]{64}', event['id']): raise ValueError('marker id')
        if not isinstance(data,dict) or event['time'] != data.get('observed_at'): raise ValueError('marker observation time')
        return {'version':1,'kind':'milestone','marker':dict(marker(data),id=event['id'])}
    if event.get('type') != 'docket.snapshot.refresh_due': raise ValueError('not refresh event')
    schema.keys(data,'version purpose item_id occurrence_id scheduled_at fired_at coalesced schedule_until provenance')
    if type(data.get('version')) is not int or data['version'] != 1 or data.get('purpose') != 'project_pages':
        raise ValueError('refresh version/purpose')
    if data.get('item_id') != item_id or data.get('occurrence_id') != event['id']:
        raise ValueError('refresh binding')
    if data.get('provenance') != 'docket_recurring_clock' or type(data.get('coalesced')) is not int or not 0 <= data['coalesced'] <= 10000000:
        raise ValueError('refresh provenance')
    scheduled, fired = schema.time(data.get('scheduled_at')), schema.time(data.get('fired_at'))
    if event['time'] != data.get('fired_at'): raise ValueError('refresh fired time')
    until = schema.stamp(data.get('schedule_until'))
    if until is None or not scheduled <= fired or scheduled > until:
        raise ValueError('refresh times')
    return {'version':1,'kind':'periodic'}


def configuration(value):
    schema.keys(value,'python script config item_id')
    schema.ident(value.get('item_id'))
    if value['item_id'] != 'dkt_tower_refresh': raise ValueError('fixed schedule required')
    for name in ('python','script','config'):
        path = Path(schema.text(value.get(name),4096))
        if not path.is_absolute() or path.resolve() != path:
            raise ValueError('fixed canonical path required')
    return value


async def invoke(config, payload):
    config = configuration(config)
    raw = json.dumps(payload,separators=(',',':')).encode()
    if len(raw) > 16384: raise kit.Fail('refresh invalidation too large')
    # No inherited agent transport, native identity, Python injection or API credentials.
    env = {key:os.environ[key] for key in ('HOME','PATH','LANG','LC_ALL') if key in os.environ}
    process = await asyncio.create_subprocess_exec(config['python'],'-I','-B',config['script'],config['config'],
        stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL,env=env)
    async def bounded():
        process.stdin.write(raw); await process.stdin.drain(); process.stdin.close()
        try: output = await process.stdout.readexactly(32769)
        except asyncio.IncompleteReadError as exc: output = exc.partial
        if len(output) > 32768: raise ValueError('refresh output bound')
        code = await process.wait()
        if code:
            try: error = json.loads(output).get('error')
            except (ValueError,AttributeError): error = None
            if error == 'lease_expired': raise kit.Fail('Snapshot lease expired; owner must renew through normal Docket client.')
            raise ValueError('refresh refused; owner must inspect source/config')
        result = json.loads(output)
        schema.keys(result,'version refreshed stale_sources redactions')
        if 'redactions' in result and (type(result['redactions']) is not int or result['redactions'] < 0):
            raise ValueError('refresh redaction count invalid')
        if type(result.get('version')) is not int or result['version'] != 1 or type(result.get('refreshed')) is not int or not 0 <= result['refreshed'] <= 100:
            raise ValueError('refresh result invalid')
        for pid in schema.seq(result.get('stale_sources'),100): schema.ident(pid)
        return result
    try:
        return await asyncio.wait_for(bounded(),timeout=60)
    finally:
        if process.returncode is None:
            process.kill(); await process.wait()


async def handle(event, ctx):
    if not ctx.config:
        raise kit.Drop('snapshot refresh not configured')
    try:
        config = configuration(ctx.config)
        payload = notice(event,config['item_id'])
    except (ValueError,TypeError,KeyError):
        raise kit.Drop('unapproved snapshot invalidation') from None
    result = await invoke(config,payload)
    ctx.log.info('snapshots refreshed',refreshed=result['refreshed'],stale_sources=len(result['stale_sources']),redactions=result.get('redactions',0))
