#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib.util
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PATH_RE = re.compile(r'(/[A-Za-z0-9._@%+=:,~\-]+(?:/[A-Za-z0-9._@%+=:,~\-]+)*)')
FS_RE = re.compile(r'^vfs\.fs\.size\[(?P<mount>.*?),(?P<mode>.*?)\]$')
DEV_RE = re.compile(r'^vfs\.dev\.(?P<kind>read|write)\[(?P<device>.*?)\]$')
NET_RE = re.compile(r'^net\.if\.[^(\[]+\[(?P<iface>.*?)(?:,.*)?\]$')
CERT_RE = re.compile(r'^web\.certificate\.get\[(?P<target>.+?)\]$')
SYSTEMD_RE = re.compile(r'^systemd\.unit\.(?:get|info)\[(?P<unit>.*?)(?:,(?P<field>.*?))?\]$')
PROC_RE = re.compile(r'^proc\.num\[(?P<args>.*)\]$')

SERVICE_HINTS = (
    'zabbix', 'grafana', 'victoria', 'victoriametrics',
    'vminsert', 'vmselect', 'vmstorage', 'vmagent', 'vmalert', 'vmauth',
    'postgres', 'postgresql', 'pgsql'
)
IGNORE_PATH_PREFIXES = ('/proc/', '/sys/', '/dev/', '/run/', '/tmp/')


class ZabbixAPIError(RuntimeError):
    pass


def load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Cannot load module from {module_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def api_call(url: str, method: str, params: dict[str, Any] | list[Any], auth: str | None, timeout: int, verify_ssl: bool) -> Any:
    payload: dict[str, Any] = {'jsonrpc': '2.0', 'method': method, 'params': params, 'id': 1}
    if auth is not None:
        payload['auth'] = auth
    response = requests.post(
        url,
        json=payload,
        timeout=timeout,
        verify=verify_ssl,
        headers={'Content-Type': 'application/json-rpc'},
    )
    response.raise_for_status()
    data = response.json()
    if 'error' in data:
        raise ZabbixAPIError(f"{method} failed: {data['error'].get('message')} / {data['error'].get('data')}")
    return data['result']


def zbx_login(url: str, user: str, password: str, timeout: int, verify_ssl: bool) -> str:
    return api_call(url, 'user.login', {'username': user, 'password': password}, None, timeout, verify_ssl)


def zbx_logout(url: str, auth: str, timeout: int, verify_ssl: bool) -> None:
    try:
        api_call(url, 'user.logout', [], auth, timeout, verify_ssl)
    except Exception:
        pass


def load_host_tree(path: Path) -> dict[str, dict[str, list[str]]]:
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('Host tree JSON must be an object')
    return data


def flatten_tree(tree: dict[str, dict[str, list[str]]]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for system_name, roles in tree.items():
        if not isinstance(roles, dict):
            raise ValueError(f'System {system_name} must contain an object of roles')
        for role_name, hosts in roles.items():
            if not isinstance(hosts, list):
                raise ValueError(f'Role {system_name}/{role_name} must contain an array of hosts')
            for host in hosts:
                result.append({'system': system_name, 'role': role_name, 'host_ref': str(host)})
    return result


def find_host(url: str, auth: str, host_ref: str, timeout: int, verify_ssl: bool) -> dict[str, Any] | None:
    exact_host = api_call(
        url, 'host.get',
        {
            'output': ['hostid', 'host', 'name', 'status'],
            'filter': {'host': [host_ref]},
            'selectParentTemplates': ['templateid', 'name', 'host'],
            'selectTags': 'extend',
            'selectGroups': ['groupid', 'name'],
            'limit': 1,
        },
        auth, timeout, verify_ssl,
    )
    if exact_host:
        return exact_host[0]

    search_name = api_call(
        url, 'host.get',
        {
            'output': ['hostid', 'host', 'name', 'status'],
            'search': {'name': host_ref},
            'searchByAny': True,
            'selectParentTemplates': ['templateid', 'name', 'host'],
            'selectTags': 'extend',
            'selectGroups': ['groupid', 'name'],
        },
        auth, timeout, verify_ssl,
    )
    for candidate in search_name:
        if candidate.get('name') == host_ref:
            return candidate
    return None


def get_items(url: str, auth: str, hostid: str, timeout: int, verify_ssl: bool) -> list[dict[str, Any]]:
    return api_call(
        url, 'item.get',
        {
            'output': ['itemid', 'name', 'key_', 'lastvalue', 'lastclock', 'units', 'value_type', 'status', 'state'],
            'hostids': [hostid],
            'filter': {'status': 0},
            'webitems': True,
            'sortfield': 'name',
        },
        auth, timeout, verify_ssl,
    )


def safe_path(path: str) -> bool:
    if not path.startswith('/'):
        return False
    if path in {'/', '/var', '/etc', '/usr', '/opt'}:
        return False
    if any(path.startswith(prefix) for prefix in IGNORE_PATH_PREFIXES):
        return False
    return True


def extract_paths(*chunks: str) -> list[str]:
    found: set[str] = set()
    for chunk in chunks:
        if not chunk:
            continue
        for match in PATH_RE.finditer(chunk):
            path = match.group(1)
            if safe_path(path):
                found.add(path)
    return sorted(found)


def classify_path(path: str) -> str:
    low = path.lower()
    if low.startswith('/var/log/') or low.endswith(('.log', '.out', '.err')):
        return 'log_like'
    if low.startswith('/etc/') or low.endswith(('.conf', '.ini', '.yaml', '.yml', '.json', '.toml', '.cnf', '.service')):
        return 'config_like'
    if '/cert' in low or low.endswith(('.crt', '.pem', '.key', '.cer')) or low.startswith('/etc/ssl/') or low.startswith('/etc/pki/'):
        return 'certificate_like'
    if '/bin/' in low or '/sbin/' in low:
        return 'binary_like'
    if any(token in low for token in ['/var/lib/', 'data', 'pg_wal', 'tablespace', 'victoria-metrics-data']):
        return 'data_like'
    return 'other'


def add_unique(rows: list[dict[str, Any]], row: dict[str, Any], dedupe_key: tuple[Any, ...], seen: set[tuple[Any, ...]]) -> None:
    if dedupe_key not in seen:
        seen.add(dedupe_key)
        rows.append(row)


def collect_confirmed_data(items: list[dict[str, Any]]) -> dict[str, Any]:
    services, filesystems, block_devices, interfaces, ssl_targets, paths, raw_matches = ([] for _ in range(7))
    seen_services, seen_fs, seen_dev, seen_iface, seen_ssl, seen_path, seen_raw = (set() for _ in range(7))

    for item in items:
        name = str(item.get('name', ''))
        key_ = str(item.get('key_', ''))
        lastvalue = str(item.get('lastvalue', ''))
        lastclock = int(item.get('lastclock') or 0)
        ts = datetime.fromtimestamp(lastclock, tz=timezone.utc).isoformat() if lastclock else None

        m = SYSTEMD_RE.match(key_)
        if m:
            unit = m.group('unit').strip().strip('"').strip("'")
            field = (m.group('field') or '').strip().strip('"').strip("'")
            add_unique(services, {
                'unit': unit, 'field': field or None, 'item_name': name, 'item_key': key_,
                'lastvalue': lastvalue, 'lastclock': ts,
            }, (unit, field, lastvalue, key_), seen_services)

        if not m and PROC_RE.match(key_):
            lowered = f'{name} {key_} {lastvalue}'.lower()
            if any(hint in lowered for hint in SERVICE_HINTS):
                add_unique(services, {
                    'unit': None, 'field': 'process_count', 'item_name': name, 'item_key': key_,
                    'lastvalue': lastvalue, 'lastclock': ts,
                }, (name, key_, lastvalue), seen_services)

        m = FS_RE.match(key_)
        if m:
            mount = m.group('mount').strip().strip('"').strip("'")
            mode = m.group('mode').strip().strip('"').strip("'")
            add_unique(filesystems, {
                'mountpoint': mount, 'metric': mode, 'item_name': name, 'item_key': key_,
                'lastvalue': lastvalue, 'units': item.get('units') or None, 'lastclock': ts,
            }, (mount, mode, key_, lastvalue), seen_fs)

        m = DEV_RE.match(key_)
        if m:
            device = m.group('device').strip().strip('"').strip("'")
            kind = m.group('kind')
            add_unique(block_devices, {
                'device': device, 'metric': kind, 'item_name': name, 'item_key': key_,
                'lastvalue': lastvalue, 'units': item.get('units') or None, 'lastclock': ts,
            }, (device, kind, key_, lastvalue), seen_dev)

        m = NET_RE.match(key_)
        if m:
            iface = m.group('iface').strip().strip('"').strip("'")
            add_unique(interfaces, {
                'interface': iface, 'item_name': name, 'item_key': key_,
                'lastvalue': lastvalue, 'units': item.get('units') or None, 'lastclock': ts,
            }, (iface, key_, lastvalue), seen_iface)

        m = CERT_RE.match(key_)
        if m:
            target = m.group('target').strip().strip('"').strip("'")
            add_unique(ssl_targets, {
                'target': target, 'item_name': name, 'item_key': key_,
                'lastvalue': lastvalue, 'lastclock': ts,
            }, (target, key_, lastvalue), seen_ssl)

        for path in extract_paths(name, key_, lastvalue):
            add_unique(paths, {
                'path': path, 'kind': classify_path(path), 'item_name': name, 'item_key': key_,
                'lastvalue_excerpt': lastvalue[:300] if lastvalue else None, 'lastclock': ts,
            }, (path, key_), seen_path)

        combined = f'{name} {key_} {lastvalue}'.lower()
        if any(hint in combined for hint in SERVICE_HINTS):
            add_unique(raw_matches, {
                'item_name': name, 'item_key': key_, 'lastvalue': lastvalue, 'lastclock': ts,
            }, (name, key_, lastvalue), seen_raw)

    return {
        'services': sorted(services, key=lambda x: (str(x.get('unit')), str(x.get('item_key')))),
        'filesystems': sorted(filesystems, key=lambda x: (x['mountpoint'], x['metric'])),
        'block_devices': sorted(block_devices, key=lambda x: (x['device'], x['metric'])),
        'network_interfaces': sorted(interfaces, key=lambda x: (x['interface'], x['item_key'])),
        'ssl_targets': sorted(ssl_targets, key=lambda x: (x['target'], x['item_key'])),
        'paths_found_in_items': sorted(paths, key=lambda x: (x['path'], x['item_key'])),
        'raw_service_related_items': sorted(raw_matches, key=lambda x: (x['item_name'], x['item_key'])),
    }


def build_report(config_module, tree: dict[str, dict[str, list[str]]]) -> dict[str, Any]:
    auth = None
    try:
        auth = zbx_login(
            config_module.ZABBIX_URL,
            config_module.ZABBIX_USER,
            config_module.ZABBIX_PASSWORD,
            config_module.TIMEOUT,
            config_module.VERIFY_SSL,
        )

        inventory: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

        for node in flatten_tree(tree):
            system_name = node['system']
            role_name = node['role']
            host_ref = node['host_ref']

            inventory.setdefault(system_name, {})
            inventory[system_name].setdefault(role_name, {})

            host_obj = find_host(
                config_module.ZABBIX_URL, auth, host_ref,
                config_module.TIMEOUT, config_module.VERIFY_SSL,
            )

            if host_obj is None:
                inventory[system_name][role_name][host_ref] = {
                    'status': 'host_not_found_in_zabbix',
                    'zabbix_host': None,
                    'templates': [],
                    'groups': [],
                    'tags': [],
                    'confirmed': {
                        'services': [], 'filesystems': [], 'block_devices': [],
                        'network_interfaces': [], 'ssl_targets': [],
                        'paths_found_in_items': [], 'raw_service_related_items': []
                    },
                    'summary': {'item_count': 0, 'matched_sections': []}
                }
                continue

            items = get_items(
                config_module.ZABBIX_URL, auth, host_obj['hostid'],
                config_module.TIMEOUT, config_module.VERIFY_SSL,
            )
            confirmed = collect_confirmed_data(items)
            matched_sections = [k for k, v in confirmed.items() if v]

            inventory[system_name][role_name][host_ref] = {
                'status': 'ok',
                'zabbix_host': {
                    'hostid': host_obj.get('hostid'),
                    'host': host_obj.get('host'),
                    'name': host_obj.get('name'),
                },
                'templates': sorted([
                    t.get('name') or t.get('host')
                    for t in host_obj.get('parentTemplates', [])
                    if t.get('name') or t.get('host')
                ]),
                'groups': sorted([g.get('name') for g in host_obj.get('groups', []) if g.get('name')]),
                'tags': sorted([
                    {'tag': tag.get('tag'), 'value': tag.get('value')}
                    for tag in host_obj.get('tags', []) if tag.get('tag')
                ], key=lambda x: (x['tag'], x.get('value') or '')),
                'confirmed': confirmed,
                'summary': {
                    'item_count': len(items),
                    'matched_sections': matched_sections,
                    'service_count': len(confirmed['services']),
                    'filesystem_metric_count': len(confirmed['filesystems']),
                    'block_device_metric_count': len(confirmed['block_devices']),
                    'network_metric_count': len(confirmed['network_interfaces']),
                    'ssl_target_count': len(confirmed['ssl_targets']),
                    'path_count': len(confirmed['paths_found_in_items']),
                },
            }

        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'source': {
                'zabbix_url': config_module.ZABBIX_URL,
                'mode': 'confirmed_only_from_items',
                'host_tree_path': str(config_module.HOST_TREE_PATH),
            },
            'inventory': inventory,
        }
    finally:
        if auth:
            zbx_logout(config_module.ZABBIX_URL, auth, config_module.TIMEOUT, config_module.VERIFY_SSL)


def main() -> int:
    base_dir = Path.cwd()
    config_path = base_dir / 'monitoring_config.py'
    if not config_path.exists():
        print('[ERROR] monitoring_config.py not found near the script.', file=sys.stderr)
        print('Copy monitoring_config.example.py to monitoring_config.py and adjust credentials.', file=sys.stderr)
        return 2

    config_module = load_module(config_path, 'monitoring_config')
    tree_path = base_dir / str(config_module.HOST_TREE_PATH)
    if not tree_path.exists():
        print(f'[ERROR] Host tree JSON not found: {tree_path}', file=sys.stderr)
        return 2

    tree = load_host_tree(tree_path)
    report = build_report(config_module, tree)

    output_path = base_dir / str(config_module.OUTPUT_JSON_PATH)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f'[OK] Report written to: {output_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
