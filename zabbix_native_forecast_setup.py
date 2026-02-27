#!/usr/bin/env python3
"""Provision native Zabbix forecast items and risk triggers for selected hosts."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import config as cfg
from processing import (
    get_hosts_by_as,
    get_items_for_hosts,
    index_items_by_host,
    parse_csv_values,
    pick_as_value,
    select_items,
)
from zabbix_client import ZabbixAPI, ZabbixAPIError


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def parse_int_values(raw: object) -> List[int]:
    if isinstance(raw, str):
        values = parse_csv_values(raw)
    elif isinstance(raw, Sequence):
        values = [str(value).strip() for value in raw if str(value).strip()]
    else:
        values = [str(raw).strip()]
    return [int(value) for value in values]


def extract_tag_value(tags: Sequence[Dict], tag_key: str) -> str:
    for tag in tags:
        if str(tag.get("tag", "")) == tag_key:
            value = str(tag.get("value", "")).strip()
            if value:
                return value
    return ""


def env_group_from_value(env_value: str) -> str:
    return "prod" if str(env_value).strip().lower() == "prod" else "non-prod"


@dataclass(frozen=True)
class ForecastItemSpec:
    hostid: str
    host: str
    metric: str
    horizon_days: int
    key_: str
    name: str
    formula: str
    delay: str
    units: str = "%"
    value_type: int = 0
    type: int = 15
    status: int = 0


@dataclass(frozen=True)
class TriggerSpec:
    hostid: str
    host: str
    metric: str
    description: str
    expression: str
    priority: int
    tags: Tuple[Tuple[str, str], ...]
    status: int = 0


def iter_chunks(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(values), size):
        yield list(values[idx : idx + size])


def get_items_extended(api: ZabbixAPI, hostids: Sequence[str], chunk_size: int) -> List[Dict]:
    fields = ["itemid", "hostid", "name", "key_", "type", "value_type", "delay", "params", "units", "status"]
    all_items: List[Dict] = []
    for host_chunk in iter_chunks(list(hostids), max(1, int(chunk_size))):
        params = {
            "output": fields,
            "hostids": host_chunk,
            "sortfield": "key_",
            "sortorder": "ASC",
        }
        all_items.extend(api.call("item.get", params))
    return all_items


def get_triggers(api: ZabbixAPI, hostids: Sequence[str], chunk_size: int) -> List[Dict]:
    fields = ["triggerid", "description", "expression", "priority", "status"]
    all_triggers: List[Dict] = []
    for host_chunk in iter_chunks(list(hostids), max(1, int(chunk_size))):
        params = {
            "output": fields,
            "hostids": host_chunk,
            "search": {"description": "[NativeForecast]"},
            "searchWildcardsEnabled": True,
            "selectHosts": ["hostid", "host"],
            "selectTags": "extend",
            "sortfield": "description",
            "sortorder": "ASC",
        }
        all_triggers.extend(api.call("trigger.get", params))
    return all_triggers


def build_forecast_formula(item_key: str, transform: str, history_window: str, horizon_days: int, fit: str) -> str:
    base = f'forecast(//{item_key},{history_window},{int(horizon_days)}d,"{fit}","value")'
    if transform == "invert_100":
        return f"100-{base}"
    return base


def build_forecast_item_specs(
    hostid: str,
    host: str,
    metric: str,
    item_key: str,
    transform: str,
    horizons: Sequence[int],
    history_window: str,
    fit: str,
    delay: str,
) -> List[ForecastItemSpec]:
    specs: List[ForecastItemSpec] = []
    for horizon in sorted(set(int(value) for value in horizons if int(value) > 0)):
        key_ = f"native.forecast.util[{metric},{horizon}d]"
        name = f"[NativeForecast] {metric.upper()} utilization +{horizon}d"
        formula = build_forecast_formula(
            item_key=item_key,
            transform=transform,
            history_window=history_window,
            horizon_days=horizon,
            fit=fit,
        )
        specs.append(
            ForecastItemSpec(
                hostid=hostid,
                host=host,
                metric=metric,
                horizon_days=horizon,
                key_=key_,
                name=name,
                formula=formula,
                delay=delay,
            )
        )
    return specs


def build_trigger_specs(
    hostid: str,
    host: str,
    metric: str,
    horizon_to_key: Dict[int, str],
    threshold: float,
    as_value: str,
    env_value: str,
    env_group: str,
) -> List[TriggerSpec]:
    required = (30, 90, 180, 365)
    if any(horizon not in horizon_to_key for horizon in required):
        return []

    expr_30 = f"last(/{host}/{horizon_to_key[30]})>={threshold}"
    expr_90 = f"last(/{host}/{horizon_to_key[90]})>={threshold}"
    expr_180 = f"last(/{host}/{horizon_to_key[180]})>={threshold}"
    expr_365 = f"last(/{host}/{horizon_to_key[365]})>={threshold}"

    common_tags = (
        ("SOURCE", "native_forecast"),
        ("AS", as_value),
        ("ENV", env_value),
        ("ENV_GROUP", env_group),
        ("METRIC", metric),
        ("THRESHOLD", str(threshold)),
    )

    return [
        TriggerSpec(
            hostid=hostid,
            host=host,
            metric=metric,
            description=f"[NativeForecast][{metric.upper()}] критично сейчас (<=30д)",
            expression=expr_30,
            priority=4,
            tags=(*common_tags, ("RISK_WINDOW", "30d")),
        ),
        TriggerSpec(
            hostid=hostid,
            host=host,
            metric=metric,
            description=f"[NativeForecast][{metric.upper()}] критично скоро (31-90д)",
            expression=f"({expr_90}) and not ({expr_30})",
            priority=3,
            tags=(*common_tags, ("RISK_WINDOW", "90d")),
        ),
        TriggerSpec(
            hostid=hostid,
            host=host,
            metric=metric,
            description=f"[NativeForecast][{metric.upper()}] риск 6м (91-180д)",
            expression=f"({expr_180}) and not ({expr_90})",
            priority=2,
            tags=(*common_tags, ("RISK_WINDOW", "180d")),
        ),
        TriggerSpec(
            hostid=hostid,
            host=host,
            metric=metric,
            description=f"[NativeForecast][{metric.upper()}] риск 12м (181-365д)",
            expression=f"({expr_365}) and not ({expr_180})",
            priority=1,
            tags=(*common_tags, ("RISK_WINDOW", "365d")),
        ),
    ]


def tags_as_tuple(tags: Sequence[Dict]) -> Tuple[Tuple[str, str], ...]:
    normalized = [(str(tag.get("tag", "")), str(tag.get("value", ""))) for tag in tags]
    normalized = [entry for entry in normalized if entry[0]]
    normalized.sort(key=lambda entry: (entry[0], entry[1]))
    return tuple(normalized)


def upsert_forecast_item(
    api: ZabbixAPI,
    existing_map: Dict[Tuple[str, str], Dict],
    spec: ForecastItemSpec,
    dry_run: bool,
) -> Tuple[str, str]:
    key = (spec.hostid, spec.key_)
    existing = existing_map.get(key)
    payload = {
        "name": spec.name,
        "key_": spec.key_,
        "type": spec.type,
        "value_type": spec.value_type,
        "delay": spec.delay,
        "params": spec.formula,
        "units": spec.units,
        "status": spec.status,
    }

    if existing is None:
        if dry_run:
            return "create", "dry-run"
        create_payload = {"hostid": spec.hostid, **payload}
        result = api.call("item.create", create_payload)
        created_id = str(result["itemids"][0])
        existing_map[key] = {"itemid": created_id, **create_payload}
        return "create", created_id

    if str(existing.get("type")) != str(spec.type):
        return "skip_conflict", str(existing.get("itemid", ""))

    changed = (
        str(existing.get("name", "")) != spec.name
        or str(existing.get("delay", "")) != spec.delay
        or str(existing.get("params", "")) != spec.formula
        or str(existing.get("units", "")) != spec.units
        or str(existing.get("status", "")) != str(spec.status)
        or str(existing.get("value_type", "")) != str(spec.value_type)
    )
    if not changed:
        return "noop", str(existing.get("itemid", ""))

    if dry_run:
        return "update", "dry-run"

    update_payload = {"itemid": existing["itemid"], **payload}
    api.call("item.update", update_payload)
    existing_map[key].update(update_payload)
    return "update", str(existing.get("itemid", ""))


def upsert_trigger(
    api: ZabbixAPI,
    existing_map: Dict[Tuple[str, str], Dict],
    spec: TriggerSpec,
    dry_run: bool,
) -> Tuple[str, str]:
    key = (spec.hostid, spec.description)
    existing = existing_map.get(key)
    tags_payload = [{"tag": tag, "value": value} for tag, value in spec.tags]
    payload = {
        "description": spec.description,
        "expression": spec.expression,
        "priority": spec.priority,
        "status": spec.status,
        "tags": tags_payload,
    }

    if existing is None:
        if dry_run:
            return "create", "dry-run"
        result = api.call("trigger.create", payload)
        triggerid = str(result["triggerids"][0])
        existing_map[key] = {"triggerid": triggerid, **payload}
        return "create", triggerid

    changed = (
        str(existing.get("expression", "")) != spec.expression
        or str(existing.get("priority", "")) != str(spec.priority)
        or str(existing.get("status", "")) != str(spec.status)
        or tags_as_tuple(existing.get("tags", [])) != tags_as_tuple(tags_payload)
    )
    if not changed:
        return "noop", str(existing.get("triggerid", ""))

    if dry_run:
        return "update", "dry-run"

    update_payload = {"triggerid": existing["triggerid"], **payload}
    api.call("trigger.update", update_payload)
    existing_map[key].update(update_payload)
    return "update", str(existing.get("triggerid", ""))


def save_summary(rows: Sequence[Dict], path: Path) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create/update native Zabbix forecast calculated items and risk triggers."
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to Zabbix.")
    parser.add_argument("--without-triggers", action="store_true", help="Create forecast items only.")
    args = parser.parse_args(argv)

    if not cfg.ZABBIX_URL or not cfg.ZABBIX_USERNAME or not cfg.ZABBIX_PASSWORD:
        raise SystemExit("Set ZABBIX_URL, ZABBIX_USERNAME and ZABBIX_PASSWORD in config.py.")
    if cfg.TAG_OPERATOR not in ("equals", "contains"):
        raise SystemExit("TAG_OPERATOR in config.py must be 'equals' or 'contains'.")

    as_values = parse_csv_values(cfg.AS_TAG_VALUES)
    disk_fs_preferences = parse_csv_values(cfg.DISK_FS) or ["/"]
    horizons = sorted(set(parse_int_values(getattr(cfg, "FORECAST_HORIZONS", "30,90,180,365"))))
    if any(value <= 0 for value in horizons):
        raise SystemExit("FORECAST_HORIZONS must contain positive integers.")

    history_window = str(getattr(cfg, "NATIVE_FORECAST_HISTORY_WINDOW", "365d")).strip() or "365d"
    fit = str(getattr(cfg, "NATIVE_FORECAST_FIT", "linear")).strip() or "linear"
    delay = str(getattr(cfg, "NATIVE_FORECAST_DELAY", "1h")).strip() or "1h"
    threshold = float(getattr(cfg, "NATIVE_FORECAST_THRESHOLD", 90.0))
    output_dir = Path(cfg.OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "csv" / "native_forecast_provision_summary.csv"

    dry_run = bool(args.dry_run)
    create_triggers = not bool(args.without_triggers)

    log("Connecting to Zabbix API...")
    api = ZabbixAPI(
        url=cfg.ZABBIX_URL,
        username=cfg.ZABBIX_USERNAME,
        password=cfg.ZABBIX_PASSWORD,
        timeout=cfg.REQUEST_TIMEOUT,
        verify_ssl=cfg.VERIFY_SSL,
    )

    try:
        log("Loading hosts by AS tags...")
        hosts = get_hosts_by_as(api, cfg.AS_TAG_KEY, as_values, cfg.TAG_OPERATOR)
        if not hosts:
            raise SystemExit("No hosts matched AS filter.")
        log(f"Matched hosts: {len(hosts)}")

        host_meta: Dict[str, Dict[str, str]] = {}
        for host in hosts:
            hostid = str(host.get("hostid"))
            as_value = pick_as_value(host.get("tags", []), cfg.AS_TAG_KEY, as_values)
            env_value = extract_tag_value(host.get("tags", []), cfg.ENV_TAG_KEY)
            host_meta[hostid] = {
                "host": str(host.get("host", "")),
                "as_value": as_value,
                "env_value": env_value,
                "env_group": env_group_from_value(env_value),
            }

        hostids = list(host_meta.keys())
        log("Loading host items and selecting base utilization items...")
        items = get_items_for_hosts(api, hostids, chunk_size=max(1, int(getattr(cfg, "ITEM_CHUNK_SIZE", cfg.CHUNK_SIZE))))
        items_by_host = index_items_by_host(items)
        direct_items, _ = select_items(items_by_host, host_meta, disk_fs_preferences)
        if not direct_items:
            raise SystemExit("No CPU/RAM/Disk base items selected.")

        direct_by_host_metric: Dict[Tuple[str, str], object] = {}
        for item in direct_items:
            direct_by_host_metric[(item.hostid, item.metric)] = item

        all_item_specs: List[ForecastItemSpec] = []
        trigger_specs: List[TriggerSpec] = []
        for hostid, meta in host_meta.items():
            host = meta["host"]
            for metric in ("cpu", "ram", "disk"):
                base = direct_by_host_metric.get((hostid, metric))
                if base is None:
                    continue
                specs = build_forecast_item_specs(
                    hostid=hostid,
                    host=host,
                    metric=metric,
                    item_key=base.key_,
                    transform=base.transform,
                    horizons=horizons,
                    history_window=history_window,
                    fit=fit,
                    delay=delay,
                )
                all_item_specs.extend(specs)
                if create_triggers:
                    horizon_to_key = {spec.horizon_days: spec.key_ for spec in specs}
                    trigger_specs.extend(
                        build_trigger_specs(
                            hostid=hostid,
                            host=host,
                            metric=metric,
                            horizon_to_key=horizon_to_key,
                            threshold=threshold,
                            as_value=meta["as_value"],
                            env_value=meta["env_value"],
                            env_group=meta["env_group"],
                        )
                    )

        log(f"Desired calculated forecast items: {len(all_item_specs)}")
        extended_items = get_items_extended(api, hostids, chunk_size=max(1, int(getattr(cfg, "ITEM_CHUNK_SIZE", cfg.CHUNK_SIZE))))
        item_map = {(str(item.get("hostid")), str(item.get("key_"))): item for item in extended_items}

        summary_rows: List[Dict] = []
        stats = {"item_create": 0, "item_update": 0, "item_noop": 0, "item_skip_conflict": 0}
        for spec in all_item_specs:
            action, object_id = upsert_forecast_item(api, item_map, spec, dry_run=dry_run)
            if action == "create":
                stats["item_create"] += 1
            elif action == "update":
                stats["item_update"] += 1
            elif action == "noop":
                stats["item_noop"] += 1
            elif action == "skip_conflict":
                stats["item_skip_conflict"] += 1
            summary_rows.append(
                {
                    "object_type": "item",
                    "action": action,
                    "hostid": spec.hostid,
                    "host": spec.host,
                    "metric": spec.metric,
                    "horizon_days": spec.horizon_days,
                    "key_": spec.key_,
                    "object_id": object_id,
                    "dry_run": int(dry_run),
                }
            )

        trigger_stats = {"trigger_create": 0, "trigger_update": 0, "trigger_noop": 0}
        if create_triggers and trigger_specs:
            log(f"Desired risk triggers: {len(trigger_specs)}")
            existing_triggers = get_triggers(
                api,
                hostids,
                chunk_size=max(1, int(getattr(cfg, "ITEM_CHUNK_SIZE", cfg.CHUNK_SIZE))),
            )
            trigger_map: Dict[Tuple[str, str], Dict] = {}
            for trigger in existing_triggers:
                hosts_ref = trigger.get("hosts", [])
                for host_ref in hosts_ref:
                    hostid = str(host_ref.get("hostid", ""))
                    if not hostid:
                        continue
                    trigger_map[(hostid, str(trigger.get("description", "")))] = trigger

            for spec in trigger_specs:
                action, object_id = upsert_trigger(api, trigger_map, spec, dry_run=dry_run)
                if action == "create":
                    trigger_stats["trigger_create"] += 1
                elif action == "update":
                    trigger_stats["trigger_update"] += 1
                elif action == "noop":
                    trigger_stats["trigger_noop"] += 1
                summary_rows.append(
                    {
                        "object_type": "trigger",
                        "action": action,
                        "hostid": spec.hostid,
                        "host": spec.host,
                        "metric": spec.metric,
                        "horizon_days": "",
                        "key_": "",
                        "description": spec.description,
                        "object_id": object_id,
                        "dry_run": int(dry_run),
                    }
                )

        save_summary(summary_rows, summary_path)
        log(f"Saved summary: {summary_path}")
        log(
            "Items: "
            f"created={stats['item_create']} updated={stats['item_update']} "
            f"noop={stats['item_noop']} conflict={stats['item_skip_conflict']}"
        )
        if create_triggers:
            log(
                "Triggers: "
                f"created={trigger_stats['trigger_create']} updated={trigger_stats['trigger_update']} "
                f"noop={trigger_stats['trigger_noop']}"
            )
        if dry_run:
            log("Dry-run mode: no changes were written to Zabbix.")
        return 0
    finally:
        try:
            api.logout()
        except Exception as exc:
            log(f"Warning: failed to logout from Zabbix API: {exc}")
        api.session.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ZabbixAPIError as exc:
        raise SystemExit(str(exc))
