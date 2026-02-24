#!/usr/bin/env python3
"""Collect CPU/RAM/Disk utilization from Zabbix API, summarize, forecast and plot."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import urllib3
from urllib3.exceptions import InsecureRequestWarning

import config as cfg
from plotting import plot_as_breakdown, plot_metric_dashboard
from processing import (
    build_direct_history,
    build_direct_trend,
    build_forecast,
    build_native_forecast,
    build_ram_pair_history,
    build_ram_pair_trend,
    fetch_history_points,
    fetch_trend_points,
    get_hosts_by_as,
    get_items_for_hosts,
    index_items_by_host,
    parse_csv_values,
    pick_as_value,
    select_native_forecast_items,
    select_items,
    summarize_history,
    summarize_trend,
)
from zabbix_client import ZabbixAPI, ZabbixAPIError


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def save_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False)


def main() -> int:
    if cfg.TAG_OPERATOR not in ("equals", "contains"):
        raise SystemExit("TAG_OPERATOR in config.py must be 'equals' or 'contains'.")
    if cfg.HISTORY_DAYS <= 0 or cfg.TREND_DAYS <= 0 or cfg.FORECAST_DAYS < 0:
        raise SystemExit("HISTORY_DAYS/TREND_DAYS must be > 0 and FORECAST_DAYS must be >= 0.")
    if cfg.CHUNK_SIZE <= 0 or cfg.REQUEST_TIMEOUT <= 0:
        raise SystemExit("CHUNK_SIZE and REQUEST_TIMEOUT in config.py must be > 0.")
    if not isinstance(cfg.VERIFY_SSL, bool):
        raise SystemExit("VERIFY_SSL in config.py must be boolean.")

    forecast_source = str(getattr(cfg, "FORECAST_SOURCE", "python")).strip().lower()
    if forecast_source not in ("python", "zabbix"):
        raise SystemExit("FORECAST_SOURCE in config.py must be 'python' or 'zabbix'.")
    forecast_lookback_days = int(getattr(cfg, "FORECAST_LOOKBACK_DAYS", cfg.HISTORY_DAYS))
    if forecast_lookback_days <= 0:
        raise SystemExit("FORECAST_LOOKBACK_DAYS in config.py must be > 0.")

    if cfg.VERIFY_SSL is False:
        log("Warning: TLS certificate verification is disabled (VERIFY_SSL=False).")
        urllib3.disable_warnings(InsecureRequestWarning)

    plots_enabled = bool(getattr(cfg, "PLOTS_ENABLED", True))

    as_values = parse_csv_values(cfg.AS_TAG_VALUES)
    disk_fs_preferences = parse_csv_values(cfg.DISK_FS) or ["/"]
    output_dir = Path(cfg.OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    plots_dir: Optional[Path] = None
    if plots_enabled:
        plots_dir = output_dir / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)

    if not cfg.ZABBIX_URL or not cfg.ZABBIX_USERNAME or not cfg.ZABBIX_PASSWORD:
        raise SystemExit(
            "Set ZABBIX_URL, ZABBIX_USERNAME and ZABBIX_PASSWORD in config.py."
        )

    log("Connecting to Zabbix API...")
    api = ZabbixAPI(
        url=cfg.ZABBIX_URL,
        username=cfg.ZABBIX_USERNAME,
        password=cfg.ZABBIX_PASSWORD,
        timeout=cfg.REQUEST_TIMEOUT,
        verify_ssl=cfg.VERIFY_SSL,
    )
    try:
        try:
            version = api.call("apiinfo.version", {})
        except Exception:
            version = "unknown"

        log("Loading hosts by AS tag filter...")
        hosts = get_hosts_by_as(api, cfg.AS_TAG_KEY, as_values, cfg.TAG_OPERATOR)
        if not hosts:
            raise SystemExit("No hosts matched the AS tag filter.")
        log(f"Matched hosts: {len(hosts)}")

        host_meta: Dict[str, Dict[str, str]] = {}
        for host in hosts:
            hostid = str(host.get("hostid"))
            as_value = pick_as_value(host.get("tags", []), cfg.AS_TAG_KEY, as_values)
            host_meta[hostid] = {"host": host.get("host", ""), "name": host.get("name", ""), "as_value": as_value}

        hostids = list(host_meta.keys())
        log("Loading enabled items for matched hosts...")
        items = get_items_for_hosts(api, hostids, chunk_size=cfg.CHUNK_SIZE)
        log(f"Loaded items: {len(items)}")

        items_by_host = index_items_by_host(items)
        direct_items, ram_pairs = select_items(items_by_host, host_meta, disk_fs_preferences)

        cpu_count = len([item for item in direct_items if item.metric == "cpu"])
        ram_direct_count = len([item for item in direct_items if item.metric == "ram"])
        disk_count = len([item for item in direct_items if item.metric == "disk"])
        log(
            "Selected items: "
            f"CPU={cpu_count}, RAM-direct={ram_direct_count}, RAM-pairs={len(ram_pairs)}, Disk={disk_count}"
        )

        if cpu_count == 0 or (ram_direct_count == 0 and len(ram_pairs) == 0) or disk_count == 0:
            log(
                "Warning: some metrics are partially missing. "
                "The script will continue with available metrics."
            )

        now = datetime.now(timezone.utc)
        history_from = int((now - timedelta(days=cfg.HISTORY_DAYS)).timestamp())
        trend_from = int((now - timedelta(days=cfg.TREND_DAYS)).timestamp())
        time_till = int(now.timestamp())

        direct_itemid_to_type = {item.itemid: item.value_type for item in direct_items}
        ram_pair_itemid_to_type: Dict[str, int] = {}
        for pair in ram_pairs:
            ram_pair_itemid_to_type[pair.total_itemid] = pair.total_value_type
            ram_pair_itemid_to_type[pair.part_itemid] = pair.part_value_type

        history_itemids = {**direct_itemid_to_type, **ram_pair_itemid_to_type}
        trend_itemids = sorted(set(list(direct_itemid_to_type.keys()) + list(ram_pair_itemid_to_type.keys())))

        log(f"Collecting {cfg.HISTORY_DAYS}-day exact history from history.get...")
        raw_history = fetch_history_points(
            api,
            itemid_to_value_type=history_itemids,
            time_from=history_from,
            time_till=time_till,
            chunk_size=cfg.CHUNK_SIZE,
        )
        log(f"Exact datapoints: {len(raw_history)}")

        log(f"Collecting {cfg.TREND_DAYS}-day trend data from trend.get...")
        raw_trend = fetch_trend_points(
            api,
            itemids=trend_itemids,
            time_from=trend_from,
            time_till=time_till,
            chunk_size=cfg.CHUNK_SIZE,
        )
        log(f"Trend datapoints: {len(raw_trend)}")

        history_direct = build_direct_history(raw_history, direct_items)
        history_ram_pairs = build_ram_pair_history(raw_history, ram_pairs)
        history_util = pd.concat([history_direct, history_ram_pairs], ignore_index=True)

        trend_direct = build_direct_trend(raw_trend, direct_items)
        trend_ram_pairs = build_ram_pair_trend(raw_trend, ram_pairs)
        trend_util = pd.concat([trend_direct, trend_ram_pairs], ignore_index=True)

        if history_util.empty and trend_util.empty:
            raise SystemExit("No utilization data extracted from selected items.")

        history_summary_all = summarize_history(history_util, by_as=False)
        history_summary_as = summarize_history(history_util, by_as=True)
        trend_summary_all = summarize_trend(trend_util, by_as=False)
        trend_summary_as = summarize_trend(trend_util, by_as=True)

        native_forecast_keys = {
            "cpu": str(getattr(cfg, "FORECAST_KEY_CPU", "")).strip(),
            "ram": str(getattr(cfg, "FORECAST_KEY_RAM", "")).strip(),
            "disk": str(getattr(cfg, "FORECAST_KEY_DISK", "")).strip(),
        }
        native_forecast_items = []
        forecast = pd.DataFrame(
            columns=["metric", "timestamp", "is_future", "actual", "fitted", "predicted", "lower", "upper"]
        )
        if forecast_source == "zabbix":
            native_forecast_items = select_native_forecast_items(
                items_by_host=items_by_host,
                host_meta=host_meta,
                key_map=native_forecast_keys,
            )
            configured_metrics = [metric for metric, key_ in native_forecast_keys.items() if key_]
            log(
                "Native forecast mode: "
                f"configured metrics={len(configured_metrics)}, selected items={len(native_forecast_items)}"
            )
            if native_forecast_items:
                native_itemid_to_type = {item.itemid: item.value_type for item in native_forecast_items}
                native_from = int((now - timedelta(days=forecast_lookback_days)).timestamp())
                log(
                    f"Collecting native forecast history from history.get "
                    f"({forecast_lookback_days} days)..."
                )
                raw_native_forecast = fetch_history_points(
                    api,
                    itemid_to_value_type=native_itemid_to_type,
                    time_from=native_from,
                    time_till=time_till,
                    chunk_size=cfg.CHUNK_SIZE,
                )
                log(f"Native forecast datapoints: {len(raw_native_forecast)}")
                forecast = build_native_forecast(
                    raw_history=raw_native_forecast,
                    selections=native_forecast_items,
                    horizon_days=cfg.FORECAST_DAYS,
                    now_ts=pd.Timestamp(now),
                )
                if forecast.empty:
                    log("Warning: native forecast data is empty; fallback to python forecast model.")
            else:
                log("Warning: no native forecast items matched configured keys; fallback to python forecast model.")

        if forecast.empty:
            forecast = build_forecast(trend_summary_all, horizon_days=cfg.FORECAST_DAYS)
            if forecast_source == "zabbix":
                log("Forecast source used: python fallback.")
        else:
            log("Forecast source used: zabbix native.")

        selection_rows = []
        for item in direct_items:
            selection_rows.append(
                {
                    "hostid": item.hostid,
                    "host": item.host,
                    "as_value": item.as_value,
                    "metric": item.metric,
                    "source": item.source,
                    "itemid": item.itemid,
                    "key_": item.key_,
                    "value_type": item.value_type,
                    "transform": item.transform,
                }
            )
        for pair in ram_pairs:
            selection_rows.append(
                {
                    "hostid": pair.hostid,
                    "host": pair.host,
                    "as_value": pair.as_value,
                    "metric": "ram",
                    "source": "pair",
                    "itemid": pair.part_itemid,
                    "key_": f"{pair.mode}+total",
                    "value_type": pair.part_value_type,
                    "transform": pair.mode,
                }
            )
        for item in native_forecast_items:
            selection_rows.append(
                {
                    "hostid": item.hostid,
                    "host": item.host,
                    "as_value": item.as_value,
                    "metric": item.metric,
                    "source": item.source,
                    "itemid": item.itemid,
                    "key_": item.key_,
                    "value_type": item.value_type,
                    "transform": item.transform,
                }
            )
        selection_report = pd.DataFrame(selection_rows)

        log("Saving CSV artifacts...")
        save_csv(selection_report, output_dir / "selected_items.csv")
        save_csv(history_util, output_dir / f"history_exact_{cfg.HISTORY_DAYS}d.csv")
        save_csv(trend_util, output_dir / f"trend_{cfg.TREND_DAYS}d.csv")
        save_csv(history_summary_all, output_dir / f"history_summary_all_{cfg.HISTORY_DAYS}d.csv")
        save_csv(history_summary_as, output_dir / f"history_summary_by_as_{cfg.HISTORY_DAYS}d.csv")
        save_csv(trend_summary_all, output_dir / f"trend_summary_all_{cfg.TREND_DAYS}d.csv")
        save_csv(trend_summary_as, output_dir / f"trend_summary_by_as_{cfg.TREND_DAYS}d.csv")
        save_csv(forecast, output_dir / f"forecast_{cfg.FORECAST_DAYS}d.csv")

        context = {
            "run_at_utc": now.isoformat(),
            "api_url": api.url,
            "api_version": version,
            "as_tag_key": cfg.AS_TAG_KEY,
            "as_tag_values": as_values,
            "history_days": cfg.HISTORY_DAYS,
            "trend_days": cfg.TREND_DAYS,
            "forecast_days": cfg.FORECAST_DAYS,
            "forecast_source": forecast_source,
            "forecast_lookback_days": forecast_lookback_days,
            "forecast_native_keys": native_forecast_keys,
            "host_count": len(hosts),
            "selected": {
                "cpu_direct": cpu_count,
                "ram_direct": ram_direct_count,
                "ram_pair": len(ram_pairs),
                "disk_direct": disk_count,
                "native_forecast_items": len(native_forecast_items),
            },
            "verify_ssl": cfg.VERIFY_SSL,
            "chunk_size": cfg.CHUNK_SIZE,
            "request_timeout": cfg.REQUEST_TIMEOUT,
            "plots_enabled": plots_enabled,
        }
        with (output_dir / "run_context.json").open("w", encoding="utf-8") as file:
            json.dump(context, file, indent=2, ensure_ascii=False)

        if plots_enabled and plots_dir is not None:
            metrics = sorted(
                set(history_util["metric"].unique())
                .union(set(trend_util["metric"].unique()))
                .union(set(forecast["metric"].unique()))
            )
            log("Building plots...")
            for metric in metrics:
                metric_history_raw = history_util[history_util["metric"] == metric].copy()
                metric_history_summary = history_summary_all[history_summary_all["metric"] == metric].copy()
                metric_trend_summary = trend_summary_all[trend_summary_all["metric"] == metric].copy()
                metric_forecast = forecast[forecast["metric"] == metric].copy()

                plot_metric_dashboard(
                    metric=metric,
                    history_raw=metric_history_raw,
                    history_summary=metric_history_summary,
                    trend_summary=metric_trend_summary,
                    forecast=metric_forecast,
                    history_days=cfg.HISTORY_DAYS,
                    trend_days=cfg.TREND_DAYS,
                    output_file=plots_dir / f"{metric}_dashboard.png",
                )
                plot_as_breakdown(
                    metric=metric,
                    history_by_as=history_summary_as,
                    output_file=plots_dir / f"{metric}_by_as.png",
                )
        else:
            log("Skipping plot generation (plotting disabled in config).")

        log(f"Done. Outputs are in: {output_dir.resolve()}")
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
