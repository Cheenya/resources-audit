#!/usr/bin/env python3
"""Collect CPU/RAM/Disk utilization from Zabbix API, summarize and plot."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
import urllib3
from urllib3.exceptions import InsecureRequestWarning

import config as cfg
from plotting import plot_as_breakdown, plot_metric_dashboard
from processing import (
    build_direct_history,
    build_direct_trend,
    build_feature_history,
    build_feature_trend,
    fetch_history_points,
    fetch_trend_points,
    get_hosts_by_as,
    get_items_for_hosts,
    index_items_by_host,
    parse_csv_values,
    pick_as_value,
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


def append_csv(frame: pd.DataFrame, path: Path) -> None:
    if frame.empty:
        return
    has_existing_data = path.exists() and path.stat().st_size > 0
    frame.to_csv(path, index=False, mode="a", header=not has_existing_data)


def load_timeseries_csv(path: Path, columns: Sequence[str]) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=list(columns))
    frame = pd.read_csv(path)
    for column in ("itemid", "hostid", "host", "as_value", "metric", "feature", "entity"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    for column in frame.columns:
        if column in ("itemid", "hostid", "host", "as_value", "metric", "feature", "entity", "clock"):
            continue
        if pd.api.types.is_integer_dtype(frame[column]):
            frame[column] = pd.to_numeric(frame[column], errors="coerce", downcast="integer")
        elif pd.api.types.is_float_dtype(frame[column]):
            frame[column] = pd.to_numeric(frame[column], errors="coerce", downcast="float")
    if "clock" in frame.columns:
        frame["clock"] = pd.to_datetime(frame["clock"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["clock"])
    return frame


def save_xlsx(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            safe_sheet_name = sheet_name[:31]
            frame.to_excel(writer, sheet_name=safe_sheet_name, index=False)


def build_conclusion(
    run_at: datetime,
    matched_hosts: int,
    selected_counts: Dict[str, int],
    history_summary_all: pd.DataFrame,
    trend_summary_all: pd.DataFrame,
) -> pd.DataFrame:
    rows = [
        {"section": "run", "key": "run_at_utc", "value": run_at.isoformat()},
        {"section": "run", "key": "matched_hosts", "value": str(matched_hosts)},
        {"section": "selection", "key": "cpu_target_items", "value": str(selected_counts.get("cpu_target", 0))},
        {"section": "selection", "key": "ram_target_items", "value": str(selected_counts.get("ram_target", 0))},
        {"section": "selection", "key": "disk_target_items", "value": str(selected_counts.get("disk_target", 0))},
        {"section": "selection", "key": "ram_feature_items", "value": str(selected_counts.get("ram_features", 0))},
        {"section": "selection", "key": "disk_feature_items", "value": str(selected_counts.get("disk_features", 0))},
        {"section": "selection", "key": "feature_items_total", "value": str(selected_counts.get("feature_total", 0))},
    ]

    metrics = sorted(
        set(history_summary_all["metric"].unique()).union(set(trend_summary_all["metric"].unique()))
    )
    for metric in metrics:
        metric_hist = history_summary_all[history_summary_all["metric"] == metric].sort_values("clock")
        if not metric_hist.empty:
            latest = metric_hist.iloc[-1]
            mean_value = float(latest["util_mean"])
            p90_value = float(latest["util_p90"])
            if p90_value >= 90.0:
                status = "critical"
            elif mean_value >= 80.0:
                status = "high"
            else:
                status = "normal"
            rows.append({"section": metric, "key": "latest_history_mean_pct", "value": f"{mean_value:.2f}"})
            rows.append({"section": metric, "key": "latest_history_p90_pct", "value": f"{p90_value:.2f}"})
            rows.append({"section": metric, "key": "status", "value": status})

        metric_trend = trend_summary_all[trend_summary_all["metric"] == metric].sort_values("clock")
        if not metric_trend.empty:
            daily = (
                metric_trend.set_index("clock")["util_avg_mean"]
                .resample("1D")
                .mean()
                .dropna()
            )
            if len(daily) >= 14:
                recent_7d = float(daily.iloc[-7:].mean())
                prev_7d = float(daily.iloc[-14:-7].mean())
                delta = recent_7d - prev_7d
                trend_state = "up" if delta > 1.0 else ("down" if delta < -1.0 else "flat")
                rows.append({"section": metric, "key": "trend_recent_7d_mean_pct", "value": f"{recent_7d:.2f}"})
                rows.append({"section": metric, "key": "trend_prev_7d_mean_pct", "value": f"{prev_7d:.2f}"})
                rows.append({"section": metric, "key": "trend_delta_pp", "value": f"{delta:.2f}"})
                rows.append({"section": metric, "key": "trend_state", "value": trend_state})

    return pd.DataFrame(rows, columns=["section", "key", "value"])


def main() -> int:
    if cfg.TAG_OPERATOR not in ("equals", "contains"):
        raise SystemExit("TAG_OPERATOR in config.py must be 'equals' or 'contains'.")
    if cfg.HISTORY_DAYS < 0 or cfg.TREND_DAYS <= 0:
        raise SystemExit("HISTORY_DAYS must be >= 0 and TREND_DAYS must be > 0.")
    if cfg.CHUNK_SIZE <= 0 or cfg.REQUEST_TIMEOUT <= 0:
        raise SystemExit("CHUNK_SIZE and REQUEST_TIMEOUT in config.py must be > 0.")
    if not isinstance(cfg.VERIFY_SSL, bool):
        raise SystemExit("VERIFY_SSL in config.py must be boolean.")
    item_chunk_size = int(getattr(cfg, "ITEM_CHUNK_SIZE", cfg.CHUNK_SIZE))
    history_chunk_size = int(getattr(cfg, "HISTORY_CHUNK_SIZE", cfg.CHUNK_SIZE))
    trend_chunk_size = int(getattr(cfg, "TREND_CHUNK_SIZE", cfg.CHUNK_SIZE))
    if item_chunk_size <= 0 or history_chunk_size <= 0 or trend_chunk_size <= 0:
        raise SystemExit("ITEM_CHUNK_SIZE/HISTORY_CHUNK_SIZE/TREND_CHUNK_SIZE must be > 0.")

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
        items = get_items_for_hosts(api, hostids, chunk_size=item_chunk_size)
        log(f"Loaded items: {len(items)}")

        items_by_host = index_items_by_host(items)
        direct_items, feature_items = select_items(items_by_host, host_meta, disk_fs_preferences)

        cpu_target_count = len([item for item in direct_items if item.metric == "cpu"])
        ram_target_count = len([item for item in direct_items if item.metric == "ram"])
        disk_target_count = len([item for item in direct_items if item.metric == "disk"])
        ram_feature_count = len([item for item in feature_items if item.metric == "ram"])
        disk_feature_count = len([item for item in feature_items if item.metric == "disk"])
        feature_total_count = len(feature_items)
        log(
            "Selected items: "
            f"CPU-target={cpu_target_count}, RAM-target={ram_target_count}, "
            f"Disk-target={disk_target_count}, RAM-features={ram_feature_count}, "
            f"Disk-features={disk_feature_count}"
        )

        if cpu_target_count == 0 or ram_target_count == 0 or disk_target_count == 0:
            log(
                "Warning: some metrics are partially missing. "
                "The script will continue with available metrics."
            )

        now = datetime.now(timezone.utc)
        history_all_available = int(cfg.HISTORY_DAYS) == 0
        history_from = 1 if history_all_available else int((now - timedelta(days=cfg.HISTORY_DAYS)).timestamp())
        trend_from = int((now - timedelta(days=cfg.TREND_DAYS)).timestamp())
        time_till = int(now.timestamp())
        history_window_label = "all" if history_all_available else f"{cfg.HISTORY_DAYS}d"
        trend_window_label = f"{cfg.TREND_DAYS}d"

        direct_itemid_to_type = {item.itemid: item.value_type for item in direct_items}
        feature_itemid_to_type = {item.itemid: item.value_type for item in feature_items}

        history_itemids = {
            **direct_itemid_to_type,
            **feature_itemid_to_type,
        }
        trend_itemids = sorted(
            set(
                list(direct_itemid_to_type.keys())
                + list(feature_itemid_to_type.keys())
            )
        )

        history_raw_path = output_dir / f"history_raw_api_{history_window_label}.csv"
        trend_raw_path = output_dir / f"trend_raw_api_{trend_window_label}.csv"
        history_util_path = output_dir / f"history_exact_{history_window_label}.csv"
        trend_util_path = output_dir / f"trend_{trend_window_label}.csv"
        history_features_path = output_dir / f"history_features_{history_window_label}.csv"
        trend_features_path = output_dir / f"trend_features_{trend_window_label}.csv"
        for path in (
            history_raw_path,
            trend_raw_path,
            history_util_path,
            trend_util_path,
            history_features_path,
            trend_features_path,
        ):
            if path.exists():
                path.unlink()

        history_raw_count = 0
        history_util_count = 0
        history_feature_count = 0

        def on_history_chunk(chunk: pd.DataFrame) -> None:
            nonlocal history_raw_count, history_util_count, history_feature_count
            history_raw_count += len(chunk)
            append_csv(chunk, history_raw_path)

            util_chunk = build_direct_history(chunk, direct_items)
            history_util_count += len(util_chunk)
            append_csv(util_chunk, history_util_path)

            feature_chunk = build_feature_history(chunk, feature_items)
            history_feature_count += len(feature_chunk)
            append_csv(feature_chunk, history_features_path)

        if history_all_available:
            log("Collecting all available exact history from history.get...")
        else:
            log(f"Collecting {cfg.HISTORY_DAYS}-day exact history from history.get...")
        fetch_history_points(
            api,
            itemid_to_value_type=history_itemids,
            time_from=history_from,
            time_till=time_till,
            chunk_size=history_chunk_size,
            on_chunk=on_history_chunk,
            collect=False,
        )
        log(
            f"Exact datapoints: raw={history_raw_count}, "
            f"target={history_util_count}, features={history_feature_count}"
        )
        log(f"Saved raw history API data: {history_raw_path.name}")

        trend_raw_count = 0
        trend_util_count = 0
        trend_feature_count = 0

        def on_trend_chunk(chunk: pd.DataFrame) -> None:
            nonlocal trend_raw_count, trend_util_count, trend_feature_count
            trend_raw_count += len(chunk)
            append_csv(chunk, trend_raw_path)

            util_chunk = build_direct_trend(chunk, direct_items)
            trend_util_count += len(util_chunk)
            append_csv(util_chunk, trend_util_path)

            feature_chunk = build_feature_trend(chunk, feature_items)
            trend_feature_count += len(feature_chunk)
            append_csv(feature_chunk, trend_features_path)

        log(f"Collecting {cfg.TREND_DAYS}-day trend data from trend.get...")
        fetch_trend_points(
            api,
            itemids=trend_itemids,
            time_from=trend_from,
            time_till=time_till,
            chunk_size=trend_chunk_size,
            on_chunk=on_trend_chunk,
            collect=False,
        )
        log(
            f"Trend datapoints: raw={trend_raw_count}, "
            f"target={trend_util_count}, features={trend_feature_count}"
        )
        log(f"Saved raw trend API data: {trend_raw_path.name}")

        history_util = load_timeseries_csv(
            history_util_path,
            [
                "metric",
                "clock",
                "hostid",
                "host",
                "as_value",
                "itemid",
                "utilization_pct",
            ],
        )
        trend_util = load_timeseries_csv(
            trend_util_path,
            [
                "metric",
                "clock",
                "hostid",
                "host",
                "as_value",
                "itemid",
                "num",
                "util_min",
                "util_avg",
                "util_max",
            ],
        )

        if history_util.empty and trend_util.empty:
            raise SystemExit("No utilization data extracted from selected items.")
        log(
            "Saved utilization checkpoints: "
            f"{history_util_path.name}, {trend_util_path.name}, "
            f"{history_features_path.name}, {trend_features_path.name}"
        )

        history_summary_all = summarize_history(history_util, by_as=False)
        history_summary_as = summarize_history(history_util, by_as=True)
        trend_summary_all = summarize_trend(trend_util, by_as=False)
        trend_summary_as = summarize_trend(trend_util, by_as=True)

        selection_rows = []
        for item in direct_items:
            selection_rows.append(
                {
                    "hostid": item.hostid,
                    "host": item.host,
                    "as_value": item.as_value,
                    "metric": item.metric,
                    "source": "target",
                    "feature": "",
                    "entity": "",
                    "itemid": item.itemid,
                    "key_": item.key_,
                    "value_type": item.value_type,
                    "transform": item.transform,
                }
            )
        for item in feature_items:
            selection_rows.append(
                {
                    "hostid": item.hostid,
                    "host": item.host,
                    "as_value": item.as_value,
                    "metric": item.metric,
                    "source": "feature",
                    "feature": item.feature,
                    "entity": item.entity,
                    "itemid": item.itemid,
                    "key_": item.key_,
                    "value_type": item.value_type,
                    "transform": item.transform,
                }
            )
        selection_report = pd.DataFrame(selection_rows)

        log("Saving CSV artifacts...")
        save_csv(selection_report, output_dir / "selected_items.csv")
        save_csv(history_summary_all, output_dir / f"history_summary_all_{history_window_label}.csv")
        save_csv(history_summary_as, output_dir / f"history_summary_by_as_{history_window_label}.csv")
        save_csv(trend_summary_all, output_dir / f"trend_summary_all_{trend_window_label}.csv")
        save_csv(trend_summary_as, output_dir / f"trend_summary_by_as_{trend_window_label}.csv")

        selected_counts = {
            "cpu_target": cpu_target_count,
            "ram_target": ram_target_count,
            "disk_target": disk_target_count,
            "ram_features": ram_feature_count,
            "disk_features": disk_feature_count,
            "feature_total": feature_total_count,
        }
        conclusion = build_conclusion(
            run_at=now,
            matched_hosts=len(hosts),
            selected_counts=selected_counts,
            history_summary_all=history_summary_all,
            trend_summary_all=trend_summary_all,
        )

        xlsx_path = output_dir / f"summary_report_{history_window_label}_{trend_window_label}.xlsx"
        save_xlsx(
            xlsx_path,
            {
                "selected_items": selection_report,
                "history_summary_all": history_summary_all,
                "history_summary_by_as": history_summary_as,
                "trend_summary_all": trend_summary_all,
                "trend_summary_by_as": trend_summary_as,
                "conclusion": conclusion,
            },
        )
        log(f"Saved XLSX summary report: {xlsx_path.name}")

        context = {
            "run_at_utc": now.isoformat(),
            "api_url": api.url,
            "api_version": version,
            "as_tag_key": cfg.AS_TAG_KEY,
            "as_tag_values": as_values,
            "history_days": cfg.HISTORY_DAYS,
            "history_mode": "all_available" if history_all_available else "fixed_days",
            "history_window_label": history_window_label,
            "trend_days": cfg.TREND_DAYS,
            "trend_window_label": trend_window_label,
            "host_count": len(hosts),
            "selected": {
                **selected_counts,
            },
            "verify_ssl": cfg.VERIFY_SSL,
            "chunk_size": cfg.CHUNK_SIZE,
            "item_chunk_size": item_chunk_size,
            "history_chunk_size": history_chunk_size,
            "trend_chunk_size": trend_chunk_size,
            "request_timeout": cfg.REQUEST_TIMEOUT,
            "plots_enabled": plots_enabled,
        }
        with (output_dir / "run_context.json").open("w", encoding="utf-8") as file:
            json.dump(context, file, indent=2, ensure_ascii=False)

        if plots_enabled and plots_dir is not None:
            metrics = sorted(
                set(history_util["metric"].unique())
                .union(set(trend_util["metric"].unique()))
            )
            log("Building plots...")
            for metric in metrics:
                metric_history_raw = history_util[history_util["metric"] == metric].copy()
                metric_history_summary = history_summary_all[history_summary_all["metric"] == metric].copy()
                metric_trend_summary = trend_summary_all[trend_summary_all["metric"] == metric].copy()

                plot_metric_dashboard(
                    metric=metric,
                    history_raw=metric_history_raw,
                    history_summary=metric_history_summary,
                    trend_summary=metric_trend_summary,
                    history_window_label=history_window_label,
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
