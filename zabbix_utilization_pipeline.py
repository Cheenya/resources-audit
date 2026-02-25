#!/usr/bin/env python3
"""Collect CPU/RAM/Disk utilization from Zabbix API, summarize and plot."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
import urllib3
from urllib3.exceptions import InsecureRequestWarning

import config as cfg
from forecasting import (
    build_actionable_recommendations,
    build_daily_p95_target,
    compute_horizon_risk_probabilities,
    compute_host_risk_metrics,
    run_host_metric_forecasts,
)
from plotting import plot_as_breakdown, plot_host_forecast, plot_metric_dashboard
from processing import (
    build_direct_history,
    build_direct_trend,
    build_feature_history,
    build_feature_trend,
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
    for column in ("itemid", "hostid", "host", "as_value", "env_value", "env_group", "metric", "feature", "entity"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    for column in frame.columns:
        if column in ("itemid", "hostid", "host", "as_value", "env_value", "env_group", "metric", "feature", "entity", "clock"):
            continue
        if pd.api.types.is_integer_dtype(frame[column]):
            frame[column] = pd.to_numeric(frame[column], errors="coerce", downcast="integer")
        elif pd.api.types.is_float_dtype(frame[column]):
            frame[column] = pd.to_numeric(frame[column], errors="coerce", downcast="float")
    if "clock" in frame.columns:
        frame["clock"] = pd.to_datetime(frame["clock"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["clock"])
    return frame


def parse_int_values(raw: object) -> List[int]:
    if isinstance(raw, str):
        values = parse_csv_values(raw)
    elif isinstance(raw, Sequence):
        values = [str(value).strip() for value in raw if str(value).strip()]
    else:
        values = [str(raw).strip()]
    parsed: List[int] = []
    for value in values:
        parsed.append(int(value))
    return parsed


def safe_slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    cleaned = cleaned.strip("._")
    return cleaned or "host"


def load_selection_report(path: Path) -> pd.DataFrame:
    columns = [
        "hostid",
        "host",
        "as_value",
        "env_value",
        "env_group",
        "metric",
        "source",
        "feature",
        "entity",
        "itemid",
        "key_",
        "value_type",
        "transform",
    ]
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    report = pd.read_csv(path)
    for column in columns:
        if column not in report.columns:
            report[column] = ""
    for column in (
        "hostid",
        "host",
        "as_value",
        "env_value",
        "env_group",
        "metric",
        "source",
        "feature",
        "entity",
        "itemid",
        "key_",
        "transform",
    ):
        if column in report.columns:
            report[column] = report[column].astype("string")
    if "value_type" in report.columns:
        report["value_type"] = pd.to_numeric(report["value_type"], errors="coerce", downcast="integer")
    return report


def build_selected_counts_from_report(selection_report: pd.DataFrame) -> Dict[str, int]:
    if selection_report.empty:
        return {
            "cpu_target": 0,
            "ram_target": 0,
            "disk_target": 0,
            "ram_features": 0,
            "disk_features": 0,
            "feature_total": 0,
        }
    source = selection_report["source"].fillna("").astype("string")
    metric = selection_report["metric"].fillna("").astype("string")
    target_mask = source == "target"
    feature_mask = source == "feature"
    return {
        "cpu_target": int(((metric == "cpu") & target_mask).sum()),
        "ram_target": int(((metric == "ram") & target_mask).sum()),
        "disk_target": int(((metric == "disk") & target_mask).sum()),
        "ram_features": int(((metric == "ram") & feature_mask).sum()),
        "disk_features": int(((metric == "disk") & feature_mask).sum()),
        "feature_total": int(feature_mask.sum()),
    }


def extract_tag_value(tags: Sequence[Dict], tag_key: str) -> str:
    for tag in tags:
        if str(tag.get("tag", "")) == tag_key:
            value = str(tag.get("value", "")).strip()
            if value:
                return value
    return ""


def env_group_from_value(env_value: str) -> str:
    return "prod" if str(env_value).strip().lower() == "prod" else "non-prod"


def ensure_scope_columns(
    frame: pd.DataFrame,
    host_scope: Optional[pd.DataFrame],
) -> pd.DataFrame:
    result = frame.copy()
    if "env_value" not in result.columns:
        result["env_value"] = ""
    if "env_group" not in result.columns:
        result["env_group"] = "non-prod"

    if host_scope is not None and not host_scope.empty:
        mapped = host_scope.drop_duplicates(subset=["hostid"]).set_index("hostid")
        if "hostid" in result.columns:
            hostid_series = result["hostid"].astype("string")
            result["env_value"] = (
                hostid_series.map(mapped["env_value"])
                .fillna(result["env_value"])
                .fillna("")
                .astype("string")
            )
            result["env_group"] = (
                hostid_series.map(mapped["env_group"])
                .fillna(result["env_group"])
                .fillna("non-prod")
                .astype("string")
            )

    result["env_value"] = result["env_value"].fillna("").astype("string")
    result["env_group"] = result["env_group"].fillna("").map(env_group_from_value).astype("string")
    return result


def format_probability(value: object) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(num):
        return "n/a"
    return f"{num:.1f}%"


def format_days(value: object) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(num):
        return "n/a"
    return f"{int(num)} дн."


def write_scope_text_conclusion(
    path: Path,
    as_value: str,
    env_group: str,
    actionable: pd.DataFrame,
    risk_probabilities: pd.DataFrame,
    run_at: datetime,
) -> None:
    title_as = as_value if as_value else "<empty>"
    lines = [
        f"Заключение по ресурсам ({run_at.strftime('%Y-%m-%d %H:%M:%S')} UTC)",
        f"AS: {title_as}",
        f"ENV-группа: {env_group}",
        "",
    ]

    if actionable.empty:
        lines.append("Нет данных для формирования заключения.")
        path.write_text("\n".join(lines), encoding="utf-8")
        return

    key_columns = ["metric", "hostid", "host", "as_value"]
    optional_columns = [column for column in ("env_value", "env_group") if column in actionable.columns]
    all_key_columns = [*key_columns, *optional_columns]
    probability_map: Dict[tuple, Dict[int, Dict[str, float]]] = {}
    if not risk_probabilities.empty:
        for _, row in risk_probabilities.iterrows():
            key = tuple(str(row.get(column, "")) for column in all_key_columns)
            horizon = int(row.get("horizon_days", 0))
            probability_map.setdefault(key, {})[horizon] = {
                "prob_cross_adjusted_pct": float(row.get("prob_cross_adjusted_pct", float("nan"))),
                "confidence_index_pct": float(row.get("confidence_index_pct", float("nan"))),
            }

    rows = actionable.copy()
    rows["days_to_90_basis"] = pd.to_numeric(rows["days_to_90_basis"], errors="coerce")
    rows = rows.sort_values(["days_to_90_basis", "host", "metric"], na_position="last")

    critical_now = rows[rows["days_to_90_basis"] <= 30]
    critical_soon = rows[(rows["days_to_90_basis"] > 30) & (rows["days_to_90_basis"] <= 90)]
    risk_6m = rows[(rows["days_to_90_basis"] > 90) & (rows["days_to_90_basis"] <= 180)]
    risk_12m = rows[(rows["days_to_90_basis"] > 180) & (rows["days_to_90_basis"] <= 365)]
    stable = rows[(rows["days_to_90_basis"].isna()) | (rows["days_to_90_basis"] > 365)]
    overprovisioned = rows[rows["status"] == "overprovisioned"]

    lines.append(f"Всего host/metric: {len(rows)}")
    lines.append(f"Критично сейчас (<=30д): {len(critical_now)}")
    lines.append(f"Критично скоро (31-90д): {len(critical_soon)}")
    lines.append(f"Риск в горизонте 6м (91-180д): {len(risk_6m)}")
    lines.append(f"Риск в горизонте 12м (181-365д): {len(risk_12m)}")
    lines.append(f"Стабильно (>365д или нет пересечения): {len(stable)}")
    lines.append(f"Избыточно (overprovisioned): {len(overprovisioned)}")
    lines.append("")

    def append_section(title: str, section_frame: pd.DataFrame, horizon: int) -> None:
        lines.append(title)
        if section_frame.empty:
            lines.append("- нет")
            lines.append("")
            return
        for _, row in section_frame.iterrows():
            key = tuple(str(row.get(column, "")) for column in all_key_columns)
            horizon_info = probability_map.get(key, {}).get(horizon, {})
            probability = format_probability(horizon_info.get("prob_cross_adjusted_pct"))
            confidence = format_probability(horizon_info.get("confidence_index_pct"))
            lines.append(
                f"- {row.get('host','')} [{str(row.get('metric','')).upper()}]: "
                f"до 90% ≈ {format_days(row.get('days_to_90_basis'))}, "
                f"P(<= {horizon}д)={probability}, доверие={confidence}, "
                f"статус={row.get('status','')}, рекомендация={row.get('recommendation','')}"
            )
        lines.append("")

    append_section("Критично сейчас (горизонт 30 дней):", critical_now, 30)
    append_section("Критично скоро (горизонт 90 дней):", critical_soon, 90)
    append_section("Риск в горизонте 6 месяцев:", risk_6m, 180)
    append_section("Риск в горизонте 12 месяцев:", risk_12m, 365)
    append_section("Стабильные:", stable, 365)
    append_section("Overprovisioned:", overprovisioned, 365)

    path.write_text("\n".join(lines), encoding="utf-8")


def pick_input_path(primary: Path, legacy: Path) -> Path:
    if primary.exists() and primary.stat().st_size > 0:
        return primary
    if legacy.exists() and legacy.stat().st_size > 0:
        return legacy
    return primary


def save_xlsx(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            safe_sheet_name = sheet_name[:31]
            frame_to_save = frame
            tz_columns = frame.select_dtypes(include=["datetimetz"]).columns
            if len(tz_columns) > 0:
                frame_to_save = frame.copy()
                for column in tz_columns:
                    frame_to_save[column] = frame_to_save[column].dt.tz_convert("UTC").dt.tz_localize(None)
            frame_to_save.to_excel(writer, sheet_name=safe_sheet_name, index=False)


def build_conclusion(
    run_at: datetime,
    matched_hosts: int,
    selected_counts: Dict[str, int],
    history_summary_all: pd.DataFrame,
    trend_summary_all: pd.DataFrame,
    risk_metrics: Optional[pd.DataFrame] = None,
    actionable_df: Optional[pd.DataFrame] = None,
    model_selection: Optional[pd.DataFrame] = None,
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

    if risk_metrics is not None and not risk_metrics.empty:
        cluster_counts = (
            risk_metrics["cluster"].fillna("unknown").value_counts().to_dict()
        )
        for cluster_name in ("cold", "warm", "hot", "unknown"):
            if cluster_name in cluster_counts:
                rows.append(
                    {
                        "section": "risk",
                        "key": f"cluster_{cluster_name}",
                        "value": str(int(cluster_counts[cluster_name])),
                    }
                )
        overprovisioned_count = int(risk_metrics["overprovisioned"].fillna(False).sum())
        rows.append(
            {
                "section": "risk",
                "key": "overprovisioned_hosts",
                "value": str(overprovisioned_count),
            }
        )

    if actionable_df is not None and not actionable_df.empty:
        status_counts = actionable_df["status"].fillna("unknown").value_counts().to_dict()
        for status_name in ("critical", "watch", "stable", "overprovisioned", "unknown"):
            if status_name in status_counts:
                rows.append(
                    {
                        "section": "actionable",
                        "key": f"status_{status_name}",
                        "value": str(int(status_counts[status_name])),
                    }
                )

    if model_selection is not None and not model_selection.empty:
        model_counts = model_selection["selected_model"].fillna("unknown").value_counts().to_dict()
        for model_name in ("seasonal_naive", "robust_trend", "gbdt_lag", "unknown"):
            if model_name in model_counts:
                rows.append(
                    {
                        "section": "forecast",
                        "key": f"model_{model_name}",
                        "value": str(int(model_counts[model_name])),
                    }
                )

    return pd.DataFrame(rows, columns=["section", "key", "value"])


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Zabbix utilization pipeline.")
    parser.add_argument(
        "--analysis-only",
        action="store_true",
        help="Skip Zabbix API collection and run analysis from existing CSV files in OUTPUT_DIR.",
    )
    args = parser.parse_args(argv)
    analysis_only = bool(args.analysis_only)

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
    forecast_enabled = bool(getattr(cfg, "FORECAST_ENABLED", True))
    forecast_horizons = sorted(
        set(parse_int_values(getattr(cfg, "FORECAST_HORIZONS", "30,90")))
    )
    forecast_backtest_horizon_days = int(
        getattr(cfg, "FORECAST_BACKTEST_HORIZON_DAYS", 30)
    )
    forecast_backtest_folds = int(getattr(cfg, "FORECAST_BACKTEST_FOLDS", 3))
    forecast_min_train_days = int(getattr(cfg, "FORECAST_MIN_TRAIN_DAYS", 90))
    forecast_max_plots = int(getattr(cfg, "FORECAST_MAX_PLOTS", 12))

    if any(value <= 0 for value in forecast_horizons):
        raise SystemExit("FORECAST_HORIZONS must contain positive integers.")
    if (
        forecast_backtest_horizon_days <= 0
        or forecast_backtest_folds <= 0
        or forecast_min_train_days <= 0
        or forecast_max_plots < 0
    ):
        raise SystemExit(
            "FORECAST_BACKTEST_HORIZON_DAYS, FORECAST_BACKTEST_FOLDS, "
            "FORECAST_MIN_TRAIN_DAYS must be > 0 and FORECAST_MAX_PLOTS >= 0."
        )

    as_values = parse_csv_values(cfg.AS_TAG_VALUES)
    disk_fs_preferences = parse_csv_values(cfg.DISK_FS) or ["/"]
    output_dir = Path(cfg.OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_subdir = str(getattr(cfg, "CSV_SUBDIR", "csv")).strip() or "csv"
    xlsx_subdir = str(getattr(cfg, "XLSX_SUBDIR", "xlsx")).strip() or "xlsx"
    csv_dir = output_dir / csv_subdir
    xlsx_dir = output_dir / xlsx_subdir
    csv_dir.mkdir(parents=True, exist_ok=True)
    xlsx_dir.mkdir(parents=True, exist_ok=True)
    plots_dir: Optional[Path] = None
    if plots_enabled:
        plots_dir = output_dir / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    history_all_available = int(cfg.HISTORY_DAYS) == 0
    history_from = 1 if history_all_available else int((now - timedelta(days=cfg.HISTORY_DAYS)).timestamp())
    trend_from = int((now - timedelta(days=cfg.TREND_DAYS)).timestamp())
    time_till = int(now.timestamp())
    history_window_label = "all" if history_all_available else f"{cfg.HISTORY_DAYS}d"
    trend_window_label = f"{cfg.TREND_DAYS}d"

    history_raw_path = csv_dir / f"history_raw_api_{history_window_label}.csv"
    trend_raw_path = csv_dir / f"trend_raw_api_{trend_window_label}.csv"
    history_util_path = csv_dir / f"history_exact_{history_window_label}.csv"
    trend_util_path = csv_dir / f"trend_{trend_window_label}.csv"
    history_features_path = csv_dir / f"history_features_{history_window_label}.csv"
    trend_features_path = csv_dir / f"trend_features_{trend_window_label}.csv"
    selection_path = csv_dir / "selected_items.csv"
    legacy_history_util_path = output_dir / f"history_exact_{history_window_label}.csv"
    legacy_trend_util_path = output_dir / f"trend_{trend_window_label}.csv"
    legacy_selection_path = output_dir / "selected_items.csv"

    history_util = pd.DataFrame(
        columns=["metric", "clock", "hostid", "host", "as_value", "env_value", "env_group", "itemid", "utilization_pct"]
    )
    trend_util = pd.DataFrame(
        columns=["metric", "clock", "hostid", "host", "as_value", "env_value", "env_group", "itemid", "num", "util_min", "util_avg", "util_max"]
    )
    selection_report = pd.DataFrame(
        columns=[
            "hostid",
            "host",
            "as_value",
            "env_value",
            "env_group",
            "metric",
            "source",
            "feature",
            "entity",
            "itemid",
            "key_",
            "value_type",
            "transform",
        ]
    )
    selected_counts = build_selected_counts_from_report(selection_report)
    host_count = 0
    version = "not_requested"
    api: Optional[ZabbixAPI] = None

    try:
        if analysis_only:
            log("Running analysis-only mode (no API collection).")
            history_input_path = pick_input_path(history_util_path, legacy_history_util_path)
            trend_input_path = pick_input_path(trend_util_path, legacy_trend_util_path)
            selection_input_path = pick_input_path(selection_path, legacy_selection_path)
            history_util = load_timeseries_csv(
                history_input_path,
                [
                    "metric",
                    "clock",
                    "hostid",
                    "host",
                    "as_value",
                    "env_value",
                    "env_group",
                    "itemid",
                    "utilization_pct",
                ],
            )
            trend_util = load_timeseries_csv(
                trend_input_path,
                [
                    "metric",
                    "clock",
                    "hostid",
                    "host",
                    "as_value",
                    "env_value",
                    "env_group",
                    "itemid",
                    "num",
                    "util_min",
                    "util_avg",
                    "util_max",
                ],
            )
            if history_util.empty and trend_util.empty:
                raise SystemExit(
                    "No existing utilization CSV found for analysis. "
                    f"Expected at least one of: {history_util_path} or {legacy_history_util_path}; "
                    f"{trend_util_path} or {legacy_trend_util_path}"
                )
            selection_report = load_selection_report(selection_input_path)
            selected_counts = build_selected_counts_from_report(selection_report)
            host_count = int(
                pd.concat(
                    [
                        history_util["hostid"] if "hostid" in history_util.columns else pd.Series(dtype="string"),
                        trend_util["hostid"] if "hostid" in trend_util.columns else pd.Series(dtype="string"),
                    ],
                    ignore_index=True,
                ).dropna().astype("string").nunique()
            )
            if selection_report.empty:
                log(
                    f"Warning: {selection_path} not found or empty. "
                    "Selection section in reports will be empty."
                )
            log(
                "Loaded existing utilization checkpoints: "
                f"{history_input_path}, {trend_input_path}"
            )
        else:
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
                version = api.call("apiinfo.version", {})
            except Exception:
                version = "unknown"

            log("Loading hosts by AS tag filter...")
            hosts = get_hosts_by_as(api, cfg.AS_TAG_KEY, as_values, cfg.TAG_OPERATOR)
            if not hosts:
                raise SystemExit("No hosts matched the AS tag filter.")
            log(f"Matched hosts: {len(hosts)}")
            host_count = len(hosts)

            host_meta: Dict[str, Dict[str, str]] = {}
            for host in hosts:
                hostid = str(host.get("hostid"))
                as_value = pick_as_value(host.get("tags", []), cfg.AS_TAG_KEY, as_values)
                env_value = extract_tag_value(host.get("tags", []), cfg.ENV_TAG_KEY)
                host_meta[hostid] = {
                    "host": host.get("host", ""),
                    "name": host.get("name", ""),
                    "as_value": as_value,
                    "env_value": env_value,
                    "env_group": env_group_from_value(env_value),
                }

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

            direct_itemid_to_type = {item.itemid: item.value_type for item in direct_items}
            feature_itemid_to_type = {item.itemid: item.value_type for item in feature_items}
            trend_itemids = sorted(set(list(direct_itemid_to_type.keys()) + list(feature_itemid_to_type.keys())))
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
            trend_raw_count = 0
            trend_util_count = 0
            trend_feature_count = 0
            history_cutoff = (
                None
                if history_all_available
                else pd.Timestamp.fromtimestamp(history_from, tz=timezone.utc)
            )

            def on_trend_chunk(chunk: pd.DataFrame) -> None:
                nonlocal trend_raw_count
                nonlocal trend_util_count
                nonlocal trend_feature_count
                nonlocal history_raw_count
                nonlocal history_util_count
                nonlocal history_feature_count
                trend_raw_count += len(chunk)
                append_csv(chunk, trend_raw_path)

                util_chunk = build_direct_trend(chunk, direct_items)
                trend_util_count += len(util_chunk)
                append_csv(util_chunk, trend_util_path)

                feature_chunk = build_feature_trend(chunk, feature_items)
                trend_feature_count += len(feature_chunk)
                append_csv(feature_chunk, trend_features_path)

                history_source = chunk if history_cutoff is None else chunk[chunk["clock"] >= history_cutoff]
                if history_source.empty:
                    return

                history_chunk = history_source[["itemid", "clock", "value_avg"]].rename(columns={"value_avg": "value"})
                history_chunk["ns"] = 0
                history_raw_count += len(history_chunk)
                append_csv(history_chunk, history_raw_path)

                history_util_chunk = build_direct_history(history_chunk, direct_items)
                history_util_count += len(history_util_chunk)
                append_csv(history_util_chunk, history_util_path)

                history_feature_chunk = build_feature_history(history_chunk, feature_items)
                history_feature_count += len(history_feature_chunk)
                append_csv(history_feature_chunk, history_features_path)

            if history_all_available:
                log(
                    "Collecting trend data once and deriving all available exact history "
                    "from trend averages..."
                )
            else:
                log(
                    f"Collecting trend data once and deriving {cfg.HISTORY_DAYS}-day exact "
                    "history from trend averages..."
                )
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
            log(
                f"Derived exact datapoints: raw={history_raw_count}, "
                f"target={history_util_count}, features={history_feature_count}"
            )
            log(f"Saved derived history data: {history_raw_path.name}")

            history_util = load_timeseries_csv(
                history_util_path,
                [
                    "metric",
                    "clock",
                    "hostid",
                    "host",
                    "as_value",
                    "env_value",
                    "env_group",
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
                    "env_value",
                    "env_group",
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

            selection_rows = []
            for item in direct_items:
                selection_rows.append(
                    {
                        "hostid": item.hostid,
                        "host": item.host,
                        "as_value": item.as_value,
                        "env_value": host_meta.get(item.hostid, {}).get("env_value", ""),
                        "env_group": host_meta.get(item.hostid, {}).get("env_group", "non-prod"),
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
                        "env_value": host_meta.get(item.hostid, {}).get("env_value", ""),
                        "env_group": host_meta.get(item.hostid, {}).get("env_group", "non-prod"),
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
            selected_counts = {
                "cpu_target": cpu_target_count,
                "ram_target": ram_target_count,
                "disk_target": disk_target_count,
                "ram_features": ram_feature_count,
                "disk_features": disk_feature_count,
                "feature_total": feature_total_count,
            }

        if "env_value" not in selection_report.columns:
            selection_report["env_value"] = ""
        if "env_group" not in selection_report.columns:
            selection_report["env_group"] = selection_report["env_value"].map(env_group_from_value)
        selection_report["env_group"] = selection_report["env_group"].fillna("").map(env_group_from_value)

        host_scope = pd.DataFrame(columns=["hostid", "as_value", "env_value", "env_group"])
        if not selection_report.empty and "hostid" in selection_report.columns:
            scoped_columns = [column for column in host_scope.columns if column in selection_report.columns]
            if scoped_columns:
                host_scope = selection_report[scoped_columns].dropna(subset=["hostid"]).drop_duplicates(subset=["hostid"])
                if "env_value" not in host_scope.columns:
                    host_scope["env_value"] = ""
                if "env_group" not in host_scope.columns:
                    host_scope["env_group"] = "non-prod"
                if "as_value" not in host_scope.columns:
                    host_scope["as_value"] = ""
        history_util = ensure_scope_columns(history_util, host_scope)
        trend_util = ensure_scope_columns(trend_util, host_scope)

        history_summary_all = summarize_history(history_util, by_as=False)
        history_summary_as = summarize_history(history_util, by_as=True)
        trend_summary_all = summarize_trend(trend_util, by_as=False)
        trend_summary_as = summarize_trend(trend_util, by_as=True)
        daily_target = build_daily_p95_target(history_util)
        risk_metrics = compute_host_risk_metrics(history_util)

        forecast_df = pd.DataFrame(
            columns=[
                "metric",
                "hostid",
                "host",
                "as_value",
                "env_value",
                "env_group",
                "date",
                "horizon_day",
                "model",
                "p50",
                "p90",
                "p95",
            ]
        )
        backtest_df = pd.DataFrame(
            columns=[
                "metric",
                "hostid",
                "host",
                "as_value",
                "env_value",
                "env_group",
                "model",
                "wape",
                "mae",
                "pinball_p90",
                "calibration_p90",
                "folds",
            ]
        )
        model_selection = pd.DataFrame(
            columns=[
                "metric",
                "hostid",
                "host",
                "as_value",
                "env_value",
                "env_group",
                "selected_model",
                "selection_score",
                "wape",
                "mae",
                "pinball_p90",
                "calibration_p90",
                "series_days",
            ]
        )
        actionable_df = pd.DataFrame(
            columns=[
                "metric",
                "hostid",
                "host",
                "as_value",
                "env_value",
                "env_group",
                "cluster",
                "overprovisioned",
                "p50",
                "p95",
                "p99",
                "duty_cycle_80",
                "duty_cycle_90",
                "burstiness",
                "volatility",
                "days_to_80_p50",
                "days_to_90_p50",
                "days_to_95_p50",
                "days_to_80_p90",
                "days_to_90_p90",
                "days_to_95_p90",
                "days_to_80_p95",
                "days_to_90_p95",
                "days_to_95_p95",
                "risk_basis",
                "days_to_90_basis",
                "crossing_date_90_basis",
                "status",
                "recommendation",
            ]
        )
        if forecast_enabled:
            log(
                "Building host-level forecasts "
                f"(daily p95 target, horizons={forecast_horizons})..."
            )
            forecast_df, backtest_df, model_selection = run_host_metric_forecasts(
                daily_target=daily_target,
                horizons=forecast_horizons,
                backtest_horizon_days=forecast_backtest_horizon_days,
                backtest_folds=forecast_backtest_folds,
                min_train_days=forecast_min_train_days,
            )
            actionable_df = build_actionable_recommendations(
                risk_metrics=risk_metrics,
                forecast_df=forecast_df,
            )
        else:
            log("Skipping forecasting stage (FORECAST_ENABLED=False).")

        risk_probabilities = compute_horizon_risk_probabilities(
            forecast_df=forecast_df,
            model_selection=model_selection,
            horizons=forecast_horizons,
            threshold=90.0,
        )
        if not risk_probabilities.empty and not actionable_df.empty:
            merge_keys = ["metric", "hostid", "host", "as_value"]
            merge_keys.extend([column for column in ("env_value", "env_group") if column in actionable_df.columns])
            confidence_frame = (
                risk_probabilities.groupby(merge_keys, as_index=False)["confidence_index_pct"]
                .max()
            )
            actionable_df = actionable_df.merge(confidence_frame, on=merge_keys, how="left")
            probability_wide = (
                risk_probabilities.pivot_table(
                    index=merge_keys,
                    columns="horizon_days",
                    values="prob_cross_adjusted_pct",
                    aggfunc="max",
                )
                .reset_index()
            )
            renamed_columns = []
            for column in probability_wide.columns:
                column_name = str(column)
                if column_name in merge_keys:
                    renamed_columns.append(column_name)
                    continue
                try:
                    horizon_value = int(column)
                except (TypeError, ValueError):
                    renamed_columns.append(column_name)
                    continue
                renamed_columns.append(f"prob_cross_90_{horizon_value}d_pct")
            probability_wide.columns = renamed_columns
            actionable_df = actionable_df.merge(probability_wide, on=merge_keys, how="left")

        log("Saving CSV artifacts...")
        save_csv(history_util, history_util_path)
        save_csv(trend_util, trend_util_path)
        save_csv(selection_report, selection_path)
        save_csv(history_summary_all, csv_dir / f"history_summary_all_{history_window_label}.csv")
        save_csv(history_summary_as, csv_dir / f"history_summary_by_as_{history_window_label}.csv")
        save_csv(trend_summary_all, csv_dir / f"trend_summary_all_{trend_window_label}.csv")
        save_csv(trend_summary_as, csv_dir / f"trend_summary_by_as_{trend_window_label}.csv")
        save_csv(daily_target, csv_dir / f"daily_target_p95_{history_window_label}.csv")
        save_csv(risk_metrics, csv_dir / f"host_risk_metrics_{history_window_label}.csv")
        save_csv(backtest_df, csv_dir / "model_backtest.csv")
        save_csv(model_selection, csv_dir / "model_selection.csv")
        save_csv(forecast_df, csv_dir / "forecast_daily.csv")
        save_csv(risk_probabilities, csv_dir / "risk_probabilities.csv")
        save_csv(actionable_df, csv_dir / "actionable_recommendations.csv")

        conclusion = build_conclusion(
            run_at=now,
            matched_hosts=host_count,
            selected_counts=selected_counts,
            history_summary_all=history_summary_all,
            trend_summary_all=trend_summary_all,
            risk_metrics=risk_metrics,
            actionable_df=actionable_df,
            model_selection=model_selection,
        )

        xlsx_path = xlsx_dir / f"summary_report_{history_window_label}_{trend_window_label}.xlsx"
        save_xlsx(
            xlsx_path,
            {
                "selected_items": selection_report,
                "history_summary_all": history_summary_all,
                "history_summary_by_as": history_summary_as,
                "trend_summary_all": trend_summary_all,
                "trend_summary_by_as": trend_summary_as,
                "daily_target_p95": daily_target,
                "host_risk_metrics": risk_metrics,
                "model_backtest": backtest_df,
                "model_selection": model_selection,
                "forecast_daily": forecast_df,
                "risk_probabilities": risk_probabilities,
                "actionable": actionable_df,
                "conclusion": conclusion,
            },
        )
        log(f"Saved XLSX summary report: {xlsx_path.name}")

        context = {
            "run_at_utc": now.isoformat(),
            "run_mode": "analysis_only" if analysis_only else "full",
            "api_url": api.url if api is not None else cfg.ZABBIX_URL,
            "api_version": version,
            "as_tag_key": cfg.AS_TAG_KEY,
            "as_tag_values": as_values,
            "env_tag_key": cfg.ENV_TAG_KEY,
            "history_days": cfg.HISTORY_DAYS,
            "history_mode": "all_available" if history_all_available else "fixed_days",
            "history_window_label": history_window_label,
            "history_source": "trend.value_avg",
            "trend_days": cfg.TREND_DAYS,
            "trend_window_label": trend_window_label,
            "host_count": host_count,
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
            "csv_dir": str(csv_dir),
            "xlsx_dir": str(xlsx_dir),
            "forecast_enabled": forecast_enabled,
            "forecast_horizons": forecast_horizons,
            "forecast_backtest_horizon_days": forecast_backtest_horizon_days,
            "forecast_backtest_folds": forecast_backtest_folds,
            "forecast_min_train_days": forecast_min_train_days,
            "forecast_max_plots": forecast_max_plots,
            "forecast_rows": int(len(forecast_df)),
            "risk_probability_rows": int(len(risk_probabilities)),
            "risk_rows": int(len(risk_metrics)),
            "actionable_rows": int(len(actionable_df)),
        }
        with (output_dir / "run_context.json").open("w", encoding="utf-8") as file:
            json.dump(context, file, indent=2, ensure_ascii=False)

        scope_sources = []
        for frame in (history_util, trend_util, actionable_df, selection_report):
            if frame.empty or "as_value" not in frame.columns:
                continue
            scope_frame = frame[["as_value"]].copy()
            if "env_group" in frame.columns:
                scope_frame["env_group"] = frame["env_group"].fillna("").map(env_group_from_value).astype("string")
            else:
                scope_frame["env_group"] = "non-prod"
            scope_sources.append(scope_frame)
        if scope_sources:
            all_scopes = pd.concat(scope_sources, ignore_index=True)
            all_scopes["as_value"] = all_scopes["as_value"].fillna("").astype("string")
            all_scopes["env_group"] = all_scopes["env_group"].fillna("").map(env_group_from_value).astype("string")
            scope_pairs = (
                all_scopes[["as_value", "env_group"]]
                .drop_duplicates()
                .sort_values(["as_value", "env_group"])
                .to_records(index=False)
            )
        else:
            scope_pairs = []

        for as_value_raw, env_group_raw in scope_pairs:
            scope_as = str(as_value_raw)
            scope_env = env_group_from_value(str(env_group_raw))
            scope_root = output_dir / safe_slug(scope_as or "_empty_as") / scope_env
            scope_csv_dir = scope_root / csv_subdir
            scope_xlsx_dir = scope_root / xlsx_subdir
            scope_csv_dir.mkdir(parents=True, exist_ok=True)
            scope_xlsx_dir.mkdir(parents=True, exist_ok=True)

            def filter_scope(frame: pd.DataFrame) -> pd.DataFrame:
                if frame.empty:
                    return frame.copy()
                scoped = frame.copy()
                if "as_value" in scoped.columns:
                    scoped = scoped[scoped["as_value"].fillna("").astype(str) == scope_as]
                if "env_group" in scoped.columns:
                    scoped = scoped[scoped["env_group"].fillna("").astype(str).map(env_group_from_value) == scope_env]
                elif scope_env == "prod":
                    return scoped.iloc[0:0].copy()
                return scoped.copy()

            scope_history = filter_scope(history_util)
            scope_trend = filter_scope(trend_util)
            scope_selection = filter_scope(selection_report)
            scope_daily_target = filter_scope(daily_target)
            scope_risk_metrics = filter_scope(risk_metrics)
            scope_backtest = filter_scope(backtest_df)
            scope_model_selection = filter_scope(model_selection)
            scope_forecast = filter_scope(forecast_df)
            scope_risk_probabilities = filter_scope(risk_probabilities)
            scope_actionable = filter_scope(actionable_df)

            scope_history_summary_all = summarize_history(scope_history, by_as=False)
            scope_history_summary_as = summarize_history(scope_history, by_as=True)
            scope_trend_summary_all = summarize_trend(scope_trend, by_as=False)
            scope_trend_summary_as = summarize_trend(scope_trend, by_as=True)
            scope_selected_counts = build_selected_counts_from_report(scope_selection)
            scope_host_count = int(
                pd.concat(
                    [
                        scope_history["hostid"] if "hostid" in scope_history.columns else pd.Series(dtype="string"),
                        scope_trend["hostid"] if "hostid" in scope_trend.columns else pd.Series(dtype="string"),
                    ],
                    ignore_index=True,
                ).dropna().astype("string").nunique()
            )
            scope_conclusion = build_conclusion(
                run_at=now,
                matched_hosts=scope_host_count,
                selected_counts=scope_selected_counts,
                history_summary_all=scope_history_summary_all,
                trend_summary_all=scope_trend_summary_all,
                risk_metrics=scope_risk_metrics,
                actionable_df=scope_actionable,
                model_selection=scope_model_selection,
            )

            save_csv(scope_selection, scope_csv_dir / "selected_items.csv")
            save_csv(scope_history, scope_csv_dir / f"history_exact_{history_window_label}.csv")
            save_csv(scope_trend, scope_csv_dir / f"trend_{trend_window_label}.csv")
            save_csv(scope_history_summary_all, scope_csv_dir / f"history_summary_all_{history_window_label}.csv")
            save_csv(scope_history_summary_as, scope_csv_dir / f"history_summary_by_as_{history_window_label}.csv")
            save_csv(scope_trend_summary_all, scope_csv_dir / f"trend_summary_all_{trend_window_label}.csv")
            save_csv(scope_trend_summary_as, scope_csv_dir / f"trend_summary_by_as_{trend_window_label}.csv")
            save_csv(scope_daily_target, scope_csv_dir / f"daily_target_p95_{history_window_label}.csv")
            save_csv(scope_risk_metrics, scope_csv_dir / f"host_risk_metrics_{history_window_label}.csv")
            save_csv(scope_backtest, scope_csv_dir / "model_backtest.csv")
            save_csv(scope_model_selection, scope_csv_dir / "model_selection.csv")
            save_csv(scope_forecast, scope_csv_dir / "forecast_daily.csv")
            save_csv(scope_risk_probabilities, scope_csv_dir / "risk_probabilities.csv")
            save_csv(scope_actionable, scope_csv_dir / "actionable_recommendations.csv")

            scope_xlsx_path = scope_xlsx_dir / f"summary_report_{history_window_label}_{trend_window_label}.xlsx"
            save_xlsx(
                scope_xlsx_path,
                {
                    "selected_items": scope_selection,
                    "history_summary_all": scope_history_summary_all,
                    "history_summary_by_as": scope_history_summary_as,
                    "trend_summary_all": scope_trend_summary_all,
                    "trend_summary_by_as": scope_trend_summary_as,
                    "daily_target_p95": scope_daily_target,
                    "host_risk_metrics": scope_risk_metrics,
                    "model_backtest": scope_backtest,
                    "model_selection": scope_model_selection,
                    "forecast_daily": scope_forecast,
                    "risk_probabilities": scope_risk_probabilities,
                    "actionable": scope_actionable,
                    "conclusion": scope_conclusion,
                },
            )

            write_scope_text_conclusion(
                path=scope_root / "conclusion.txt",
                as_value=scope_as,
                env_group=scope_env,
                actionable=scope_actionable,
                risk_probabilities=scope_risk_probabilities,
                run_at=now,
            )

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

            if (
                forecast_enabled
                and forecast_max_plots > 0
                and not daily_target.empty
                and not forecast_df.empty
                and not actionable_df.empty
            ):
                forecast_plots_dir = plots_dir / "forecasts"
                forecast_plots_dir.mkdir(parents=True, exist_ok=True)
                model_lookup = {
                    (str(row["metric"]), str(row["hostid"])): str(row["selected_model"])
                    for _, row in model_selection.iterrows()
                }
                status_priority = {
                    "critical": 0,
                    "watch": 1,
                    "stable": 2,
                    "overprovisioned": 3,
                }
                ranked = actionable_df.copy()
                ranked["status_priority"] = ranked["status"].map(status_priority).fillna(4).astype(int)
                ranked["days_to_90_basis"] = pd.to_numeric(
                    ranked["days_to_90_basis"], errors="coerce"
                )
                ranked["days_sort"] = ranked["days_to_90_basis"].fillna(10**9).astype(float)
                ranked = ranked.sort_values(
                    ["status_priority", "days_sort", "metric", "host"]
                )
                top_rows = ranked.head(forecast_max_plots)
                log(
                    f"Building forecast plots for top {len(top_rows)} host/metric pairs..."
                )
                for _, row in top_rows.iterrows():
                    metric = str(row["metric"])
                    hostid = str(row["hostid"])
                    host = str(row["host"])
                    selected_model = model_lookup.get((metric, hostid), "")
                    probability_column = f"prob_cross_90_{int(max(forecast_horizons))}d_pct"
                    host_daily = daily_target[
                        (daily_target["metric"] == metric)
                        & (daily_target["hostid"] == hostid)
                    ].copy()
                    host_forecast = forecast_df[
                        (forecast_df["metric"] == metric)
                        & (forecast_df["hostid"] == hostid)
                    ].copy()
                    if host_daily.empty or host_forecast.empty:
                        continue
                    plot_host_forecast(
                        metric=metric,
                        host=host,
                        history_daily=host_daily,
                        forecast_daily=host_forecast,
                        status=str(row.get("status", "")),
                        cluster=str(row.get("cluster", "")),
                        recommendation=str(row.get("recommendation", "")),
                        selected_model=selected_model,
                        risk_basis=str(row.get("risk_basis", "")),
                        days_to_90_basis=row.get("days_to_90_basis"),
                        horizon_days=int(host_forecast["horizon_day"].max()) if "horizon_day" in host_forecast.columns else None,
                        scenario_probability=row.get(probability_column),
                        confidence_index=row.get("confidence_index_pct"),
                        output_file=forecast_plots_dir
                        / f"{metric}_{safe_slug(host)}_{safe_slug(hostid)}.png",
                    )
            elif forecast_enabled and forecast_max_plots > 0:
                log("Skipping forecast plots: no forecast/actionable rows available.")
        else:
            log("Skipping plot generation (plotting disabled in config).")

        log(f"Done. Outputs are in: {output_dir.resolve()}")
        return 0
    finally:
        if api is not None:
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
