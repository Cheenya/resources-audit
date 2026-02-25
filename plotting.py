from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


METRIC_LABELS_RU = {
    "cpu": "CPU",
    "ram": "RAM",
    "disk": "Диск",
}
STATUS_LABELS_RU = {
    "critical": "критично",
    "watch": "под наблюдением",
    "stable": "стабильно",
    "overprovisioned": "избыточные ресурсы",
}
CLUSTER_LABELS_RU = {
    "cold": "cold (низкая загрузка)",
    "warm": "warm (рабочая зона)",
    "hot": "hot (повышенный риск)",
}
MODEL_LABELS_RU = {
    "seasonal_naive": "seasonal_naive",
    "robust_trend": "robust_trend",
    "gbdt_lag": "gbdt_lag",
}
RISK_BASIS_LABELS_RU = {
    "p50": "по p50",
    "hot_p90_p95": "по p90/p95 для hot",
}


def _label_metric(metric: str) -> str:
    return METRIC_LABELS_RU.get(metric.lower(), metric.upper())


def _label_status(status: str) -> str:
    return STATUS_LABELS_RU.get(status.lower(), status)


def _label_cluster(cluster: str) -> str:
    return CLUSTER_LABELS_RU.get(cluster.lower(), cluster)


def _label_model(model: str) -> str:
    return MODEL_LABELS_RU.get(model, model)


def _label_risk_basis(value: str) -> str:
    return RISK_BASIS_LABELS_RU.get(value, value)


def _format_days(days_to_threshold: object) -> str:
    if days_to_threshold is None:
        return "не ожидается на горизонте"
    try:
        value = float(days_to_threshold)
    except (TypeError, ValueError):
        return "не ожидается на горизонте"
    if np.isnan(value):
        return "не ожидается на горизонте"
    return f"{int(value)} дн."


def _format_percent(value: object) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if np.isnan(num):
        return "n/a"
    return f"{num:.1f}%"


def plot_metric_dashboard(
    metric: str,
    history_raw: pd.DataFrame,
    history_summary: pd.DataFrame,
    trend_summary: pd.DataFrame,
    history_window_label: str,
    trend_days: int,
    output_file: Path,
) -> None:
    fig, axes = plt.subplots(3, 1, figsize=(16, 16), constrained_layout=True)

    ax = axes[0]
    metric_label = _label_metric(metric)
    if not history_summary.empty:
        summary = history_summary.sort_values("clock")
        ax.fill_between(
            summary["clock"], summary["util_p10"], summary["util_p90"], alpha=0.2, label="P10-P90"
        )
        ax.plot(summary["clock"], summary["util_mean"], label="Среднее", linewidth=1.7)
        ax.plot(summary["clock"], summary["util_median"], label="Медиана", linewidth=1.2, alpha=0.9)
        ax.plot(summary["clock"], summary["util_max"], label="Максимум", linewidth=1.0, alpha=0.6)
        ax.set_title(f"{metric_label}: точные данные ({history_window_label})")
        ax.set_ylabel("Утилизация, %")
        ax.legend(loc="upper left")
    else:
        ax.text(
            0.5,
            0.5,
            f"Нет точных данных ({history_window_label})",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )

    ax = axes[1]
    if not trend_summary.empty:
        summary = trend_summary.sort_values("clock")
        ax.fill_between(
            summary["clock"], summary["util_min"], summary["util_max"], alpha=0.2, label="Диапазон min-max"
        )
        ax.plot(summary["clock"], summary["util_avg_mean"], label="Среднее по трендам", linewidth=1.8)
        ax.plot(summary["clock"], summary["util_avg_p90"], label="P90 по трендам", linewidth=1.0, alpha=0.8)
        ax.set_title(f"{metric_label}: тренды за последние {trend_days} дней")
        ax.set_ylabel("Утилизация, %")
        ax.legend(loc="upper left")
    else:
        ax.text(
            0.5,
            0.5,
            f"Нет трендов за {trend_days} дней",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )

    ax = axes[2]
    if not history_raw.empty:
        heat = history_raw.copy()
        heat["date"] = heat["clock"].dt.floor("D")
        pivot = heat.pivot_table(index="host", columns="date", values="utilization_pct", aggfunc="mean")
        pivot = pivot.sort_index()
        if pivot.shape[0] > 40:
            pivot = pivot.iloc[:40]
        if pivot.shape[1] > 0 and pivot.shape[0] > 0:
            image = ax.imshow(pivot.to_numpy(), aspect="auto", vmin=0.0, vmax=100.0, cmap="viridis")
            ax.set_title(f"{metric_label}: тепловая карта хостов (среднее за день, первые {pivot.shape[0]})")
            ax.set_ylabel("Хост")
            ax.set_xlabel("Дата")

            y_ticks = np.arange(pivot.shape[0])
            ax.set_yticks(y_ticks)
            ax.set_yticklabels(pivot.index.to_list(), fontsize=7)

            tick_count = min(10, pivot.shape[1])
            x_ticks = np.linspace(0, pivot.shape[1] - 1, tick_count, dtype=int)
            ax.set_xticks(x_ticks)
            labels = [pivot.columns[idx].strftime("%Y-%m-%d") for idx in x_ticks]
            ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=8)
            fig.colorbar(image, ax=ax, pad=0.01, label="Утилизация, %")
        else:
            ax.text(0.5, 0.5, "Недостаточно данных для heatmap", ha="center", va="center", transform=ax.transAxes)
    else:
        ax.text(0.5, 0.5, "Нет точных данных для heatmap", ha="center", va="center", transform=ax.transAxes)

    for axis in axes:
        axis.grid(alpha=0.25)
        axis.tick_params(axis="x", rotation=25)

    fig.savefig(output_file, dpi=140)
    plt.close(fig)


def plot_as_breakdown(
    metric: str,
    history_by_as: pd.DataFrame,
    output_file: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 6), constrained_layout=True)
    metric_as = history_by_as[history_by_as["metric"] == metric].copy()
    if metric_as.empty:
        ax.text(0.5, 0.5, "Нет данных по разрезу AS", ha="center", va="center", transform=ax.transAxes)
    else:
        for as_value, frame in metric_as.groupby("as_value"):
            frame = frame.sort_values("clock")
            ax.plot(frame["clock"], frame["util_mean"], label=as_value if as_value else "<пусто>")
        ax.set_title(f"{_label_metric(metric)}: средняя утилизация по тегу AS")
        ax.set_ylabel("Утилизация, %")
        ax.set_xlabel("Дата")
        ax.legend(loc="upper left", ncol=3, fontsize=8)
    ax.grid(alpha=0.3)
    ax.tick_params(axis="x", rotation=25)
    fig.savefig(output_file, dpi=140)
    plt.close(fig)


def plot_host_forecast(
    metric: str,
    host: str,
    history_daily: pd.DataFrame,
    forecast_daily: pd.DataFrame,
    output_file: Path,
    *,
    status: str = "",
    cluster: str = "",
    recommendation: str = "",
    selected_model: str = "",
    risk_basis: str = "",
    days_to_90_basis: object = None,
    horizon_days: Optional[int] = None,
    scenario_probability: object = None,
    confidence_index: object = None,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 6), constrained_layout=True)
    metric_label = _label_metric(metric)

    if not history_daily.empty:
        history_plot = history_daily.sort_values("date")
        ax.plot(
            history_plot["date"],
            history_plot["target_p95"],
            label="История (daily p95)",
            linewidth=1.5,
            alpha=0.9,
        )

    if not forecast_daily.empty:
        forecast_plot = forecast_daily.sort_values("date")
        ax.plot(forecast_plot["date"], forecast_plot["p50"], label="Прогноз p50", linewidth=1.8)
        ax.plot(forecast_plot["date"], forecast_plot["p90"], label="Прогноз p90", linewidth=1.2)
        ax.plot(forecast_plot["date"], forecast_plot["p95"], label="Прогноз p95", linewidth=1.0, alpha=0.9)

    ax.axhline(80.0, color="#d6a800", linestyle="--", linewidth=1.0, label="Порог 80%")
    ax.axhline(90.0, color="#d45f00", linestyle="--", linewidth=1.0, label="Порог 90%")
    ax.axhline(95.0, color="#b30000", linestyle="--", linewidth=1.0, label="Порог 95%")

    ax.set_title(f"{metric_label}: прогноз утилизации и риска — {host}")
    ax.set_ylabel("Утилизация, %")
    ax.set_xlabel("Дата")
    ax.set_ylim(0.0, 100.0)
    ax.grid(alpha=0.3)
    ax.tick_params(axis="x", rotation=25)
    ax.legend(loc="upper left", ncol=3, fontsize=8)

    explain_lines = []
    if selected_model:
        explain_lines.append(f"Модель: {_label_model(selected_model)}")
    if cluster:
        explain_lines.append(f"Кластер: {_label_cluster(cluster)}")
    if status:
        explain_lines.append(f"Статус: {_label_status(status)}")
    if risk_basis:
        explain_lines.append(f"Оценка риска: {_label_risk_basis(risk_basis)}")
    explain_lines.append(f"До пересечения 90%: {_format_days(days_to_90_basis)}")
    if horizon_days is not None:
        explain_lines.append(f"Горизонт прогноза: {int(horizon_days)} дн.")
    if recommendation:
        explain_lines.append(f"Рекомендация: {recommendation}")
    if scenario_probability is not None:
        explain_lines.append(f"Вероятность сценария: {_format_percent(scenario_probability)}")
    if confidence_index is not None:
        explain_lines.append(f"Индекс доверия: {_format_percent(confidence_index)}")
    explain_text = "\n".join(explain_lines)
    ax.text(
        0.99,
        0.01,
        explain_text,
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "alpha": 0.85, "edgecolor": "#b8b8b8"},
    )

    fig.savefig(output_file, dpi=140)
    plt.close(fig)
