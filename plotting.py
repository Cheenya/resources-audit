from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_metric_dashboard(
    metric: str,
    history_raw: pd.DataFrame,
    history_summary: pd.DataFrame,
    trend_summary: pd.DataFrame,
    forecast: pd.DataFrame,
    history_days: int,
    trend_days: int,
    output_file: Path,
) -> None:
    fig, axes = plt.subplots(4, 1, figsize=(16, 20), constrained_layout=True)

    ax = axes[0]
    if not history_summary.empty:
        summary = history_summary.sort_values("clock")
        ax.fill_between(
            summary["clock"], summary["util_p10"], summary["util_p90"], alpha=0.2, label="P10-P90"
        )
        ax.plot(summary["clock"], summary["util_mean"], label="Mean", linewidth=1.7)
        ax.plot(summary["clock"], summary["util_median"], label="Median", linewidth=1.2, alpha=0.9)
        ax.plot(summary["clock"], summary["util_max"], label="Max", linewidth=1.0, alpha=0.6)
        ax.set_title(f"{metric.upper()} exact data (last {history_days} days)")
        ax.set_ylabel("Utilization %")
        ax.legend(loc="upper left")
    else:
        ax.text(
            0.5,
            0.5,
            f"No {history_days}-day exact data",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )

    ax = axes[1]
    if not trend_summary.empty:
        summary = trend_summary.sort_values("clock")
        ax.fill_between(
            summary["clock"], summary["util_min"], summary["util_max"], alpha=0.2, label="Min-Max envelope"
        )
        ax.plot(summary["clock"], summary["util_avg_mean"], label="Trend mean", linewidth=1.8)
        ax.plot(summary["clock"], summary["util_avg_p90"], label="Trend p90", linewidth=1.0, alpha=0.8)
        ax.set_title(f"{metric.upper()} trend data (last {trend_days} days)")
        ax.set_ylabel("Utilization %")
        ax.legend(loc="upper left")
    else:
        ax.text(
            0.5,
            0.5,
            f"No {trend_days}-day trend data",
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
            ax.set_title(f"{metric.upper()} host heatmap (daily mean, first {pivot.shape[0]} hosts)")
            ax.set_ylabel("Host")
            ax.set_xlabel("Date")

            y_ticks = np.arange(pivot.shape[0])
            ax.set_yticks(y_ticks)
            ax.set_yticklabels(pivot.index.to_list(), fontsize=7)

            tick_count = min(10, pivot.shape[1])
            x_ticks = np.linspace(0, pivot.shape[1] - 1, tick_count, dtype=int)
            ax.set_xticks(x_ticks)
            labels = [pivot.columns[idx].strftime("%Y-%m-%d") for idx in x_ticks]
            ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=8)
            fig.colorbar(image, ax=ax, pad=0.01, label="Utilization %")
        else:
            ax.text(0.5, 0.5, "Not enough data for heatmap", ha="center", va="center", transform=ax.transAxes)
    else:
        ax.text(0.5, 0.5, "No exact data for heatmap", ha="center", va="center", transform=ax.transAxes)

    ax = axes[3]
    if not forecast.empty:
        history_part = forecast[forecast["is_future"] == False].sort_values("timestamp")
        future_part = forecast[forecast["is_future"] == True].sort_values("timestamp")

        if not history_part.empty:
            ax.plot(history_part["timestamp"], history_part["actual"], label="Historical daily mean", linewidth=1.2)
            ax.plot(
                history_part["timestamp"],
                history_part["fitted"],
                label="Model fit",
                linestyle="--",
                linewidth=1.1,
                alpha=0.9,
            )
        if not future_part.empty:
            ax.plot(future_part["timestamp"], future_part["predicted"], label="Forecast", linewidth=1.8)
            ax.fill_between(
                future_part["timestamp"],
                future_part["lower"],
                future_part["upper"],
                alpha=0.2,
                label="95% interval",
            )
        ax.axhline(80, color="orange", linewidth=1.0, linestyle=":")
        ax.axhline(90, color="red", linewidth=1.0, linestyle=":")
        ax.set_title(f"{metric.upper()} utilization forecast")
        ax.set_ylabel("Utilization %")
        ax.set_ylim(0, 100)
        ax.legend(loc="upper left")
    else:
        ax.text(0.5, 0.5, "No forecast data", ha="center", va="center", transform=ax.transAxes)

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
        ax.text(0.5, 0.5, "No AS breakdown data", ha="center", va="center", transform=ax.transAxes)
    else:
        for as_value, frame in metric_as.groupby("as_value"):
            frame = frame.sort_values("clock")
            ax.plot(frame["clock"], frame["util_mean"], label=as_value if as_value else "<empty>")
        ax.set_title(f"{metric.upper()} mean utilization by AS tag")
        ax.set_ylabel("Utilization %")
        ax.legend(loc="upper left", ncol=3, fontsize=8)
    ax.grid(alpha=0.3)
    ax.tick_params(axis="x", rotation=25)
    fig.savefig(output_file, dpi=140)
    plt.close(fig)
