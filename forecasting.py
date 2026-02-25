from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


LAGS: Tuple[int, ...] = (1, 2, 3, 7, 14, 21, 28)
ROLL_WINDOWS: Tuple[int, ...] = (3, 7, 14)
EPS = 1e-9
Z90 = 1.2815515655446004


def progress_bar(prefix: str, current: int, total: int) -> None:
    if total <= 0:
        return
    width = 28
    ratio = min(max(current / total, 0.0), 1.0)
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    print(
        f"\r{prefix}: [{bar}] {current}/{total}",
        end="" if current < total else "\n",
        file=sys.stderr,
        flush=True,
    )


def _safe_wape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = float(np.sum(np.abs(y_true)))
    if denom <= EPS:
        return float(np.mean(np.abs(y_true - y_pred)))
    return float(np.sum(np.abs(y_true - y_pred)) / denom)


def _pinball_loss(y_true: np.ndarray, y_q: np.ndarray, q: float) -> float:
    delta = y_true - y_q
    loss = np.where(delta >= 0.0, q * delta, (1.0 - q) * (-delta))
    return float(np.mean(loss))


def _first_crossing_days(
    frame: pd.DataFrame, value_column: str, threshold: float
) -> Tuple[Optional[int], Optional[pd.Timestamp]]:
    if frame.empty or value_column not in frame.columns:
        return None, None
    first_date = frame["date"].iloc[0]
    hit = frame[frame[value_column] >= threshold]
    if hit.empty:
        return None, None
    crossing_date = hit["date"].iloc[0]
    days = int((crossing_date - first_date).days + 1)
    return days, crossing_date


def _quantile(values: Iterable[float], q: float) -> float:
    arr = np.asarray(list(values), dtype=float)
    if arr.size == 0:
        return float("nan")
    return float(np.quantile(arr, q))


def build_daily_p95_target(history_util: pd.DataFrame) -> pd.DataFrame:
    base_columns = ["metric", "hostid", "host", "as_value"]
    optional_columns = [column for column in ("env_value", "env_group") if column in history_util.columns]
    output_columns = [*base_columns, *optional_columns, "date", "target_p95"]
    if history_util.empty:
        return pd.DataFrame(columns=output_columns)

    frame = history_util.copy()
    frame["date"] = pd.to_datetime(frame["clock"], utc=True, errors="coerce").dt.floor("D")
    frame = frame.dropna(subset=["date", "utilization_pct"])
    if frame.empty:
        return pd.DataFrame(columns=output_columns)

    for column in optional_columns:
        if column not in frame.columns:
            frame[column] = ""

    grouped = (
        frame.groupby([*base_columns, *optional_columns, "date"])["utilization_pct"]
        .quantile(0.95)
        .rename("target_p95")
        .reset_index()
    )
    grouped["target_p95"] = grouped["target_p95"].clip(lower=0.0, upper=100.0)
    return grouped[output_columns]


def compute_host_risk_metrics(history_util: pd.DataFrame) -> pd.DataFrame:
    base_columns = ["metric", "hostid", "host", "as_value"]
    optional_columns = [column for column in ("env_value", "env_group") if column in history_util.columns]
    output_columns = [
        *base_columns,
        *optional_columns,
        "point_count",
        "util_mean",
        "util_std",
        "p50",
        "p95",
        "p99",
        "duty_cycle_80",
        "duty_cycle_90",
        "burstiness",
        "volatility",
        "cluster",
        "overprovisioned",
    ]
    if history_util.empty:
        return pd.DataFrame(columns=output_columns)

    group_cols = [*base_columns, *optional_columns]
    grouped = history_util.groupby(group_cols)["utilization_pct"]
    summary = grouped.agg(
        point_count="count",
        util_mean="mean",
        util_std="std",
    )
    summary = summary.join(grouped.quantile(0.50).rename("p50"))
    summary = summary.join(grouped.quantile(0.95).rename("p95"))
    summary = summary.join(grouped.quantile(0.99).rename("p99"))
    summary = summary.join((grouped.apply(lambda series: float((series >= 80.0).mean()))).rename("duty_cycle_80"))
    summary = summary.join((grouped.apply(lambda series: float((series >= 90.0).mean()))).rename("duty_cycle_90"))
    summary = summary.reset_index()

    summary["util_std"] = summary["util_std"].fillna(0.0)
    summary["burstiness"] = summary["p95"] - summary["p50"]
    summary["volatility"] = np.where(
        summary["util_mean"] > 0.0,
        summary["util_std"] / summary["util_mean"],
        np.nan,
    )

    def classify(row: pd.Series) -> str:
        if row["p95"] >= 85.0 or row["duty_cycle_90"] >= 0.10:
            return "hot"
        if row["p50"] <= 35.0 and row["volatility"] <= 0.45 and row["duty_cycle_80"] <= 0.05:
            return "cold"
        return "warm"

    summary["cluster"] = summary.apply(classify, axis=1)
    summary["overprovisioned"] = (summary["p95"] <= 25.0) & (summary["duty_cycle_80"] <= 0.01)
    return summary[output_columns]


def _prepare_daily_series(series: pd.Series) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce")
    clean = clean.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return clean
    clean = clean.sort_index()
    daily = clean.asfreq("D")
    daily = daily.interpolate(limit_direction="both").ffill().bfill()
    return daily.clip(lower=0.0, upper=100.0)


def _build_feature_frame(series: pd.Series) -> pd.DataFrame:
    frame = pd.DataFrame({"y": series.astype(float)})
    for lag in LAGS:
        frame[f"lag_{lag}"] = frame["y"].shift(lag)
    for window in ROLL_WINDOWS:
        frame[f"roll_mean_{window}"] = frame["y"].shift(1).rolling(window).mean()
        frame[f"roll_std_{window}"] = frame["y"].shift(1).rolling(window).std()
    day_of_week = frame.index.dayofweek.astype(float)
    frame["dow_sin"] = np.sin((2.0 * np.pi * day_of_week) / 7.0)
    frame["dow_cos"] = np.cos((2.0 * np.pi * day_of_week) / 7.0)
    frame = frame.replace([np.inf, -np.inf], np.nan).dropna()
    return frame


def _build_feature_row(history_values: Sequence[float], date: pd.Timestamp) -> Dict[str, float]:
    values = list(history_values)
    if not values:
        values = [0.0]
    row: Dict[str, float] = {}
    for lag in LAGS:
        idx = max(0, len(values) - lag)
        row[f"lag_{lag}"] = float(values[idx])
    for window in ROLL_WINDOWS:
        window_values = values[-window:] if len(values) >= window else values
        row[f"roll_mean_{window}"] = float(np.mean(window_values))
        row[f"roll_std_{window}"] = float(np.std(window_values))
    day_of_week = float(date.dayofweek)
    row["dow_sin"] = math.sin((2.0 * math.pi * day_of_week) / 7.0)
    row["dow_cos"] = math.cos((2.0 * math.pi * day_of_week) / 7.0)
    return row


def _fit_robust_trend(train_series: pd.Series) -> Tuple[np.ndarray, Dict[int, float], np.ndarray]:
    y = train_series.to_numpy(dtype=float)
    x = np.arange(len(y), dtype=float)
    weights = np.ones_like(y, dtype=float)
    beta = np.array([float(np.mean(y)), 0.0], dtype=float)
    for _ in range(8):
        design = np.column_stack([np.ones_like(x), x])
        weighted_design = design * weights[:, None]
        weighted_target = y * weights
        beta, _, _, _ = np.linalg.lstsq(weighted_design, weighted_target, rcond=None)
        fitted = beta[0] + beta[1] * x
        residuals = y - fitted
        scale = np.median(np.abs(residuals)) + EPS
        weights = 1.0 / np.maximum(1.0, np.abs(residuals) / (1.5 * scale))

    fitted = beta[0] + beta[1] * x
    residuals = y - fitted
    day_of_week = train_series.index.dayofweek
    seasonal: Dict[int, float] = {}
    for dow in range(7):
        dow_mask = day_of_week == dow
        if np.any(dow_mask):
            seasonal[dow] = float(np.median(residuals[dow_mask]))
        else:
            seasonal[dow] = 0.0
    return beta, seasonal, residuals


def _predict_robust_trend(
    beta: np.ndarray,
    seasonal: Dict[int, float],
    start_index: int,
    forecast_dates: pd.DatetimeIndex,
) -> np.ndarray:
    x = np.arange(start_index, start_index + len(forecast_dates), dtype=float)
    trend = beta[0] + beta[1] * x
    seasonal_component = np.array(
        [seasonal.get(int(date.dayofweek), 0.0) for date in forecast_dates], dtype=float
    )
    pred = trend + seasonal_component
    return np.clip(pred, 0.0, 100.0)


@dataclass
class DecisionStump:
    feature_idx: int
    threshold: float
    left_value: float
    right_value: float


class SimpleGradientBoostingStumps:
    def __init__(
        self,
        n_estimators: int = 80,
        learning_rate: float = 0.08,
        n_thresholds: int = 15,
        min_samples_leaf: int = 8,
    ) -> None:
        self.n_estimators = int(n_estimators)
        self.learning_rate = float(learning_rate)
        self.n_thresholds = int(n_thresholds)
        self.min_samples_leaf = int(min_samples_leaf)
        self.initial_value: float = 0.0
        self.stumps: List[DecisionStump] = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        if X.size == 0 or y.size == 0:
            self.initial_value = 0.0
            self.stumps = []
            return
        self.initial_value = float(np.mean(y))
        pred = np.full_like(y, self.initial_value, dtype=float)
        self.stumps = []

        _, n_features = X.shape
        for _ in range(self.n_estimators):
            residual = y - pred
            best_loss = float("inf")
            best_stump: Optional[DecisionStump] = None
            best_update: Optional[np.ndarray] = None

            for feature_idx in range(n_features):
                feature_values = X[:, feature_idx]
                quantiles = np.linspace(0.05, 0.95, self.n_thresholds)
                thresholds = np.unique(np.quantile(feature_values, quantiles))
                for threshold in thresholds:
                    left_mask = feature_values <= threshold
                    right_mask = ~left_mask
                    left_count = int(np.sum(left_mask))
                    right_count = int(np.sum(right_mask))
                    if left_count < self.min_samples_leaf or right_count < self.min_samples_leaf:
                        continue
                    left_value = float(np.mean(residual[left_mask]))
                    right_value = float(np.mean(residual[right_mask]))
                    update = np.where(left_mask, left_value, right_value)
                    loss = float(np.mean((residual - update) ** 2))
                    if loss < best_loss:
                        best_loss = loss
                        best_stump = DecisionStump(
                            feature_idx=feature_idx,
                            threshold=float(threshold),
                            left_value=left_value,
                            right_value=right_value,
                        )
                        best_update = update

            if best_stump is None or best_update is None:
                break
            pred = pred + self.learning_rate * best_update
            self.stumps.append(best_stump)

    def predict(self, X: np.ndarray) -> np.ndarray:
        if X.size == 0:
            return np.array([], dtype=float)
        pred = np.full(X.shape[0], self.initial_value, dtype=float)
        for stump in self.stumps:
            feature_values = X[:, stump.feature_idx]
            update = np.where(
                feature_values <= stump.threshold,
                stump.left_value,
                stump.right_value,
            )
            pred += self.learning_rate * update
        return np.clip(pred, 0.0, 100.0)


@dataclass
class ModelForecast:
    model: str
    p50: np.ndarray
    p90: np.ndarray
    p95: np.ndarray


def _fit_predict_seasonal_naive(train_series: pd.Series, forecast_dates: pd.DatetimeIndex) -> ModelForecast:
    y = train_series.to_numpy(dtype=float)
    n_steps = len(forecast_dates)
    if y.size == 0:
        base = np.zeros(n_steps, dtype=float)
        return ModelForecast("seasonal_naive", base, base, base)

    if y.size >= 7:
        weekly = y[-7:]
        p50 = np.array([weekly[idx % 7] for idx in range(n_steps)], dtype=float)
        in_sample_pred = np.full_like(y, np.nan, dtype=float)
        in_sample_pred[7:] = y[:-7]
    else:
        p50 = np.full(n_steps, y[-1], dtype=float)
        in_sample_pred = np.full_like(y, y[-1], dtype=float)

    abs_error = np.abs(y - in_sample_pred)
    abs_error = abs_error[~np.isnan(abs_error)]
    if abs_error.size == 0:
        abs_error = np.array([0.0], dtype=float)
    q90 = float(np.quantile(abs_error, 0.90))
    q95 = float(np.quantile(abs_error, 0.95))
    p90 = np.clip(p50 + q90, 0.0, 100.0)
    p95 = np.clip(p50 + q95, 0.0, 100.0)
    return ModelForecast("seasonal_naive", np.clip(p50, 0.0, 100.0), p90, p95)


def _fit_predict_robust_trend(train_series: pd.Series, forecast_dates: pd.DatetimeIndex) -> ModelForecast:
    if train_series.empty:
        base = np.zeros(len(forecast_dates), dtype=float)
        return ModelForecast("robust_trend", base, base, base)
    beta, seasonal, residuals = _fit_robust_trend(train_series)
    p50 = _predict_robust_trend(beta, seasonal, len(train_series), forecast_dates)
    abs_error = np.abs(residuals)
    q90 = float(np.quantile(abs_error, 0.90)) if abs_error.size else 0.0
    q95 = float(np.quantile(abs_error, 0.95)) if abs_error.size else 0.0
    p90 = np.clip(p50 + q90, 0.0, 100.0)
    p95 = np.clip(p50 + q95, 0.0, 100.0)
    return ModelForecast("robust_trend", p50, p90, p95)


def _fit_predict_gbdt_lag(train_series: pd.Series, forecast_dates: pd.DatetimeIndex) -> ModelForecast:
    if train_series.empty:
        base = np.zeros(len(forecast_dates), dtype=float)
        return ModelForecast("gbdt_lag", base, base, base)

    feature_frame = _build_feature_frame(train_series)
    if feature_frame.empty or len(feature_frame) < 45:
        return _fit_predict_seasonal_naive(train_series, forecast_dates)

    feature_columns = [column for column in feature_frame.columns if column != "y"]
    X_train = feature_frame[feature_columns].to_numpy(dtype=float)
    y_train = feature_frame["y"].to_numpy(dtype=float)

    model = SimpleGradientBoostingStumps()
    model.fit(X_train, y_train)
    in_sample_pred = model.predict(X_train)
    abs_error = np.abs(y_train - in_sample_pred)
    q90 = float(np.quantile(abs_error, 0.90)) if abs_error.size else 0.0
    q95 = float(np.quantile(abs_error, 0.95)) if abs_error.size else 0.0

    history_values = train_series.to_list()
    preds: List[float] = []
    for date in forecast_dates:
        row = _build_feature_row(history_values, pd.Timestamp(date))
        x = np.array([[row[column] for column in feature_columns]], dtype=float)
        pred = float(model.predict(x)[0])
        pred = float(np.clip(pred, 0.0, 100.0))
        preds.append(pred)
        history_values.append(pred)

    p50 = np.array(preds, dtype=float)
    p90 = np.clip(p50 + q90, 0.0, 100.0)
    p95 = np.clip(p50 + q95, 0.0, 100.0)
    return ModelForecast("gbdt_lag", p50, p90, p95)


def _get_model_forecast(
    model_name: str, train_series: pd.Series, forecast_dates: pd.DatetimeIndex
) -> ModelForecast:
    if model_name == "seasonal_naive":
        return _fit_predict_seasonal_naive(train_series, forecast_dates)
    if model_name == "robust_trend":
        return _fit_predict_robust_trend(train_series, forecast_dates)
    if model_name == "gbdt_lag":
        return _fit_predict_gbdt_lag(train_series, forecast_dates)
    raise ValueError(f"Unsupported model: {model_name}")


def _rolling_backtest(
    series: pd.Series,
    model_name: str,
    horizon_days: int,
    folds: int,
    min_train_days: int,
) -> Dict[str, float]:
    fold_metrics: List[Dict[str, float]] = []
    n = len(series)
    if n < min_train_days + horizon_days:
        return {
            "wape": float("nan"),
            "mae": float("nan"),
            "pinball_p90": float("nan"),
            "calibration_p90": float("nan"),
            "folds": 0.0,
        }

    for fold in range(folds, 0, -1):
        split_point = n - (fold * horizon_days)
        if split_point < min_train_days:
            continue
        train = series.iloc[:split_point]
        test = series.iloc[split_point : split_point + horizon_days]
        if len(test) == 0:
            continue

        forecast = _get_model_forecast(model_name, train, test.index)
        pred = forecast.p50[: len(test)]
        pred_p90 = forecast.p90[: len(test)]
        y_true = test.to_numpy(dtype=float)

        fold_metrics.append(
            {
                "wape": _safe_wape(y_true, pred),
                "mae": float(np.mean(np.abs(y_true - pred))),
                "pinball_p90": _pinball_loss(y_true, pred_p90, 0.90),
                "calibration_p90": float(np.mean(y_true <= pred_p90)),
            }
        )

    if not fold_metrics:
        return {
            "wape": float("nan"),
            "mae": float("nan"),
            "pinball_p90": float("nan"),
            "calibration_p90": float("nan"),
            "folds": 0.0,
        }

    metrics_frame = pd.DataFrame(fold_metrics)
    return {
        "wape": float(metrics_frame["wape"].mean()),
        "mae": float(metrics_frame["mae"].mean()),
        "pinball_p90": float(metrics_frame["pinball_p90"].mean()),
        "calibration_p90": float(metrics_frame["calibration_p90"].mean()),
        "folds": float(len(fold_metrics)),
    }


def run_host_metric_forecasts(
    daily_target: pd.DataFrame,
    horizons: Sequence[int],
    backtest_horizon_days: int = 30,
    backtest_folds: int = 3,
    min_train_days: int = 90,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    key_columns = ["metric", "hostid", "host", "as_value"]
    optional_key_columns = [column for column in ("env_value", "env_group") if column in daily_target.columns]
    group_columns = [*key_columns, *optional_key_columns]
    forecast_columns = [*group_columns, "date", "horizon_day", "model", "p50", "p90", "p95"]
    backtest_columns = [*group_columns, "model", "wape", "mae", "pinball_p90", "calibration_p90", "folds"]
    selection_columns = [
        *group_columns,
        "selected_model",
        "selection_score",
        "wape",
        "mae",
        "pinball_p90",
        "calibration_p90",
        "series_days",
    ]

    if daily_target.empty:
        return (
            pd.DataFrame(columns=forecast_columns),
            pd.DataFrame(columns=backtest_columns),
            pd.DataFrame(columns=selection_columns),
        )

    models = ("seasonal_naive", "robust_trend", "gbdt_lag")
    max_horizon = int(max(horizons))
    grouped = list(daily_target.groupby(group_columns))

    forecast_rows: List[Dict] = []
    backtest_rows: List[Dict] = []
    selection_rows: List[Dict] = []

    total_groups = len(grouped)
    progress_bar("forecast", 0, total_groups)
    completed_groups = 0

    for group_key, group in grouped:
        if isinstance(group_key, tuple):
            key_values = dict(zip(group_columns, group_key))
        else:
            key_values = {group_columns[0]: group_key}
        metric = str(key_values["metric"])
        hostid = str(key_values["hostid"])
        host = str(key_values["host"])
        as_value = str(key_values["as_value"])
        series = (
            group.sort_values("date")
            .set_index("date")["target_p95"]
            .astype(float)
        )
        series = _prepare_daily_series(series)
        if series.empty:
            completed_groups += 1
            progress_bar("forecast", completed_groups, total_groups)
            continue

        eval_results: List[Dict[str, float]] = []
        for model_name in models:
            metrics = _rolling_backtest(
                series=series,
                model_name=model_name,
                horizon_days=backtest_horizon_days,
                folds=backtest_folds,
                min_train_days=min_train_days,
            )
            score = (
                (metrics["wape"] if np.isfinite(metrics["wape"]) else 10.0)
                + (metrics["pinball_p90"] if np.isfinite(metrics["pinball_p90"]) else 10.0)
                + (
                    abs(metrics["calibration_p90"] - 0.90)
                    if np.isfinite(metrics["calibration_p90"])
                    else 1.0
                )
            )
            eval_results.append(
                {
                    "model": model_name,
                    "score": float(score),
                    **metrics,
                }
            )

            backtest_rows.append(
                {
                    **key_values,
                    "model": model_name,
                    "wape": metrics["wape"],
                    "mae": metrics["mae"],
                    "pinball_p90": metrics["pinball_p90"],
                    "calibration_p90": metrics["calibration_p90"],
                    "folds": metrics["folds"],
                }
            )

        eval_frame = pd.DataFrame(eval_results)
        best_row = eval_frame.sort_values("score", ascending=True).iloc[0]
        selected_model = str(best_row["model"])

        selection_rows.append(
            {
                **key_values,
                "selected_model": selected_model,
                "selection_score": float(best_row["score"]),
                "wape": float(best_row["wape"]),
                "mae": float(best_row["mae"]),
                "pinball_p90": float(best_row["pinball_p90"]),
                "calibration_p90": float(best_row["calibration_p90"]),
                "series_days": int(len(series)),
            }
        )

        forecast_dates = pd.date_range(
            start=series.index[-1] + pd.Timedelta(days=1),
            periods=max_horizon,
            freq="D",
            tz=series.index.tz,
        )
        model_forecast = _get_model_forecast(selected_model, series, forecast_dates)
        for idx, forecast_date in enumerate(forecast_dates):
            horizon_day = idx + 1
            forecast_rows.append(
                {
                    **key_values,
                    "date": forecast_date,
                    "horizon_day": horizon_day,
                    "model": selected_model,
                    "p50": float(model_forecast.p50[idx]),
                    "p90": float(model_forecast.p90[idx]),
                    "p95": float(model_forecast.p95[idx]),
                }
            )

        completed_groups += 1
        progress_bar("forecast", completed_groups, total_groups)

    forecast_df = pd.DataFrame(forecast_rows, columns=forecast_columns)
    backtest_df = pd.DataFrame(backtest_rows, columns=backtest_columns)
    selection_df = pd.DataFrame(selection_rows, columns=selection_columns)

    if not forecast_df.empty:
        forecast_df["date"] = pd.to_datetime(forecast_df["date"], utc=True, errors="coerce")
    return forecast_df, backtest_df, selection_df


def build_actionable_recommendations(
    risk_metrics: pd.DataFrame,
    forecast_df: pd.DataFrame,
) -> pd.DataFrame:
    optional_key_columns = [column for column in ("env_value", "env_group") if column in risk_metrics.columns]
    columns = [
        "metric",
        "hostid",
        "host",
        "as_value",
        *optional_key_columns,
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
    if risk_metrics.empty:
        return pd.DataFrame(columns=columns)

    rows: List[Dict] = []
    for _, risk_row in risk_metrics.iterrows():
        metric = str(risk_row["metric"])
        hostid = str(risk_row["hostid"])
        mask = (forecast_df["metric"] == metric) & (forecast_df["hostid"] == hostid)
        for column in optional_key_columns:
            if column in forecast_df.columns:
                mask = mask & (
                    forecast_df[column].fillna("").astype(str)
                    == str(risk_row.get(column, ""))
                )
        host_forecast = forecast_df[mask].sort_values("date")

        row = {
            "metric": metric,
            "hostid": hostid,
            "host": risk_row["host"],
            "as_value": risk_row["as_value"],
            **{column: risk_row.get(column, "") for column in optional_key_columns},
            "cluster": risk_row["cluster"],
            "overprovisioned": bool(risk_row["overprovisioned"]),
            "p50": float(risk_row["p50"]),
            "p95": float(risk_row["p95"]),
            "p99": float(risk_row["p99"]),
            "duty_cycle_80": float(risk_row["duty_cycle_80"]),
            "duty_cycle_90": float(risk_row["duty_cycle_90"]),
            "burstiness": float(risk_row["burstiness"]),
            "volatility": float(risk_row["volatility"]) if pd.notna(risk_row["volatility"]) else float("nan"),
        }

        for curve in ("p50", "p90", "p95"):
            for threshold in (80, 90, 95):
                days, _ = _first_crossing_days(host_forecast, curve, float(threshold))
                row[f"days_to_{threshold}_{curve}"] = days

        if str(risk_row["cluster"]) == "hot":
            candidates = [row.get("days_to_90_p90"), row.get("days_to_90_p95")]
            valid_candidates = [value for value in candidates if value is not None]
            basis_days = min(valid_candidates) if valid_candidates else None
            basis_curve = "hot_p90_p95"
            _, basis_date = _first_crossing_days(host_forecast, "p90", 90.0)
            _, p95_date = _first_crossing_days(host_forecast, "p95", 90.0)
            if basis_date is None or (p95_date is not None and p95_date < basis_date):
                basis_date = p95_date
        else:
            basis_days, basis_date = _first_crossing_days(host_forecast, "p50", 90.0)
            basis_curve = "p50"

        row["risk_basis"] = basis_curve
        row["days_to_90_basis"] = basis_days
        row["crossing_date_90_basis"] = basis_date

        if row["overprovisioned"] and str(risk_row["cluster"]) == "cold":
            status = "overprovisioned"
            recommendation = "рассмотреть консолидацию/переразмеривание"
        elif basis_days is None:
            status = "stable"
            recommendation = "пересечение порога в горизонте прогноза не ожидается"
        elif basis_days < 30:
            status = "critical"
            recommendation = "требуются меры по емкости в ближайшие 30 дней"
        elif basis_days <= 90:
            status = "watch"
            recommendation = "запланировать расширение емкости в горизонте 30-90 дней"
        else:
            status = "stable"
            recommendation = "мониторить тренд, немедленных действий не требуется"

        row["status"] = status
        row["recommendation"] = recommendation
        rows.append(row)

    output = pd.DataFrame(rows, columns=columns)
    if not output.empty and "crossing_date_90_basis" in output.columns:
        output["crossing_date_90_basis"] = pd.to_datetime(
            output["crossing_date_90_basis"], utc=True, errors="coerce"
        )
    return output


def _normal_cdf(values: np.ndarray) -> np.ndarray:
    vectorized_erf = np.vectorize(math.erf)
    return 0.5 * (1.0 + vectorized_erf(values / math.sqrt(2.0)))


def _confidence_index(wape: float, pinball: float, calibration: float) -> float:
    if np.isfinite(wape):
        err_score = 1.0 - min(max(wape, 0.0), 1.5) / 1.5
    else:
        err_score = 0.5
    if np.isfinite(pinball):
        pinball_score = 1.0 - min(max(pinball, 0.0), 20.0) / 20.0
    else:
        pinball_score = 0.5
    if np.isfinite(calibration):
        calibration_score = 1.0 - min(abs(calibration - 0.90), 0.50) / 0.50
    else:
        calibration_score = 0.5
    confidence = 100.0 * (0.45 * err_score + 0.25 * pinball_score + 0.30 * calibration_score)
    return float(np.clip(confidence, 0.0, 100.0))


def compute_horizon_risk_probabilities(
    forecast_df: pd.DataFrame,
    model_selection: pd.DataFrame,
    horizons: Sequence[int],
    threshold: float = 90.0,
) -> pd.DataFrame:
    key_columns = ["metric", "hostid", "host", "as_value"]
    optional_key_columns = [column for column in ("env_value", "env_group") if column in forecast_df.columns]
    group_columns = [*key_columns, *optional_key_columns]
    output_columns = [
        *group_columns,
        "horizon_days",
        "threshold",
        "prob_cross_pct",
        "prob_cross_adjusted_pct",
        "confidence_index_pct",
        "selected_model",
        "wape",
        "mae",
        "pinball_p90",
        "calibration_p90",
    ]
    if forecast_df.empty:
        return pd.DataFrame(columns=output_columns)

    normalized_horizons = sorted({int(value) for value in horizons if int(value) > 0})
    if not normalized_horizons:
        return pd.DataFrame(columns=output_columns)

    model_lookup: Dict[Tuple[str, ...], Dict[str, float]] = {}
    if not model_selection.empty:
        for _, row in model_selection.iterrows():
            key = tuple(str(row[column]) for column in group_columns)
            model_lookup[key] = {
                "selected_model": str(row.get("selected_model", "")),
                "wape": float(row.get("wape", float("nan"))),
                "mae": float(row.get("mae", float("nan"))),
                "pinball_p90": float(row.get("pinball_p90", float("nan"))),
                "calibration_p90": float(row.get("calibration_p90", float("nan"))),
            }

    rows: List[Dict] = []
    grouped = list(forecast_df.groupby(group_columns))
    for group_key, group in grouped:
        if isinstance(group_key, tuple):
            key_values = dict(zip(group_columns, group_key))
        else:
            key_values = {group_columns[0]: group_key}
        key_tuple = tuple(str(key_values[column]) for column in group_columns)
        model_info = model_lookup.get(
            key_tuple,
            {
                "selected_model": "",
                "wape": float("nan"),
                "mae": float("nan"),
                "pinball_p90": float("nan"),
                "calibration_p90": float("nan"),
            },
        )
        confidence_index = _confidence_index(
            wape=float(model_info["wape"]),
            pinball=float(model_info["pinball_p90"]),
            calibration=float(model_info["calibration_p90"]),
        )
        sorted_group = group.sort_values("horizon_day")
        for horizon in normalized_horizons:
            horizon_slice = sorted_group[sorted_group["horizon_day"] <= horizon]
            if horizon_slice.empty:
                continue

            mu = pd.to_numeric(horizon_slice["p50"], errors="coerce").to_numpy(dtype=float)
            p90_values = pd.to_numeric(horizon_slice["p90"], errors="coerce").to_numpy(dtype=float)
            spread = np.maximum(p90_values - mu, 0.5)
            sigma = np.maximum(spread / Z90, 0.25)
            z_values = (threshold - mu) / sigma
            p_day = 1.0 - _normal_cdf(z_values)
            p_day = np.clip(p_day, 0.0, 1.0)
            prob_cross = float(1.0 - np.prod(1.0 - p_day))
            adjustment = 0.5 + 0.5 * (confidence_index / 100.0)
            prob_cross_adjusted = float(np.clip(prob_cross * adjustment, 0.0, 1.0))

            rows.append(
                {
                    **key_values,
                    "horizon_days": int(horizon),
                    "threshold": float(threshold),
                    "prob_cross_pct": prob_cross * 100.0,
                    "prob_cross_adjusted_pct": prob_cross_adjusted * 100.0,
                    "confidence_index_pct": confidence_index,
                    "selected_model": model_info["selected_model"],
                    "wape": model_info["wape"],
                    "mae": model_info["mae"],
                    "pinball_p90": model_info["pinball_p90"],
                    "calibration_p90": model_info["calibration_p90"],
                }
            )

    return pd.DataFrame(rows, columns=output_columns)
