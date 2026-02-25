# Zabbix Utilization Pipeline (API only)

Сценарий собирает утилизацию `CPU`, `RAM`, `Disk` по хостам, отфильтрованным по тегу `AS`, затем:

- выгружает часовые тренды (`trend.get`);
- формирует "exact" окно из trend average (без поминутной выгрузки `history.get`);
- делает суммаризацию по всем хостам и по значениям тега `AS`;
- считает host-level risk-метрики (`p50/p95/p99`, `duty_cycle_80/90`, `burstiness`, `volatility`, `cold/warm/hot`);
- строит per-host/per-metric прогноз daily `p95` утилизации на горизонты `30/90` (модельный отбор по rolling backtest);
- формирует actionable-статусы (`critical/watch/stable/overprovisioned`);
- сохраняет детальные plot-схемы.

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Настройка

Все пользовательские параметры находятся в `config.py`:

- `ZABBIX_URL`
- `ZABBIX_USERNAME`
- `ZABBIX_PASSWORD`
- `AS_TAG_KEY`, `AS_TAG_VALUES`, `TAG_OPERATOR`
- `ENV_TAG_KEY`
- `HISTORY_DAYS`, `TREND_DAYS`
- `DISK_FS`
- `CHUNK_SIZE`, `ITEM_CHUNK_SIZE`, `HISTORY_CHUNK_SIZE`, `TREND_CHUNK_SIZE`
- `REQUEST_TIMEOUT`, `VERIFY_SSL`
- `OUTPUT_DIR`, `CSV_SUBDIR`, `XLSX_SUBDIR`
- `PLOTS_ENABLED`
- `FORECAST_ENABLED`
- `FORECAST_HORIZONS`
- `FORECAST_BACKTEST_HORIZON_DAYS`
- `FORECAST_BACKTEST_FOLDS`
- `FORECAST_MIN_TRAIN_DAYS`
- `FORECAST_MAX_PLOTS`

Заполните `config.py` перед запуском.

`HISTORY_DAYS` поддерживает два режима:
- `> 0` - фиксированное окно в днях (например `30`)
- `0` - всё доступное окно, которое пришло из `trend.get` (обычно `TREND_DAYS`)

## Структура кода

- `zabbix_utilization_pipeline.py` - оркестратор пайплайна: валидация конфига, запуск этапов, сохранение артефактов.
- `zabbix_client.py` - JSON-RPC клиент Zabbix API (login/call/logout, обработка ошибок).
- `processing.py` - выбор item'ов, загрузка `history/trend`, трансформации и суммаризация.
- `plotting.py` - построение dashboard-графиков и разреза по `AS`.
- `forecasting.py` - risk-метрики, rolling backtest, выбор модели, прогноз и actionable-рекомендации.

## Запуск

```bash
python3 zabbix_utilization_pipeline.py
```

Во время `item.get`, `trend.get` и этапа forecast выводится прогресс-бар.

Если данные уже собраны и нужно пересчитать только аналитику/прогноз:

```bash
python3 zabbix_utilization_pipeline.py --analysis-only
```

В этом режиме API Zabbix не вызывается; используются уже существующие файлы в `OUTPUT_DIR`:
- `csv/history_exact_<HISTORY_WINDOW>.csv`
- `csv/trend_<TREND_WINDOW>.csv`
- `csv/selected_items.csv` (если есть)

Если на машине не импортируется `matplotlib.pyplot`:

- установите зависимости в `venv` через `pip install -r requirements.txt`;
- не копируйте пакет `matplotlib` вручную в корень проекта.

Если видите ошибку `Importing the numpy C-extensions failed` и путь к `python3.13t.exe`:

- это конфликт free-threaded Python (`3.13t`) и бинарных wheel-пакетов;
- используйте обычный `python.exe` (не `python3.13t.exe`) в новом `venv`;
- заново установите зависимости: `pip install -r requirements.txt`.

Если видите `SSLCertVerificationError` при подключении к Zabbix API:

- убедитесь, что на машине корректная цепочка доверенных сертификатов;
- `VERIFY_SSL = False` используйте только как временный обход для теста.

Если видите `Gateway Time-out` (`504`) на `trend.get`:

- сценарий автоматически повторяет запрос и дробит проблемный запрос по `itemids` и по времени;
- при повторных сбоях на минимальном временном окне сценарий пропускает только этот маленький участок и продолжает сбор;
- если ошибка повторяется, уменьшите `CHUNK_SIZE` (например, `100 -> 25`) и/или увеличьте `REQUEST_TIMEOUT`.

## Что сохраняется

В каталоге `OUTPUT_DIR` (по умолчанию `output/`) формируется общая папка и разрез по областям:

- `output/csv/`, `output/xlsx/`, `output/plots/` - общие артефакты по всем AS
- `output/<AS>/<prod|non-prod>/csv/` - CSV по конкретной AS и окружению
- `output/<AS>/<prod|non-prod>/xlsx/` - XLSX-отчет по конкретной AS и окружению
- `output/<AS>/<prod|non-prod>/conclusion.txt` - текстовое заключение по рискам

Где `prod` определяется по тегу `ENV` (`ENV_TAG_KEY`) со значением `prod`, а всё остальное попадает в `non-prod`.

В `csv/` (и аналогично внутри каждой scope-папки):

- папка `csv/` - все табличные выгрузки CSV
- папка `xlsx/` - итоговый Excel-отчет
- папка `plots/` - графики

В папке `csv/`:

- `selected_items.csv` - какие элементы выбраны для метрик
- `history_raw_api_<HISTORY_WINDOW>.csv` - "exact" ряд, полученный из `trend.value_avg`
- `trend_raw_api_<TREND_WINDOW>.csv` - сырой ответ `trend.get` после нормализации
- `history_exact_<HISTORY_WINDOW>.csv` - точные данные утилизации (host-level)
- `trend_<TREND_WINDOW>.csv` - тренды утилизации (host-level)
- `history_features_<HISTORY_WINDOW>.csv` - дополнительные признаки для предикта (exact, host-level)
- `trend_features_<TREND_WINDOW>.csv` - дополнительные признаки для предикта (trend, host-level)
- `history_summary_all_<HISTORY_WINDOW>.csv` - суммаризация по всем выбранным хостам
- `history_summary_by_as_<HISTORY_WINDOW>.csv` - суммаризация по AS
- `trend_summary_all_<TREND_WINDOW>.csv` - суммаризация трендов по всем хостам
- `trend_summary_by_as_<TREND_WINDOW>.csv` - суммаризация трендов по AS
- `daily_target_p95_<HISTORY_WINDOW>.csv` - дневная целевая серия (`daily p95`) для прогноза
- `host_risk_metrics_<HISTORY_WINDOW>.csv` - risk-метрики по host/metric (`p50/p95/p99`, `duty_cycle`, `burstiness`, `volatility`, `cluster`)
- `model_backtest.csv` - качество моделей на rolling backtest (`WAPE`, `MAE`, `pinball_p90`, `calibration_p90`)
- `model_selection.csv` - выбранная модель на каждый host/metric
- `forecast_daily.csv` - прогноз на каждый день горизонта (`p50`, `p90`, `p95`)
- `risk_probabilities.csv` - вероятность сценария пересечения порога 90% по горизонтам + индекс доверия
- `actionable_recommendations.csv` - итоговые статусы и рекомендации (`critical/watch/stable/overprovisioned`)
- `run_context.json` - параметры и метаинформация запуска (в корне `OUTPUT_DIR`)

В папке `xlsx/`:
- `summary_report_<HISTORY_WINDOW>_<TREND_WINDOW>.xlsx` - единый отчет:
  - `selected_items`
  - `history_summary_all`
  - `history_summary_by_as`
  - `trend_summary_all`
  - `trend_summary_by_as`
  - `daily_target_p95`
  - `host_risk_metrics`
  - `model_backtest`
  - `model_selection`
  - `forecast_daily`
  - `actionable`
  - `conclusion` (включая сводку по `critical/watch/stable/overprovisioned`)

В каталоге `<OUTPUT_DIR>/plots/`:

- `<metric>_dashboard.png` для `cpu`, `ram`, `disk`:
  - exact окно (`HISTORY_DAYS` или `all`): mean/median/p10-p90/max
  - trend окно (`TREND_DAYS`): mean + min/max envelope
  - host heatmap (daily mean)
- `<metric>_by_as.png` - средняя утилизация по значениям тега `AS`
- `forecasts/<metric>_<host>_<hostid>.png` - forecast-кривые (`history daily p95` + `p50/p90/p95`, пороги `80/90/95`) для top-risk хостов

## Логика предикта

- Цель: `daily p95 utilization` на host/metric.
- Горизонты: из `FORECAST_HORIZONS` (по умолчанию `30,90,180,365`).
- Модели: `seasonal_naive` (неделя-к-неделе), `robust_trend` (робастный линейный тренд + недельная сезонность), `gbdt_lag` (градиентный бустинг по лагам/окнам).
- Отбор: лучшая модель выбирается по rolling backtest.
- Метрики качества: `WAPE`, `MAE`, `pinball_p90`, `calibration_p90`.
- Для каждого горизонта считается вероятность сценария пересечения 90% и индекс доверия прогноза (на базе качества backtest).
- Для каждой AS и ENV-группы формируется текстовое заключение с секциями:
  - критично сейчас (<= 30 дней),
  - критично скоро (31-90 дней),
  - риск в 6 месяцев (91-180 дней),
  - риск в 12 месяцев (181-365 дней),
  - стабильные и overprovisioned.
- Для `hot` риск оценивается по консервативным кривым (`p90/p95`), для остальных по `p50`.
- Actionable-статусы:
  - `critical`: пересечение `90%` менее чем через `30` дней.
  - `watch`: пересечение `90%` в диапазоне `30-90` дней.
  - `stable`: пересечение позже `90` дней или не ожидается на горизонте.
  - `overprovisioned`: стабильно низкая утилизация (`cold` + низкий `p95`).

## Замечания по ключам Zabbix

Сценарий автоматически подбирает стандартные ключи:

- CPU: используется только `system.cpu.util*`; приоритет у `system.cpu.util[...,idle,...]` (берется `100 - idle`)
- RAM target: `100 - vm.memory.size[pavailable]`
- RAM features: `vm.memory.size[pavailable]`, `vm.memory.size[pused]`, `vm.memory.size[available]/[free]`, `vm.memory.size[used]`, `vm.memory.size[total]`
- Disk target: `vfs.fs.size[<fs>,pused]` (по приоритету `DISK_FS`)
- Disk features: `vfs.fs.size[<fs>,used]`, `vfs.fs.size[<fs>,free]`, `vfs.fs.size[<fs>,total]` для того же выбранного `<fs>`

Если в вашей инсталляции нестандартные item keys, потребуется адаптировать логику выбора в `processing.py`.
