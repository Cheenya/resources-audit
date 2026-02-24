# Zabbix Utilization Pipeline (API only)

Сценарий собирает утилизацию `CPU`, `RAM`, `Disk` по хостам, отфильтрованным по тегу `AS`, затем:

- выгружает точные данные за последние 30 дней (`history.get`);
- выгружает тренды за год (`trend.get`);
- делает суммаризацию по всем хостам и по значениям тега `AS`;
- строит прогноз утилизации;
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
- `HISTORY_DAYS`, `TREND_DAYS`, `FORECAST_DAYS`
- `FORECAST_SOURCE`, `FORECAST_LOOKBACK_DAYS`
- `FORECAST_KEY_CPU`, `FORECAST_KEY_RAM`, `FORECAST_KEY_DISK`
- `CHECK_FORECAST_ONLY`
- `DISK_FS`
- `CHUNK_SIZE`, `ITEM_CHUNK_SIZE`, `HISTORY_CHUNK_SIZE`, `TREND_CHUNK_SIZE`
- `REQUEST_TIMEOUT`, `VERIFY_SSL`
- `OUTPUT_DIR`
- `PLOTS_ENABLED`

Заполните `config.py` перед запуском.

### Нативный forecast Zabbix

По умолчанию используется локальная модель (`FORECAST_SOURCE = "python"`).

Если хотите использовать нативный `forecast()` из Zabbix:

- создайте в Zabbix рассчитанные item'ы (calculated items), где значение считается через `forecast(...)`;
- в `config.py` включите `FORECAST_SOURCE = "zabbix"`;
- задайте ключи рассчитанных item'ов:
  - `FORECAST_KEY_CPU`
  - `FORECAST_KEY_RAM`
  - `FORECAST_KEY_DISK`
- задайте `FORECAST_LOOKBACK_DAYS` (период истории этих forecast-item'ов для выгрузки).

Если нативные forecast-item'ы не найдены или по ним нет данных, сценарий автоматически переключится на Python forecast.

Для быстрой проверки forecast-item'ов без долгого сбора истории/трендов:

- задайте `FORECAST_KEY_CPU/RAM/DISK`;
- включите `CHECK_FORECAST_ONLY = True`;
- запустите сценарий и проверьте `output/forecast_item_check.csv`.

## Структура кода (4 файла)

- `zabbix_utilization_pipeline.py` - оркестратор пайплайна: валидация конфига, запуск этапов, сохранение артефактов.
- `zabbix_client.py` - JSON-RPC клиент Zabbix API (login/call/logout, обработка ошибок).
- `processing.py` - выбор item'ов, загрузка `history/trend`, трансформации, суммаризация и прогноз.
- `plotting.py` - построение dashboard-графиков и разреза по `AS`.

## Запуск

```bash
python3 zabbix_utilization_pipeline.py
```

Во время `item.get`, `history.get` и `trend.get` выводится прогресс-бар по чанкам API.

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

Если видите `Gateway Time-out` (`504`) на `history.get`/`trend.get`:

- сценарий автоматически повторяет запрос и дробит проблемный запрос по `itemids` и по времени;
- при повторных сбоях на минимальном временном окне сценарий пропускает только этот маленький участок и продолжает сбор;
- если ошибка повторяется, уменьшите `CHUNK_SIZE` (например, `100 -> 25`) и/или увеличьте `REQUEST_TIMEOUT`.

## Что сохраняется

В каталоге, указанном в `OUTPUT_DIR` (по умолчанию `output/`):

- `selected_items.csv` - какие элементы выбраны для метрик
- `history_exact_<HISTORY_DAYS>d.csv` - точные данные утилизации (host-level)
- `trend_<TREND_DAYS>d.csv` - тренды утилизации (host-level)
- `history_summary_all_<HISTORY_DAYS>d.csv` - суммаризация по всем выбранным хостам
- `history_summary_by_as_<HISTORY_DAYS>d.csv` - суммаризация по AS
- `trend_summary_all_<TREND_DAYS>d.csv` - суммаризация трендов по всем хостам
- `trend_summary_by_as_<TREND_DAYS>d.csv` - суммаризация трендов по AS
- `forecast_<FORECAST_DAYS>d.csv` - прогноз утилизации
- `run_context.json` - параметры и метаинформация запуска
- `forecast_item_check.csv` - проверка forecast-item'ов (если заданы forecast-ключи)

В каталоге `<OUTPUT_DIR>/plots/`:

- `<metric>_dashboard.png` для `cpu`, `ram`, `disk`:
  - exact окно (`HISTORY_DAYS`): mean/median/p10-p90/max
  - trend окно (`TREND_DAYS`): mean + min/max envelope
  - host heatmap (daily mean)
  - forecast + доверительный интервал
- `<metric>_by_as.png` - средняя утилизация по значениям тега `AS`

## Замечания по ключам Zabbix

Сценарий автоматически подбирает стандартные ключи:

- CPU: `system.cpu.util*`
- RAM: `vm.memory.utilization*`, `vm.memory.size[pused]`, либо вычисление из пар `total + available/free/used`
- Disk: `vfs.fs.size[<fs>,pused]`

Если в вашей инсталляции нестандартные item keys, потребуется адаптировать логику выбора в `processing.py`.
