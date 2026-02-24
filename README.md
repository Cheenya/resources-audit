# Zabbix Utilization Pipeline (API only)

Сценарий собирает утилизацию `CPU`, `RAM`, `Disk` по хостам, отфильтрованным по тегу `AS`, затем:

- выгружает точные данные за последние 30 дней (`history.get`);
- выгружает тренды за год (`trend.get`);
- делает суммаризацию по всем хостам и по значениям тега `AS`;
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
- `HISTORY_DAYS`, `TREND_DAYS`
- `DISK_FS`
- `CHUNK_SIZE`, `ITEM_CHUNK_SIZE`, `HISTORY_CHUNK_SIZE`, `TREND_CHUNK_SIZE`
- `REQUEST_TIMEOUT`, `VERIFY_SSL`
- `OUTPUT_DIR`
- `PLOTS_ENABLED`

Заполните `config.py` перед запуском.

## Структура кода (4 файла)

- `zabbix_utilization_pipeline.py` - оркестратор пайплайна: валидация конфига, запуск этапов, сохранение артефактов.
- `zabbix_client.py` - JSON-RPC клиент Zabbix API (login/call/logout, обработка ошибок).
- `processing.py` - выбор item'ов, загрузка `history/trend`, трансформации и суммаризация.
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
- `history_raw_api_<HISTORY_DAYS>d.csv` - сырой ответ `history.get` после нормализации
- `trend_raw_api_<TREND_DAYS>d.csv` - сырой ответ `trend.get` после нормализации
- `history_exact_<HISTORY_DAYS>d.csv` - точные данные утилизации (host-level)
- `trend_<TREND_DAYS>d.csv` - тренды утилизации (host-level)
- `history_summary_all_<HISTORY_DAYS>d.csv` - суммаризация по всем выбранным хостам
- `history_summary_by_as_<HISTORY_DAYS>d.csv` - суммаризация по AS
- `trend_summary_all_<TREND_DAYS>d.csv` - суммаризация трендов по всем хостам
- `trend_summary_by_as_<TREND_DAYS>d.csv` - суммаризация трендов по AS
- `run_context.json` - параметры и метаинформация запуска
- `summary_report_<HISTORY_DAYS>d_<TREND_DAYS>d.xlsx` - единый отчет:
  - `selected_items`
  - `history_summary_all`
  - `history_summary_by_as`
  - `trend_summary_all`
  - `trend_summary_by_as`
  - `conclusion` (краткое заключение по статусам и трендам)

В каталоге `<OUTPUT_DIR>/plots/`:

- `<metric>_dashboard.png` для `cpu`, `ram`, `disk`:
  - exact окно (`HISTORY_DAYS`): mean/median/p10-p90/max
  - trend окно (`TREND_DAYS`): mean + min/max envelope
  - host heatmap (daily mean)
- `<metric>_by_as.png` - средняя утилизация по значениям тега `AS`

## Замечания по ключам Zabbix

Сценарий автоматически подбирает стандартные ключи:

- CPU: приоритет у `system.cpu.util[...,idle,...]` (берется `100 - idle`); если такого ключа нет, fallback на `system.cpu.load[all,avg*] / system.cpu.num`
- RAM: `vm.memory.utilization*`, `vm.memory.size[pused]`, либо вычисление из пар `total + available/free/used`
- Disk: `vfs.fs.size[<fs>,pused]`

Если в вашей инсталляции нестандартные item keys, потребуется адаптировать логику выбора в `processing.py`.
