# Zabbix inventory collector

Файлы:
- `monitoring_config.example.py` — пример конфига подключения к Zabbix API
- `hosts_tree.example.json` — отдельный JSON со структурой `Система -> Роль -> Хосты`
- `zabbix_inventory_collect.py` — основной скрипт

## Как запустить

1. Скопируй `monitoring_config.example.py` в `monitoring_config.py`
2. Скопируй `hosts_tree.example.json` в `hosts_tree.json`
3. Заполни URL/логин/пароль и свои хосты
4. Установи зависимость:

```bash
pip install requests
```

5. Запусти:

```bash
python zabbix_inventory_collect.py
```

На выходе появится `zabbix_inventory_report.json`.

## Что именно скрипт собирает

Только то, что реально уже есть в Zabbix items:
- systemd/services
- mounted filesystems
- block devices / diskstats
- network interfaces
- SSL endpoints
- абсолютные пути, которые реально попались в item name / key / lastvalue

Скрипт специально **не подставляет типовые пути по умолчанию** и **не угадывает** каталоги, которых нет в Zabbix.
