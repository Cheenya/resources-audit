"""User configuration for zabbix_utilization_pipeline.py."""

# Zabbix connection
ZABBIX_URL = "https://zabbix.example.com/api_jsonrpc.php"
ZABBIX_USERNAME = "zabbix_user"
ZABBIX_PASSWORD = "zabbix_password"

# Host selection by AS tag
AS_TAG_KEY = "AS"
AS_TAG_VALUES = "AS-01,AS-02"
TAG_OPERATOR = "equals"  # equals | contains
ENV_TAG_KEY = "ENV"

# Time windows
HISTORY_DAYS = 30  # set 0 to use the full available trend window as "exact"
TREND_DAYS = 365

# Preferred filesystems for disk utilization item selection
DISK_FS = "/,C:"

# API and output settings
CHUNK_SIZE = 100
ITEM_CHUNK_SIZE = CHUNK_SIZE
HISTORY_CHUNK_SIZE = CHUNK_SIZE
TREND_CHUNK_SIZE = CHUNK_SIZE
REQUEST_TIMEOUT = 120
VERIFY_SSL = True
OUTPUT_DIR = "output"
CSV_SUBDIR = "csv"
XLSX_SUBDIR = "xlsx"

# Plot settings
PLOTS_ENABLED = True

# Forecast settings
FORECAST_ENABLED = True
FORECAST_HORIZONS = "30,90,180,365"
FORECAST_BACKTEST_HORIZON_DAYS = 30
FORECAST_BACKTEST_FOLDS = 3
FORECAST_MIN_TRAIN_DAYS = 90
FORECAST_MAX_PLOTS = 12
