"""User configuration for zabbix_utilization_pipeline.py."""

# Zabbix connection
ZABBIX_URL = "https://zabbix.example.com/api_jsonrpc.php"
ZABBIX_USERNAME = "zabbix_user"
ZABBIX_PASSWORD = "zabbix_password"

# Host selection by AS tag
AS_TAG_KEY = "AS"
AS_TAG_VALUES = "AS-01,AS-02"
TAG_OPERATOR = "equals"  # equals | contains

# Time windows and forecast
HISTORY_DAYS = 30
TREND_DAYS = 365
FORECAST_DAYS = 90

# Forecast mode:
# - "python": local regression forecast from trend summary
# - "zabbix": read precomputed native forecast() values from Zabbix calculated items
FORECAST_SOURCE = "python"  # python | zabbix

# Native Zabbix forecast item keys (required only for FORECAST_SOURCE="zabbix")
# Example: "custom.cpu.forecast.p90d"
FORECAST_KEY_CPU = ""
FORECAST_KEY_RAM = ""
FORECAST_KEY_DISK = ""
FORECAST_LOOKBACK_DAYS = 30

# Preferred filesystems for disk utilization item selection
DISK_FS = "/,C:"

# API and output settings
CHUNK_SIZE = 100
REQUEST_TIMEOUT = 120
VERIFY_SSL = True
OUTPUT_DIR = "output"

# Plot settings
PLOTS_ENABLED = True
