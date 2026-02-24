"""User configuration for zabbix_utilization_pipeline.py."""

# Zabbix connection
ZABBIX_URL = "https://zabbix.example.com/api_jsonrpc.php"
ZABBIX_USERNAME = "zabbix_user"
ZABBIX_PASSWORD = "zabbix_password"

# Host selection by AS tag
AS_TAG_KEY = "AS"
AS_TAG_VALUES = "AS-01,AS-02"
TAG_OPERATOR = "equals"  # equals | contains

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

# Plot settings
PLOTS_ENABLED = True
