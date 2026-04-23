"""
SSH inventory config.

1) Copy this file to `inventory_ssh_config.py`
2) Fill in passwords / key paths locally
3) Do NOT commit the real credentials.

Used by: `zabbix_inventory_ssh_scan.py`
"""

from __future__ import annotations

DEFAULT_SSH_PORT: int = 22
SSH_CONNECT_TIMEOUT_SEC: int = 10

# Map domain suffix -> auth parameters
# Example: host "app01.rosap.com" will use DOMAIN_ACCOUNTS["rosap.com"]
DOMAIN_ACCOUNTS: dict[str, dict[str, str | None]] = {
    "rosap.com": {
        "username": "rosap",
        "password": "",  # fill in OR set to None and use key_filename
        "key_filename": None,  # e.g. "/home/user/.ssh/id_rsa"; None to use password
    },
    "dom.ru": {
        "username": "dom",
        "password": "",  # fill in OR set to None and use key_filename
        "key_filename": None,
    },
}
