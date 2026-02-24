from __future__ import annotations

import time
from typing import Any, Dict

import requests


class ZabbixAPIError(RuntimeError):
    """Raised when Zabbix API returns an error."""


class ZabbixAPI:
    """Minimal JSON-RPC client for Zabbix API."""

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        timeout: int = 120,
        verify_ssl: Any = True,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
    ) -> None:
        self.url = self._normalize_url(url)
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff = max(0.1, float(retry_backoff))
        self.retry_http_codes = {429, 502, 503, 504}
        self.session = requests.Session()
        self.request_id = 1
        self.auth = self._login(username, password)

    @staticmethod
    def _normalize_url(url: str) -> str:
        stripped = url.strip()
        if stripped.endswith("/"):
            stripped = stripped[:-1]
        if stripped.endswith("api_jsonrpc.php"):
            return stripped
        return f"{stripped}/api_jsonrpc.php"

    def _post(self, payload: Dict, headers: Dict[str, str]) -> Dict:
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.post(
                    self.url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )
                if response.status_code in self.retry_http_codes and attempt < self.max_retries:
                    self._sleep_before_retry(attempt)
                    continue
                response.raise_for_status()
                try:
                    body = response.json()
                except ValueError as exc:
                    raise ZabbixAPIError("Failed to decode API response as JSON.") from exc
                if not isinstance(body, dict):
                    raise ZabbixAPIError("Unexpected API response format.")
                return body
            except requests.exceptions.SSLError as exc:
                raise ZabbixAPIError(
                    "TLS certificate validation failed for Zabbix API. "
                    "Set VERIFY_SSL=False only for temporary testing. "
                    f"Original error: {exc}"
                ) from exc
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else "unknown"
                if (
                    isinstance(status_code, int)
                    and status_code in self.retry_http_codes
                    and attempt < self.max_retries
                ):
                    self._sleep_before_retry(attempt)
                    continue
                snippet = ""
                if exc.response is not None and exc.response.text:
                    snippet = exc.response.text.strip().replace("\n", " ")[:220]
                details = f" Response: {snippet}" if snippet else ""
                raise ZabbixAPIError(
                    f"HTTP request to Zabbix API failed with status {status_code}.{details}"
                ) from exc
            except (
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError,
            ) as exc:
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt)
                    continue
                raise ZabbixAPIError(f"HTTP request to Zabbix API failed: {exc}") from exc
            except requests.exceptions.RequestException as exc:
                raise ZabbixAPIError(f"HTTP request to Zabbix API failed: {exc}") from exc
        raise ZabbixAPIError("HTTP request to Zabbix API failed after retries.")

    def _sleep_before_retry(self, attempt: int) -> None:
        delay = min(self.retry_backoff * (2 ** attempt), 20.0)
        time.sleep(delay)

    def _raw_call(self, method: str, params: Any, include_auth: bool) -> Dict:
        payload: Dict = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.request_id,
        }
        self.request_id += 1

        headers = {"Content-Type": "application/json-rpc"}
        if include_auth and method != "user.login":
            payload["auth"] = self.auth

        return self._post(payload, headers)

    def _login(self, username: str, password: str) -> str:
        for params in (
            {"username": username, "password": password},
            {"user": username, "password": password},
        ):
            response = self._raw_call(
                method="user.login",
                params=params,
                include_auth=False,
            )
            if "error" in response:
                continue
            auth = response.get("result")
            if isinstance(auth, str) and auth:
                return auth
        raise ZabbixAPIError("Unable to login with provided credentials.")

    def call(self, method: str, params: Any) -> Any:
        response = self._raw_call(
            method=method,
            params=params,
            include_auth=(method != "apiinfo.version"),
        )
        if "error" in response:
            error = response["error"]
            raise ZabbixAPIError(
                f"{method} failed: {error.get('code')} {error.get('message')} "
                f"{error.get('data', '')}"
            )
        return response.get("result", [])

    def logout(self) -> None:
        if not self.auth:
            return
        response = self._raw_call(
            method="user.logout",
            params=[],
            include_auth=True,
        )
        if "error" in response:
            error = response["error"]
            error_data = str(error.get("data", ""))
            if 'No permissions to call "user.logout"' in error_data:
                self.auth = ""
                return
            raise ZabbixAPIError(
                f"user.logout failed: {error.get('code')} {error.get('message')} "
                f"{error_data}"
            )
        self.auth = ""
