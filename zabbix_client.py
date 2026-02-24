from __future__ import annotations

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
    ) -> None:
        self.url = self._normalize_url(url)
        self.timeout = timeout
        self.verify_ssl = verify_ssl
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
        try:
            response = self.session.post(
                self.url,
                json=payload,
                headers=headers,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
        except requests.exceptions.SSLError as exc:
            raise ZabbixAPIError(
                "TLS certificate validation failed for Zabbix API. "
                "Set VERIFY_SSL=False only for temporary testing. "
                f"Original error: {exc}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise ZabbixAPIError(f"HTTP request to Zabbix API failed: {exc}") from exc
        body = response.json()
        if not isinstance(body, dict):
            raise ZabbixAPIError("Unexpected API response format.")
        return body

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
            try:
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
            except requests.RequestException:
                raise
            except (TypeError, ValueError, KeyError) as exc:
                raise ZabbixAPIError(f"Invalid login response format: {exc}") from exc
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
