from __future__ import annotations

from collections import deque
import math
import re
import sys
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from zabbix_client import ZabbixAPI


@dataclass(frozen=True)
class ItemSelection:
    hostid: str
    host: str
    itemid: str
    key_: str
    value_type: int
    metric: str
    transform: str
    as_value: str
    source: str = "direct"


@dataclass(frozen=True)
class RamPairSelection:
    hostid: str
    host: str
    as_value: str
    total_itemid: str
    total_value_type: int
    part_itemid: str
    part_value_type: int
    mode: str  # available or used


def progress_bar(prefix: str, current: int, total: int) -> None:
    if total <= 0:
        return
    width = 28
    ratio = min(max(current / total, 0.0), 1.0)
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    print(
        f"\r{prefix}: [{bar}] {current}/{total}",
        end="" if current < total else "\n",
        file=sys.stderr,
        flush=True,
    )


def chunked(seq: Sequence[str], size: int) -> Iterator[List[str]]:
    for idx in range(0, len(seq), size):
        yield list(seq[idx : idx + size])


def parse_csv_values(raw: str) -> List[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def pick_as_value(
    tags: Sequence[Dict], as_tag_key: str, preferred_values: Sequence[str]
) -> str:
    values = [tag.get("value", "") for tag in tags if tag.get("tag") == as_tag_key]
    if not values:
        return ""
    if preferred_values:
        preferred_set = {value: idx for idx, value in enumerate(preferred_values)}
        matched = [value for value in values if value in preferred_set]
        if matched:
            matched.sort(key=lambda value: preferred_set[value])
            return matched[0]
    return values[0]


def get_hosts_by_as(
    api: ZabbixAPI,
    as_tag_key: str,
    as_values: Sequence[str],
    tag_operator: str,
) -> List[Dict]:
    if as_values:
        operator = 1 if tag_operator == "equals" else 0
        tags = [
            {"tag": as_tag_key, "value": value, "operator": operator}
            for value in as_values
        ]
        evaltype = 2 if len(tags) > 1 else 0
    else:
        tags = [{"tag": as_tag_key, "operator": 4}]
        evaltype = 0

    params = {
        "output": ["hostid", "host", "name", "status"],
        "selectTags": "extend",
        "filter": {"status": "0"},
        "tags": tags,
        "evaltype": evaltype,
    }
    hosts = api.call("host.get", params)
    hosts.sort(key=lambda host: host.get("host", ""))
    return hosts


def get_items_for_hosts(
    api: ZabbixAPI, hostids: Sequence[str], chunk_size: int
) -> List[Dict]:
    fields = ["itemid", "hostid", "name", "key_", "value_type", "status", "units"]
    all_items: List[Dict] = []
    queue = deque(chunked(list(hostids), chunk_size))
    total_chunks = len(queue)
    completed_chunks = 0
    while queue:
        host_chunk = queue.popleft()
        params = {
            "output": fields,
            "hostids": host_chunk,
            "filter": {"status": "0"},
        }
        try:
            chunk_items = api.call("item.get", params)
        except Exception as exc:
            if len(host_chunk) > 1 and is_transient_api_error(exc):
                midpoint = len(host_chunk) // 2
                left_chunk = host_chunk[:midpoint]
                right_chunk = host_chunk[midpoint:]
                if left_chunk and right_chunk:
                    print(
                        f"\nitem.get: transient failure on chunk size {len(host_chunk)}, "
                        f"splitting into {len(left_chunk)} + {len(right_chunk)}",
                        file=sys.stderr,
                        flush=True,
                    )
                    queue.appendleft(right_chunk)
                    queue.appendleft(left_chunk)
                    total_chunks += 1
                    continue
            raise
        all_items.extend(chunk_items)
        completed_chunks += 1
        progress_bar("item.get", completed_chunks, total_chunks)
    return all_items


def index_items_by_host(items: Sequence[Dict]) -> Dict[str, List[Dict]]:
    grouped: Dict[str, List[Dict]] = {}
    for item in items:
        hostid = item.get("hostid", "")
        grouped.setdefault(hostid, []).append(item)
    return grouped


def cpu_score(item: Dict) -> Tuple[int, str]:
    key = item.get("key_", "")
    name = item.get("name", "").lower()

    if key == "system.cpu.util":
        score = 200
        transform = "identity"
    elif key.startswith("system.cpu.util[") and "idle" in key:
        score = 180
        transform = "invert_100"
    elif key.startswith("system.cpu.util"):
        score = 50
        transform = "identity"
    else:
        score = -1
        transform = "identity"

    if "cpu utilization" in name:
        score += 10
    return score, transform


def ram_direct_score(item: Dict) -> int:
    key = item.get("key_", "")
    name = item.get("name", "").lower()
    if key.startswith("vm.memory.utilization"):
        score = 200
    elif key == "vm.memory.size[pused]":
        score = 180
    elif key.startswith("vm.memory.size[pused"):
        score = 170
    else:
        score = -1
    if "memory utilization" in name:
        score += 10
    return score


DISK_PUSED_RE = re.compile(r"^vfs\.fs\.size\[(?P<fs>[^,]+),pused\]$")
TRANSIENT_API_ERROR_PATTERNS = (
    "gateway timeout",
    "gateway time-out",
    "bad gateway",
    "read timed out",
    "connect timeout",
    "timed out",
    "status 429",
    "status 502",
    "status 503",
    "status 504",
)
MIN_HISTORY_WINDOW_SECONDS = 15 * 60
MIN_TREND_WINDOW_SECONDS = 6 * 60 * 60


def is_transient_api_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(pattern in message for pattern in TRANSIENT_API_ERROR_PATTERNS)


def split_time_window(
    window_from: int, window_till: int, min_window_seconds: int
) -> Optional[Tuple[Tuple[int, int], Tuple[int, int]]]:
    span = int(window_till) - int(window_from)
    if span <= max(1, int(min_window_seconds)):
        return None
    midpoint = int(window_from) + (span // 2)
    if midpoint <= int(window_from) or midpoint >= int(window_till):
        return None
    left = (int(window_from), int(midpoint))
    right = (int(midpoint) + 1, int(window_till))
    if left[0] > left[1] or right[0] > right[1]:
        return None
    return left, right


def disk_score(item: Dict, fs_preferences: Sequence[str]) -> Tuple[int, str]:
    key = item.get("key_", "")
    match = DISK_PUSED_RE.match(key)
    if not match:
        return -1, ""

    fs_name = match.group("fs")
    fs_map = {fs: idx for idx, fs in enumerate(fs_preferences)}
    score = 80
    if fs_name in fs_map:
        score = 200 - fs_map[fs_name]
    elif fs_name == "/":
        score = 170
    elif fs_name.upper().startswith("C"):
        score = 150
    return score, fs_name


def select_items(
    items_by_host: Dict[str, List[Dict]],
    host_meta: Dict[str, Dict[str, str]],
    disk_fs_preferences: Sequence[str],
) -> Tuple[List[ItemSelection], List[RamPairSelection]]:
    direct: List[ItemSelection] = []
    ram_pairs: List[RamPairSelection] = []

    for hostid, host_items in items_by_host.items():
        host_info = host_meta.get(hostid)
        if host_info is None:
            continue
        host_name = host_info["host"]
        as_value = host_info["as_value"]

        cpu_candidates: List[Tuple[int, str, Dict]] = []
        for item in host_items:
            score, transform = cpu_score(item)
            if score >= 0:
                cpu_candidates.append((score, transform, item))
        if cpu_candidates:
            cpu_candidates.sort(key=lambda row: row[0], reverse=True)
            _, transform, item = cpu_candidates[0]
            direct.append(
                ItemSelection(
                    hostid=hostid,
                    host=host_name,
                    itemid=str(item["itemid"]),
                    key_=item.get("key_", ""),
                    value_type=int(item.get("value_type", 0)),
                    metric="cpu",
                    transform=transform,
                    as_value=as_value,
                )
            )

        ram_direct_candidates: List[Tuple[int, Dict]] = []
        for item in host_items:
            score = ram_direct_score(item)
            if score >= 0:
                ram_direct_candidates.append((score, item))
        if ram_direct_candidates:
            ram_direct_candidates.sort(key=lambda row: row[0], reverse=True)
            item = ram_direct_candidates[0][1]
            direct.append(
                ItemSelection(
                    hostid=hostid,
                    host=host_name,
                    itemid=str(item["itemid"]),
                    key_=item.get("key_", ""),
                    value_type=int(item.get("value_type", 0)),
                    metric="ram",
                    transform="identity",
                    as_value=as_value,
                )
            )
        else:
            total_item: Optional[Dict] = None
            available_item: Optional[Dict] = None
            used_item: Optional[Dict] = None
            for item in host_items:
                key = item.get("key_", "")
                if key == "vm.memory.size[total]":
                    total_item = item
                elif key in ("vm.memory.size[available]", "vm.memory.size[free]"):
                    available_item = item
                elif key == "vm.memory.size[used]":
                    used_item = item

            if total_item is not None and available_item is not None:
                ram_pairs.append(
                    RamPairSelection(
                        hostid=hostid,
                        host=host_name,
                        as_value=as_value,
                        total_itemid=str(total_item["itemid"]),
                        total_value_type=int(total_item.get("value_type", 3)),
                        part_itemid=str(available_item["itemid"]),
                        part_value_type=int(available_item.get("value_type", 3)),
                        mode="available",
                    )
                )
            elif total_item is not None and used_item is not None:
                ram_pairs.append(
                    RamPairSelection(
                        hostid=hostid,
                        host=host_name,
                        as_value=as_value,
                        total_itemid=str(total_item["itemid"]),
                        total_value_type=int(total_item.get("value_type", 3)),
                        part_itemid=str(used_item["itemid"]),
                        part_value_type=int(used_item.get("value_type", 3)),
                        mode="used",
                    )
                )

        disk_candidates: List[Tuple[int, str, Dict]] = []
        for item in host_items:
            score, fs_name = disk_score(item, disk_fs_preferences)
            if score >= 0:
                disk_candidates.append((score, fs_name, item))
        if disk_candidates:
            disk_candidates.sort(key=lambda row: row[0], reverse=True)
            _, _, item = disk_candidates[0]
            direct.append(
                ItemSelection(
                    hostid=hostid,
                    host=host_name,
                    itemid=str(item["itemid"]),
                    key_=item.get("key_", ""),
                    value_type=int(item.get("value_type", 0)),
                    metric="disk",
                    transform="identity",
                    as_value=as_value,
                )
            )

    return direct, ram_pairs


def fetch_history_points(
    api: ZabbixAPI,
    itemid_to_value_type: Dict[str, int],
    time_from: int,
    time_till: int,
    chunk_size: int,
) -> pd.DataFrame:
    if not itemid_to_value_type:
        return pd.DataFrame(columns=["itemid", "clock", "value", "ns"])

    by_type: Dict[int, List[str]] = {}
    for itemid, value_type in itemid_to_value_type.items():
        by_type.setdefault(int(value_type), []).append(str(itemid))

    frames: List[pd.DataFrame] = []
    request_plan: List[Tuple[int, List[str], int, int]] = []
    for value_type, itemids in sorted(by_type.items()):
        for item_chunk in chunked(itemids, chunk_size):
            request_plan.append((value_type, item_chunk, int(time_from), int(time_till)))

    queue = deque(request_plan)
    total_chunks = len(queue)
    completed_chunks = 0
    while queue:
        value_type, item_chunk, window_from, window_till = queue.popleft()
        params = {
            "output": ["itemid", "clock", "ns", "value"],
            "history": value_type,
            "itemids": item_chunk,
            "time_from": window_from,
            "time_till": window_till,
            "sortfield": "clock",
            "sortorder": "ASC",
        }
        try:
            records = api.call("history.get", params)
        except Exception as exc:
            if is_transient_api_error(exc):
                if len(item_chunk) > 1:
                    midpoint = len(item_chunk) // 2
                    left_chunk = item_chunk[:midpoint]
                    right_chunk = item_chunk[midpoint:]
                    if left_chunk and right_chunk:
                        print(
                            f"\nhistory.get: transient failure on chunk size {len(item_chunk)}, "
                            f"splitting into {len(left_chunk)} + {len(right_chunk)}",
                            file=sys.stderr,
                            flush=True,
                        )
                        queue.appendleft((value_type, right_chunk, window_from, window_till))
                        queue.appendleft((value_type, left_chunk, window_from, window_till))
                        total_chunks += 1
                        continue
                split = split_time_window(
                    window_from,
                    window_till,
                    MIN_HISTORY_WINDOW_SECONDS,
                )
                if split is not None:
                    (left_from, left_till), (right_from, right_till) = split
                    print(
                        f"\nhistory.get: transient failure on window {window_from}->{window_till}, "
                        f"splitting into {left_from}->{left_till} and {right_from}->{right_till}",
                        file=sys.stderr,
                        flush=True,
                    )
                    queue.appendleft((value_type, item_chunk, right_from, right_till))
                    queue.appendleft((value_type, item_chunk, left_from, left_till))
                    total_chunks += 1
                    continue
                print(
                    f"\nhistory.get: skipped tiny failing window {window_from}->{window_till} "
                    f"for itemids={len(item_chunk)} ({exc})",
                    file=sys.stderr,
                    flush=True,
                )
                completed_chunks += 1
                progress_bar("history.get", completed_chunks, total_chunks)
                continue
            raise
        if records:
            frame = pd.DataFrame.from_records(records)
            frames.append(frame)
        completed_chunks += 1
        progress_bar("history.get", completed_chunks, total_chunks)

    if not frames:
        return pd.DataFrame(columns=["itemid", "clock", "value", "ns"])

    raw = pd.concat(frames, ignore_index=True)
    raw["itemid"] = raw["itemid"].astype(str)
    raw["clock"] = pd.to_datetime(raw["clock"].astype("int64"), unit="s", utc=True)
    raw["value"] = pd.to_numeric(raw["value"], errors="coerce")
    raw = raw.dropna(subset=["value"])
    return raw


def fetch_trend_points(
    api: ZabbixAPI,
    itemids: Sequence[str],
    time_from: int,
    time_till: int,
    chunk_size: int,
) -> pd.DataFrame:
    if not itemids:
        return pd.DataFrame(
            columns=["itemid", "clock", "num", "value_min", "value_avg", "value_max"]
        )

    frames: List[pd.DataFrame] = []
    unique_itemids = sorted(set(itemids))
    queue = deque(
        (item_chunk, int(time_from), int(time_till))
        for item_chunk in chunked(unique_itemids, chunk_size)
    )
    total_chunks = len(queue)
    completed_chunks = 0
    while queue:
        item_chunk, window_from, window_till = queue.popleft()
        params = {
            "output": ["itemid", "clock", "num", "value_min", "value_avg", "value_max"],
            "itemids": item_chunk,
            "time_from": window_from,
            "time_till": window_till,
            "sortfield": "clock",
            "sortorder": "ASC",
        }
        try:
            records = api.call("trend.get", params)
        except Exception as exc:
            if is_transient_api_error(exc):
                if len(item_chunk) > 1:
                    midpoint = len(item_chunk) // 2
                    left_chunk = item_chunk[:midpoint]
                    right_chunk = item_chunk[midpoint:]
                    if left_chunk and right_chunk:
                        print(
                            f"\ntrend.get: transient failure on chunk size {len(item_chunk)}, "
                            f"splitting into {len(left_chunk)} + {len(right_chunk)}",
                            file=sys.stderr,
                            flush=True,
                        )
                        queue.appendleft((right_chunk, window_from, window_till))
                        queue.appendleft((left_chunk, window_from, window_till))
                        total_chunks += 1
                        continue
                split = split_time_window(
                    window_from,
                    window_till,
                    MIN_TREND_WINDOW_SECONDS,
                )
                if split is not None:
                    (left_from, left_till), (right_from, right_till) = split
                    print(
                        f"\ntrend.get: transient failure on window {window_from}->{window_till}, "
                        f"splitting into {left_from}->{left_till} and {right_from}->{right_till}",
                        file=sys.stderr,
                        flush=True,
                    )
                    queue.appendleft((item_chunk, right_from, right_till))
                    queue.appendleft((item_chunk, left_from, left_till))
                    total_chunks += 1
                    continue
                print(
                    f"\ntrend.get: skipped tiny failing window {window_from}->{window_till} "
                    f"for itemids={len(item_chunk)} ({exc})",
                    file=sys.stderr,
                    flush=True,
                )
                completed_chunks += 1
                progress_bar("trend.get", completed_chunks, total_chunks)
                continue
            raise
        if records:
            frames.append(pd.DataFrame.from_records(records))
        completed_chunks += 1
        progress_bar("trend.get", completed_chunks, total_chunks)

    if not frames:
        return pd.DataFrame(
            columns=["itemid", "clock", "num", "value_min", "value_avg", "value_max"]
        )

    raw = pd.concat(frames, ignore_index=True)
    raw["itemid"] = raw["itemid"].astype(str)
    raw["clock"] = pd.to_datetime(raw["clock"].astype("int64"), unit="s", utc=True)
    for column in ("num", "value_min", "value_avg", "value_max"):
        raw[column] = pd.to_numeric(raw[column], errors="coerce")
    raw = raw.dropna(subset=["value_avg"])
    return raw


def build_direct_history(
    raw_history: pd.DataFrame, selections: Sequence[ItemSelection]
) -> pd.DataFrame:
    if raw_history.empty or not selections:
        return pd.DataFrame(
            columns=["metric", "clock", "hostid", "host", "as_value", "itemid", "utilization_pct"]
        )

    lookup = {selection.itemid: selection for selection in selections}
    frame = raw_history[raw_history["itemid"].isin(lookup.keys())].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=["metric", "clock", "hostid", "host", "as_value", "itemid", "utilization_pct"]
        )

    frame["transform"] = frame["itemid"].map(lambda itemid: lookup[itemid].transform)
    frame["metric"] = frame["itemid"].map(lambda itemid: lookup[itemid].metric)
    frame["hostid"] = frame["itemid"].map(lambda itemid: lookup[itemid].hostid)
    frame["host"] = frame["itemid"].map(lambda itemid: lookup[itemid].host)
    frame["as_value"] = frame["itemid"].map(lambda itemid: lookup[itemid].as_value)

    frame["utilization_pct"] = np.where(
        frame["transform"] == "invert_100", 100.0 - frame["value"], frame["value"]
    )
    frame["utilization_pct"] = frame["utilization_pct"].clip(lower=0.0, upper=100.0)

    return frame[
        ["metric", "clock", "hostid", "host", "as_value", "itemid", "utilization_pct"]
    ].copy()


def build_direct_trend(
    raw_trend: pd.DataFrame, selections: Sequence[ItemSelection]
) -> pd.DataFrame:
    if raw_trend.empty or not selections:
        return pd.DataFrame(
            columns=[
                "metric",
                "clock",
                "hostid",
                "host",
                "as_value",
                "itemid",
                "num",
                "util_min",
                "util_avg",
                "util_max",
            ]
        )

    lookup = {selection.itemid: selection for selection in selections}
    frame = raw_trend[raw_trend["itemid"].isin(lookup.keys())].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "metric",
                "clock",
                "hostid",
                "host",
                "as_value",
                "itemid",
                "num",
                "util_min",
                "util_avg",
                "util_max",
            ]
        )

    frame["transform"] = frame["itemid"].map(lambda itemid: lookup[itemid].transform)
    frame["metric"] = frame["itemid"].map(lambda itemid: lookup[itemid].metric)
    frame["hostid"] = frame["itemid"].map(lambda itemid: lookup[itemid].hostid)
    frame["host"] = frame["itemid"].map(lambda itemid: lookup[itemid].host)
    frame["as_value"] = frame["itemid"].map(lambda itemid: lookup[itemid].as_value)

    inverted = frame["transform"] == "invert_100"
    frame["util_avg"] = np.where(inverted, 100.0 - frame["value_avg"], frame["value_avg"])
    frame["util_min"] = np.where(inverted, 100.0 - frame["value_max"], frame["value_min"])
    frame["util_max"] = np.where(inverted, 100.0 - frame["value_min"], frame["value_max"])

    frame["util_avg"] = frame["util_avg"].clip(lower=0.0, upper=100.0)
    frame["util_min"] = frame["util_min"].clip(lower=0.0, upper=100.0)
    frame["util_max"] = frame["util_max"].clip(lower=0.0, upper=100.0)

    return frame[
        [
            "metric",
            "clock",
            "hostid",
            "host",
            "as_value",
            "itemid",
            "num",
            "util_min",
            "util_avg",
            "util_max",
        ]
    ].copy()


def build_ram_pair_history(
    raw_history: pd.DataFrame, ram_pairs: Sequence[RamPairSelection]
) -> pd.DataFrame:
    if raw_history.empty or not ram_pairs:
        return pd.DataFrame(
            columns=["metric", "clock", "hostid", "host", "as_value", "itemid", "utilization_pct"]
        )

    rows: List[pd.DataFrame] = []
    for pair in ram_pairs:
        total = raw_history[raw_history["itemid"] == pair.total_itemid][["clock", "value"]].copy()
        part = raw_history[raw_history["itemid"] == pair.part_itemid][["clock", "value"]].copy()
        if total.empty or part.empty:
            continue
        total = total.sort_values("clock").rename(columns={"value": "total_value"})
        part = part.sort_values("clock").rename(columns={"value": "part_value"})

        merged = pd.merge_asof(
            part,
            total,
            on="clock",
            direction="nearest",
            tolerance=pd.Timedelta(minutes=5),
        )
        merged = merged.dropna(subset=["part_value", "total_value"])
        merged = merged[merged["total_value"] > 0]
        if merged.empty:
            continue

        if pair.mode == "available":
            util = (1.0 - merged["part_value"] / merged["total_value"]) * 100.0
        else:
            util = (merged["part_value"] / merged["total_value"]) * 100.0

        data = pd.DataFrame(
            {
                "metric": "ram",
                "clock": merged["clock"],
                "hostid": pair.hostid,
                "host": pair.host,
                "as_value": pair.as_value,
                "itemid": pair.part_itemid,
                "utilization_pct": util.clip(lower=0.0, upper=100.0),
            }
        )
        rows.append(data)

    if not rows:
        return pd.DataFrame(
            columns=["metric", "clock", "hostid", "host", "as_value", "itemid", "utilization_pct"]
        )

    return pd.concat(rows, ignore_index=True)


def build_ram_pair_trend(
    raw_trend: pd.DataFrame, ram_pairs: Sequence[RamPairSelection]
) -> pd.DataFrame:
    if raw_trend.empty or not ram_pairs:
        return pd.DataFrame(
            columns=[
                "metric",
                "clock",
                "hostid",
                "host",
                "as_value",
                "itemid",
                "num",
                "util_min",
                "util_avg",
                "util_max",
            ]
        )

    rows: List[pd.DataFrame] = []
    for pair in ram_pairs:
        total = raw_trend[raw_trend["itemid"] == pair.total_itemid][
            ["clock", "num", "value_min", "value_avg", "value_max"]
        ].copy()
        part = raw_trend[raw_trend["itemid"] == pair.part_itemid][
            ["clock", "num", "value_min", "value_avg", "value_max"]
        ].copy()
        if total.empty or part.empty:
            continue

        total = total.rename(
            columns={
                "num": "total_num",
                "value_min": "total_min",
                "value_avg": "total_avg",
                "value_max": "total_max",
            }
        )
        part = part.rename(
            columns={
                "num": "part_num",
                "value_min": "part_min",
                "value_avg": "part_avg",
                "value_max": "part_max",
            }
        )
        merged = pd.merge(part, total, on="clock", how="inner")
        merged = merged[merged["total_avg"] > 0]
        merged = merged[merged["total_min"] > 0]
        merged = merged[merged["total_max"] > 0]
        if merged.empty:
            continue

        if pair.mode == "available":
            util_avg = (1.0 - merged["part_avg"] / merged["total_avg"]) * 100.0
            util_min = (1.0 - merged["part_max"] / merged["total_min"]) * 100.0
            util_max = (1.0 - merged["part_min"] / merged["total_max"]) * 100.0
        else:
            util_avg = (merged["part_avg"] / merged["total_avg"]) * 100.0
            util_min = (merged["part_min"] / merged["total_max"]) * 100.0
            util_max = (merged["part_max"] / merged["total_min"]) * 100.0

        data = pd.DataFrame(
            {
                "metric": "ram",
                "clock": merged["clock"],
                "hostid": pair.hostid,
                "host": pair.host,
                "as_value": pair.as_value,
                "itemid": pair.part_itemid,
                "num": np.minimum(merged["part_num"], merged["total_num"]),
                "util_min": util_min.clip(lower=0.0, upper=100.0),
                "util_avg": util_avg.clip(lower=0.0, upper=100.0),
                "util_max": util_max.clip(lower=0.0, upper=100.0),
            }
        )
        rows.append(data)

    if not rows:
        return pd.DataFrame(
            columns=[
                "metric",
                "clock",
                "hostid",
                "host",
                "as_value",
                "itemid",
                "num",
                "util_min",
                "util_avg",
                "util_max",
            ]
        )
    return pd.concat(rows, ignore_index=True)


def summarize_history(data: pd.DataFrame, by_as: bool) -> pd.DataFrame:
    if data.empty:
        columns = ["metric", "clock", "host_count", "util_mean", "util_median", "util_min", "util_max", "util_p10", "util_p90"]
        if by_as:
            columns.insert(1, "as_value")
        return pd.DataFrame(columns=columns)

    group_cols = ["metric", "clock"]
    if by_as:
        group_cols = ["metric", "as_value", "clock"]

    grouped = data.groupby(group_cols)["utilization_pct"]
    summary = grouped.agg(
        host_count="count",
        util_mean="mean",
        util_median="median",
        util_min="min",
        util_max="max",
    )
    summary = summary.join(grouped.quantile(0.10).rename("util_p10"))
    summary = summary.join(grouped.quantile(0.90).rename("util_p90"))
    return summary.reset_index()


def summarize_trend(data: pd.DataFrame, by_as: bool) -> pd.DataFrame:
    if data.empty:
        columns = [
            "metric",
            "clock",
            "host_count",
            "util_avg_mean",
            "util_avg_median",
            "util_avg_p10",
            "util_avg_p90",
            "util_min",
            "util_max",
        ]
        if by_as:
            columns.insert(1, "as_value")
        return pd.DataFrame(columns=columns)

    group_cols = ["metric", "clock"]
    if by_as:
        group_cols = ["metric", "as_value", "clock"]

    grouped = data.groupby(group_cols)
    summary = grouped.agg(
        host_count=("hostid", "nunique"),
        util_avg_mean=("util_avg", "mean"),
        util_avg_median=("util_avg", "median"),
        util_min=("util_min", "min"),
        util_max=("util_max", "max"),
    )
    summary = summary.join(grouped["util_avg"].quantile(0.10).rename("util_avg_p10"))
    summary = summary.join(grouped["util_avg"].quantile(0.90).rename("util_avg_p90"))
    return summary.reset_index()


def design_matrix(t: np.ndarray) -> np.ndarray:
    weekly = 2.0 * math.pi * t / 7.0
    yearly = 2.0 * math.pi * t / 365.0
    return np.column_stack(
        [
            np.ones_like(t),
            t,
            np.sin(weekly),
            np.cos(weekly),
            np.sin(yearly),
            np.cos(yearly),
        ]
    )


def build_forecast(summary_trend_all: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    if summary_trend_all.empty:
        return pd.DataFrame(
            columns=["metric", "timestamp", "is_future", "actual", "fitted", "predicted", "lower", "upper"]
        )

    for metric, metric_df in summary_trend_all.groupby("metric"):
        daily = (
            metric_df.sort_values("clock")
            .set_index("clock")["util_avg_mean"]
            .resample("1D")
            .mean()
            .interpolate(method="time")
            .dropna()
        )
        if len(daily) < 30:
            continue

        y = daily.to_numpy(dtype=float)
        t = np.arange(len(y), dtype=float)
        x = design_matrix(t)
        beta, *_ = np.linalg.lstsq(x, y, rcond=None)
        fitted = x @ beta

        dof = max(1, len(y) - x.shape[1])
        sigma = float(np.sqrt(np.sum((y - fitted) ** 2) / dof))
        interval = 1.96 * sigma

        hist = pd.DataFrame(
            {
                "metric": metric,
                "timestamp": daily.index,
                "is_future": False,
                "actual": np.clip(y, 0.0, 100.0),
                "fitted": np.clip(fitted, 0.0, 100.0),
                "predicted": np.clip(fitted, 0.0, 100.0),
                "lower": np.nan,
                "upper": np.nan,
            }
        )
        rows.append(hist)

        if horizon_days <= 0:
            continue
        future_index = pd.date_range(
            start=daily.index[-1] + pd.Timedelta(days=1),
            periods=horizon_days,
            freq="1D",
            tz="UTC",
        )
        t_future = np.arange(len(y), len(y) + horizon_days, dtype=float)
        x_future = design_matrix(t_future)
        pred = x_future @ beta

        future = pd.DataFrame(
            {
                "metric": metric,
                "timestamp": future_index,
                "is_future": True,
                "actual": np.nan,
                "fitted": np.nan,
                "predicted": np.clip(pred, 0.0, 100.0),
                "lower": np.clip(pred - interval, 0.0, 100.0),
                "upper": np.clip(pred + interval, 0.0, 100.0),
            }
        )
        rows.append(future)

    if not rows:
        return pd.DataFrame(
            columns=["metric", "timestamp", "is_future", "actual", "fitted", "predicted", "lower", "upper"]
        )
    return pd.concat(rows, ignore_index=True)
