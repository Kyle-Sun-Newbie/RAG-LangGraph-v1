# app/nodes/normalize_time_agent.py
# -*- coding: utf-8 -*-
"""
LangGraph 节点：时间归一化（Normalize Time Range）

作用：
- 读取 rag_agent 解析出的 hints["time_range"]
- 将自然语言时间结构化为统一的 [start_local, end_local)
- 写入 state["time_window"]
"""

from __future__ import annotations
from datetime import datetime, timedelta
from dateutil import tz
from typing import Dict, Tuple, Any

DEFAULT_TZ = "Asia/Shanghai"


# ========== 工具函数 ==========
def _now_local(tz_name: str) -> datetime:
    return datetime.now(tz.gettz(tz_name))


def _start_of_day(dt: datetime) -> datetime:
    """归零为当天 00:00"""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_dt(s: str, tz_name: str) -> datetime:
    """解析 'YYYY-MM-DD' 或 'YYYY-MM-DDTHH:MM'"""
    if not s:
        raise ValueError("空字符串无法解析时间")
    fmt_list = ["%Y-%m-%dT%H:%M", "%Y-%m-%d"]
    for fmt in fmt_list:
        try:
            dt_naive = datetime.strptime(s, fmt)
            return dt_naive.replace(tzinfo=tz.gettz(tz_name))
        except ValueError:
            pass
    raise ValueError(f"无法解析时间字符串: {s}")


# ========== 时间窗口生成 ==========
def _window_relative_days(now_local: datetime, days_ago: int) -> Tuple[datetime, datetime, str]:
    base = _start_of_day(now_local) - timedelta(days=days_ago)
    start = base
    end = base + timedelta(days=1)
    label = "昨天" if days_ago == 1 else f"{days_ago}天前"
    return start, end, label


def _window_last_hours(now_local: datetime, hours: int) -> Tuple[datetime, datetime, str]:
    end = now_local
    start = end - timedelta(hours=hours)
    label = f"最近{hours}小时"
    return start, end, label


def _window_absolute(start_str: str, end_str: str, tz_name: str) -> Tuple[datetime, datetime, str]:
    start = _parse_dt(start_str, tz_name)
    end = _parse_dt(end_str, tz_name)
    label = f"{start_str} ~ {end_str}"
    return start, end, label


def _window_point_in_time(at_str: str, tz_name: str) -> Tuple[datetime, datetime, str]:
    at = _parse_dt(at_str, tz_name)
    start = at
    end = at + timedelta(hours=1)
    label = f"{at_str}"
    return start, end, label


# ========== 主逻辑 ==========
def normalize_time(hints: Dict[str, Any], tz_name: str = DEFAULT_TZ) -> Dict[str, Any]:
    """
    根据 hints["time_range"] 生成统一时间窗口。
    如果用户根本没问时间，就不要捏造“昨天”。
    """
    now_local = _now_local(tz_name)
    tr = hints.get("time_range") or {}

    try:
        kind = tr.get("kind")

        if kind == "relative_days":
            days_ago = int(tr.get("days_ago", 1))
            start, end, label = _window_relative_days(now_local, days_ago)

            return {
                "start_local": start.isoformat(),
                "end_local": end.isoformat(),
                "label": label,
                "ok": True,
                "error": None,
            }

        elif kind == "last_hours":
            hours = int(tr.get("hours", 6))
            start, end, label = _window_last_hours(now_local, max(1, hours))

            return {
                "start_local": start.isoformat(),
                "end_local": end.isoformat(),
                "label": label,
                "ok": True,
                "error": None,
            }

        elif kind == "absolute":
            start, end, label = _window_absolute(tr.get("start", ""), tr.get("end", ""), tz_name)

            return {
                "start_local": start.isoformat(),
                "end_local": end.isoformat(),
                "label": label,
                "ok": True,
                "error": None,
            }

        elif kind == "point_in_time":
            start, end, label = _window_point_in_time(tr.get("at", ""), tz_name)

            return {
                "start_local": start.isoformat(),
                "end_local": end.isoformat(),
                "label": label,
                "ok": True,
                "error": None,
            }

        else:
            # ✅ 用户没提时间，老老实实说“无时间限定”，不要编时间
            return {
                "start_local": None,
                "end_local": None,
                "label": "（无时间限定）",
                "ok": True,
                "error": None,
            }

    except Exception as e:
        # ✅ 出错也不要再假装“昨天”，直接把错误暴露给前端
        return {
            "start_local": None,
            "end_local": None,
            "label": "（时间解析失败）",
            "ok": False,
            "error": f"时间解析失败: {e}",
        }


# ========== LangGraph 节点入口 ==========
def node_normalize_time(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph 节点：归一化时间窗口"""
    hints = state.get("hints", {}) or {}
    result = normalize_time(hints, tz_name=DEFAULT_TZ)

    state["time_window"] = result
    trace = list(state.get("trace", []))
    trace.append("normalize_time")
    state["trace"] = trace
    return state
