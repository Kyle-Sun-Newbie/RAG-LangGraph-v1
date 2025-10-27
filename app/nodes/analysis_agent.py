# app/nodes/analysis_agent.py
from __future__ import annotations
from pathlib import Path
from typing import Iterable, List, Dict, Optional, Sequence, Tuple, Any
from datetime import datetime, timedelta
import pandas as pd, numpy as np
from dateutil import tz

ABS = Path(r"F:\Task\RAG-LangGraph-Demo\data\timeseries.csv")
REL = Path(__file__).resolve().parents[2] / "data" / "timeseries.csv"
CSV_PATH = ABS if ABS.exists() else REL

DEFAULT_TZ = "Asia/Shanghai"

def _metric_from_tsid(tsid: str) -> Tuple[str, str, str]:
    t = (tsid or "").lower()
    if ".temp" in t:
        return ("temperature", "温度", "°C")
    if ".rh" in t:
        return ("humidity", "湿度", "%RH")
    if ".lux" in t:
        return ("illuminance", "光照强度", "lux")
    if ".co2" in t:
        return ("co2", "二氧化碳浓度", "ppm")
    if ".pm25" in t or ".pm2.5" in t:
        # 生成脚本里 ts_id 用的是 ".pm25"
        return ("pm25", "PM2.5 浓度", "µg/m³")
    return ("value", "数值", "")


def _load_df() -> pd.DataFrame:
    """
    读取 timeseries.csv，并确保：
    - tsid: str
    - timestamp: pandas.Timestamp(UTC)
    - value: float
    """
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"未找到 timeseries.csv: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    # 列名清洗 + 兼容 ts_id
    df.columns = [str(c).strip() for c in df.columns]
    df = df.rename(columns={"ts_id": "tsid"})

    if "tsid" not in df.columns:
        raise ValueError("timeseries.csv 缺少列：tsid")
    if "timestamp" not in df.columns:
        raise ValueError("timeseries.csv 缺少列：timestamp")
    if "value" not in df.columns:
        raise ValueError("timeseries.csv 缺少列：value")

    df["tsid"] = df["tsid"].astype(str).str.strip()

    # 若时间无时区，按本地时区理解后转 UTC；若已有时区，直接转 UTC
    ts = pd.to_datetime(df["timestamp"], utc=False, errors="coerce")
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize(DEFAULT_TZ)
    df["timestamp"] = ts.dt.tz_convert("UTC")

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _trend(vals: pd.Series) -> Optional[str]:
    """
    简单线性拟合斜率判断趋势：
    > 0.02 上升
    < -0.02 下降
    其他 基本稳定
    """
    if vals.size < 3:
        return None
    x = np.arange(vals.size)
    slope = float(np.polyfit(x, vals.values.astype(float), 1)[0])
    if slope > 0.02:
        return "上升"
    if slope < -0.02:
        return "下降"
    return "基本稳定"


def _to_utc(dt_like, tz_name: str) -> pd.Timestamp:
    """
    安全的本地→UTC 转换：
    - 若 dt 本身不带时区：按 tz_name 本地化，再 tz_convert('UTC')
    - 若 dt 已带时区：直接 tz_convert('UTC')
    - 传入 datetime 或 pandas.Timestamp 均可
    """
    ts = pd.Timestamp(dt_like)
    if ts.tz is None:
        ts = ts.tz_localize(tz_name)
    return ts.tz_convert("UTC")

# ===== 指标计算注册表 =====
def _stat_avg(vals: pd.Series) -> float | None:
    if vals.empty:
        return None
    return float(vals.mean())

def _stat_max(vals: pd.Series) -> float | None:
    if vals.empty:
        return None
    return float(vals.max())

def _stat_min(vals: pd.Series) -> float | None:
    if vals.empty:
        return None
    return float(vals.min())

def _stat_trend(vals: pd.Series) -> str | None:
    if vals.empty:
        return None
    return _trend(vals)

# 注册表：LLM 可以请求这些指标名
_METRIC_REGISTRY = {
    "avg":   _stat_avg,
    "max":   _stat_max,
    "min":   _stat_min,
    "trend": _stat_trend,
}

def analyze(
    tsids: Iterable[str] | str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    need: Sequence[str] = ("avg", "max", "min", "trend"),
    tz_name: str = DEFAULT_TZ,
    label: Optional[str] = None,
) -> List[Dict]:
    """
    对一批 tsid 在 [start, end) 窗口内做统计。

    升级点：
    - 允许 LLM（hints.need）指定任意一组我们支持的指标名，而不是死锁 avg/max/min/trend。
    - 每条结果都带诊断信息，不会因为缺数据而直接报错。
    - 如果指标计算崩了，不影响主流程，会把错误塞进 _diag_errors。
    """

    # 统一 tsids 成 list[str]
    tsids = [str(tsids)] if isinstance(tsids, (str, int)) else [str(t) for t in tsids]

    df = _load_df()
    tsid_set = set(df["tsid"])

    results: List[Dict[str, Any]] = []

    # LLM/上游想要哪些统计指标？
    want_raw = [str(x).strip().lower() for x in (need or [])]
    # 过滤到我们真的支持的指标，避免 LLM 乱写导致崩
    want = [w for w in want_raw if w in _METRIC_REGISTRY]

    # === 情况1：时间窗口缺失，没法切片 ===
    if start is None or end is None:
        for tsid in tsids:
            metric, metric_zh, unit = _metric_from_tsid(tsid)
            item: Dict[str, Any] = {
                "tsid": tsid,
                "span": label or "",
                "metric": metric,
                "metric_zh": metric_zh,
                "unit": unit,
                "n": 0,
                "_diag": {
                    "reason": "no_time_window",
                    "csv_path": str(CSV_PATH),
                    "tsid_in_csv": (tsid in tsid_set),
                },
            }
            # 给每个请求的指标名占位
            for w in want:
                item[w] = None
            results.append(item)
        return results

    # === 情况2：有窗口，正常分析 ===
    start_utc = _to_utc(start, tz_name)
    end_utc = _to_utc(end, tz_name)

    for tsid in tsids:
        metric, metric_zh, unit = _metric_from_tsid(tsid)

        # 情况2a：CSV 中没有这个 tsid
        if tsid not in tsid_set:
            item: Dict[str, Any] = {
                "tsid": tsid,
                "span": label or "",
                "metric": metric,
                "metric_zh": metric_zh,
                "unit": unit,
                "n": 0,
                "_diag": {
                    "reason": "tsid_not_found_in_csv",
                    "csv_path": str(CSV_PATH),
                    "window_utc": [str(start_utc), str(end_utc)],
                    "window_local": [
                        str(pd.Timestamp(start_utc).tz_convert(tz_name)),
                        str(pd.Timestamp(end_utc).tz_convert(tz_name)),
                    ],
                    "samples": 0,
                }
            }
            for w in want:
                item[w] = None
            results.append(item)
            continue

        # 情况2b：CSV 里有 tsid，取窗口内的值
        mask = (
            (df["tsid"] == tsid) &
            (df["timestamp"] >= start_utc) &
            (df["timestamp"] < end_utc)
        )
        sub = df.loc[mask, ["timestamp", "value"]].dropna()
        vals = sub["value"]

        item: Dict[str, Any] = {
            "tsid": tsid,
            "span": label or "",
            "metric": metric,
            "metric_zh": metric_zh,
            "unit": unit,
            "n": int(vals.size),
            "_diag": {
                "csv_path": str(CSV_PATH),
                "window_utc": [str(start_utc), str(end_utc)],
                "window_local": [
                    str(pd.Timestamp(start_utc).tz_convert(tz_name)),
                    str(pd.Timestamp(end_utc).tz_convert(tz_name)),
                ],
                "first_ts": str(sub["timestamp"].min()) if not sub.empty else None,
                "last_ts":  str(sub["timestamp"].max()) if not sub.empty else None,
                "samples": int(sub.shape[0]),
            },
        }

        # 动态尝试计算每个指标
        for w in want:
            func = _METRIC_REGISTRY.get(w)
            try:
                item[w] = func(vals) if callable(func) else None
            except Exception as e:
                item[w] = None
                # 把单项报错记到 _diag_errors，不让整个 flow 崩
                item.setdefault("_diag_errors", {})[w] = f"{type(e).__name__}: {e}"

        results.append(item)

    return results

def analyze_state(state: Dict) -> List[Dict]:
    """
    LangGraph 风格入口：
    - 从 state["rows"] 里拿 tsid 列表（这些来自 SPARQL 执行）
    - 从 state["time_window"] 里拿最终已经归一化的时间窗口
      （time_window 是 normalize_time_agent.node_normalize_time 写入的）
    - 从 state["hints"]["need"] 里拿用户关心的统计指标
    - 返回统计结果列表
    """
    rows = state.get("rows", []) or []
    tsids = [r.get("tsid") for r in rows if isinstance(r, dict) and r.get("tsid")]

    # 时间窗口
    tw = state.get("time_window") or {}
    start_local_iso = tw.get("start_local")
    end_local_iso = tw.get("end_local")
    label = tw.get("label", "")

    # 解析成 tz-aware 本地时间（pandas 会保留时区信息）
    start_local = pd.to_datetime(start_local_iso) if start_local_iso else None
    end_local = pd.to_datetime(end_local_iso) if end_local_iso else None

    # 用户要求哪些统计？
    hints = state.get("hints", {}) or {}
    need = hints.get("need") or ("avg", "max", "min", "trend")

    # 分析并返回
    return analyze(
        tsids or [],
        start=start_local.to_pydatetime() if start_local is not None else None,
        end=end_local.to_pydatetime() if end_local is not None else None,
        need=need,
        tz_name=DEFAULT_TZ,
        label=label,
    )


# —— 调试用：单点体检 —— #
def quick_probe(tsid: str, hours: int = 24, tz_name: str = DEFAULT_TZ):
    """
    手动探查某个 tsid 最近 N 小时，用于线下排查数据问题。
    不影响主流程。
    """
    zone = tz.gettz(tz_name)
    now = datetime.now(zone)
    end_local = now
    start_local = end_local - timedelta(hours=hours)

    start_utc = _to_utc(start_local, tz_name)
    end_utc = _to_utc(end_local, tz_name)

    df = _load_df()
    exists = tsid in set(df["tsid"])
    mask = (
        (df["tsid"] == tsid) &
        (df["timestamp"] >= start_utc) &
        (df["timestamp"] < end_utc)
    )
    sub = df.loc[mask, ["timestamp", "value"]].dropna()

    print("CSV_PATH:", str(CSV_PATH))
    print("TSID exists in CSV:", exists)
    print("Probe window (local):", start_local, "→", end_local)
    print("Probe window (UTC):  ", start_utc, "→", end_utc)
    print("Samples:", sub.shape[0])
    if not sub.empty:
        print("First ts:", sub["timestamp"].min(), "Last ts:", sub["timestamp"].max())
        print(
            "Mean:", float(sub["value"].mean()),
            "Min:",  float(sub["value"].min()),
            "Max:",  float(sub["value"].max()),
        )

def analyze_point_in_time_state(state: Dict) -> List[Dict]:
    """
    专门处理精确时间点的分析 - 修复单个数据点的数值提取
    """
    rows = state.get("rows", []) or []
    tsids = [r.get("tsid") for r in rows if isinstance(r, dict) and r.get("tsid")]

    # 获取时间窗口
    tw = state.get("time_window") or {}
    target_time_iso = tw.get("start_local")

    if not target_time_iso:
        return [{
            "tsid": tsid,
            "error": "没有目标时间",
            "n": 0
        } for tsid in tsids]

    # 解析目标时间
    target_local = pd.to_datetime(target_time_iso)
    target_utc = _to_utc(target_local.to_pydatetime(), DEFAULT_TZ)

    df = _load_df()
    results = []

    for tsid in tsids:
        metric, metric_zh, unit = _metric_from_tsid(tsid)

        # 检查TSID是否存在
        if tsid not in set(df["tsid"]):
            results.append({
                "tsid": tsid,
                "metric": metric,
                "metric_zh": metric_zh,
                "unit": unit,
                "value": None,
                "n": 0,
                "_diag": {"reason": "tsid_not_found"}
            })
            continue

        # 直接查找该精确时间点的数据
        mask = (
                (df["tsid"] == tsid) &
                (df["timestamp"] == target_utc)
        )
        exact_match = df.loc[mask]

        if not exact_match.empty:
            # 找到精确匹配 - 修复：直接提取数值
            match_row = exact_match.iloc[0]
            value = float(match_row['value'])

            results.append({
                "tsid": tsid,
                "metric": metric,
                "metric_zh": metric_zh,
                "unit": unit,
                "value": value,  # 直接提供数值
                "avg": value,  # 单个点的情况下，平均值就是该值
                "max": value,  # 最大值也是该值
                "min": value,  # 最小值也是该值
                "n": 1,
                "_diag": {
                    "reason": "exact_match",
                    "target_time": str(target_local),
                    "actual_time": str(match_row['timestamp'])
                }
            })
        else:
            # 没有精确匹配，尝试在1小时窗口内查找（使用原有时窗逻辑）
            start_utc = target_utc
            end_utc = target_utc + timedelta(hours=0.05)

            mask = (
                    (df["tsid"] == tsid) &
                    (df["timestamp"] >= start_utc) &
                    (df["timestamp"] < end_utc)
            )
            window_data = df.loc[mask, ["timestamp", "value"]].dropna()

            if not window_data.empty:
                # 在时间窗口内有数据
                vals = window_data["value"]
                results.append({
                    "tsid": tsid,
                    "metric": metric,
                    "metric_zh": metric_zh,
                    "unit": unit,
                    "value": float(vals.iloc[0]),  # 取第一个值
                    "avg": float(vals.mean()),
                    "max": float(vals.max()),
                    "min": float(vals.min()),
                    "n": len(vals),
                    "_diag": {
                        "reason": "window_match",
                        "target_time": str(target_local),
                        "window_data_points": len(vals),
                        "first_timestamp": str(window_data["timestamp"].iloc[0])
                    }
                })
            else:
                # 完全没数据
                results.append({
                    "tsid": tsid,
                    "metric": metric,
                    "metric_zh": metric_zh,
                    "unit": unit,
                    "value": None,
                    "avg": None,
                    "max": None,
                    "min": None,
                    "n": 0,
                    "_diag": {
                        "reason": "no_data_in_window",
                        "target_time": str(target_local),
                        "window": f"{start_utc} to {end_utc}"
                    }
                })

    return results