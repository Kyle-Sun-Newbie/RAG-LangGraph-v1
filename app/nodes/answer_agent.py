# app/nodes/answer_agent.py
from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
from functools import lru_cache
import os, json, re

# ============== Prompt 文件（只从磁盘读取；不存在就报错） ==============
ABS_PROMPT = Path(r"F:\Task\RAG-LangGraph-Demo\prompt\prompt-answer.md")
REL_PROMPT = Path(__file__).resolve().parents[2] / "prompt" / "prompt-answer.md"

@lru_cache(maxsize=2)
def _load_prompt_text() -> str:
    if ABS_PROMPT.exists():
        return ABS_PROMPT.read_text(encoding="utf-8")
    if REL_PROMPT.exists():
        return REL_PROMPT.read_text(encoding="utf-8")
    raise RuntimeError(f"找不到回答用的 Prompt 文件：{ABS_PROMPT} 或 {REL_PROMPT}")

def _extract_lang(md: str, prefer_zh: bool) -> str:
    import re as _re
    target = "zh" if prefer_zh else "en"
    parts = _re.split(r"(?im)^##\s+", md.strip())
    for part in parts[1:]:
        head, _, body = part.partition("\n")
        if head.strip().lower().startswith(target):
            return body.strip()
    return md.strip()

# ============== 与 rag_agent 相同风格的 LLM 懒加载 ==============
_LLM = None
def _get_llm():
    global _LLM
    if _LLM is not None:
        return _LLM
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("未发现 DEEPSEEK_API_KEY")
    from langchain.chat_models import init_chat_model
    _LLM = init_chat_model("deepseek:deepseek-chat", temperature=0, api_key=api_key)
    return _LLM

# ============== 清洗工具 ==============
def _is_zh(text: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in (text or ""))

def _short(s: str) -> str:
    s = str(s or "").strip()
    if "#" in s:
        s = s.rsplit("#", 1)[-1]
    if "/" in s:
        s = s.rstrip("/").rsplit("/", 1)[-1]
    return s

def _clean_rows(
    rows: List[Dict[str, Any]],
    limit: int = 50,
    query_type: str | None = None
) -> List[Dict[str, Any]]:
    """
    通用 rows 清洗：
    - 对于时序类问题：保留房间、点位、传感器类型等核心字段（并做截断，防止几千行 tsid 炸前端和 LLM）。
    - 对于拓扑类问题（count_rooms / list_rooms / 哪些房间有PM2.5）：直接原样保留全集，不截断。
    """
    if not rows:
        return []

    if query_type == "topology":
        return rows

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "room": _short(r.get("room") or r.get("Room") or ""),
            "name": _short(r.get("pt") or r.get("sensor") or r.get("point") or ""),
            "type": _short(r.get("ptType") or r.get("type") or r.get("pttype") or ""),
            "tsid": (
                str(r.get("tsid") or r.get("ts_id"))
                if (r.get("tsid") or r.get("ts_id"))
                else ""
            ),
        })
        if len(out) >= limit:
            break
    return out


def _clean_analysis(analysis: Any, limit: int = 20) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for item in (analysis or []):
        if not isinstance(item, dict):
            continue
        cleaned.append({
            "tsid": item.get("tsid"),
            "span": item.get("span"),
            "metric_zh": item.get("metric_zh"),
            "unit": item.get("unit"),
            "n": item.get("n"),
            "value": item.get("value"),  # 确保包含直接数值
            "avg": item.get("avg"),
            "max": item.get("max"),
            "min": item.get("min"),
            "trend": item.get("trend"),
        })
        if len(cleaned) >= limit:
            break
    return cleaned

def _clean_time_window(time_window: Dict[str, Any] | None) -> Dict[str, Any]:
    tw = time_window or {}
    start_local = str(tw.get("start_local") or "")
    end_local = str(tw.get("end_local") or "")
    def _just_date(iso_str: str) -> str:
        m = re.match(r"^(\d{4}-\d{2}-\d{2})", iso_str.strip())
        return m.group(1) if m else ""
    return {
        "label": tw.get("label"),
        "start_local_date": _just_date(start_local),
        "end_local_date": _just_date(end_local),
    }

# ============== Prompt 拼装增强 ==============
def _augment_prompt(base_prompt: str, prefer_zh: bool, question_type: str | None = None) -> str:
    """
    在原 prompt 后追加约束说明：
    - 对 timeseries：用真实数值回答，不编造日期
    - 对 topology：允许基于全集 vs 子集做集合推理，但禁止夸张绝对化措辞
    """
    if prefer_zh:
        patch_ts = """
【补充要求】：
1. 若我提供 analysis 且 n>0，说明确实有数据，请引用这些数值回答；
   不要说“无数据”或“无法计算”。
2. 我会提供 time_window.clean_dates，若要提及具体日期，请使用我提供的日期。
3. 仅当所有 n==0 时可说“该时间段没有有效数据”。"""
        patch_topo = """
【拓扑类问题特别说明】：
1. 你正在回答楼宇/房间/传感器的结构性问题。
2. 若 rows 含有房间或传感器列表，请用“查询结果显示…”、“我查询到…”等中性表述。
3. 避免使用“只有这些房间”、“全部传感器都…”、“没有其他”这类绝对化措辞，除非我提供了完整全集。
4. 如果 rows 里给的是计数（例如房间数量），你可以直接说“共有 X 个房间实体”。
5. 如果我同时提供：
   - topology_all_rooms: 全部房间（全集）
   - rows: 符合某条件的房间（子集）
   你可以基于它们做集合推理（例如推断“哪些房间没有这种传感器”），并把结果清楚地描述出来。"""
    else:
        patch_ts = """
[Additional rules for timeseries]:
1. Use provided numeric values from analysis if n>0.
2. Use given calendar dates from time_window.clean_dates, do not invent dates.
3. Only if all n==0 may you say "no valid data in this period"."""
        patch_topo = """
[Special rules for topology questions]:
1. You are answering a structural/topological question about rooms/sensors.
2. If `rows` contains a list of rooms or sensors, describe it neutrally ("the query returned ...").
3. Avoid absolute terms like "only", "all", "none" unless I explicitly gave you the full universe.
4. If `rows` contains an aggregate count, you may say "there are X rooms in total".
5. If I provide BOTH:
   - `topology_all_rooms`: full list of rooms (universe)
   - `rows`: rooms matching some condition (subset)
   you MAY reason about the complement (e.g. rooms without that sensor) and present that."""
    if question_type == "topology":
        patch = patch_topo
    else:
        patch = patch_ts
    return base_prompt.strip() + "\n\n" + patch.strip()

# ============== 主函数 ==============
def compose(
    user_query: str,
    rows: List[Dict[str, Any]] | None = None,
    analysis: Any | None = None,
    hints: Dict[str, Any] | None = None,
    time_window: Dict[str, Any] | None = None,
    topology_all_rooms: List[Dict[str, Any]] | None = None,
) -> str:
    rows = rows or []
    analysis = analysis or []
    hints = hints or {}
    time_window = time_window or {}

    zh = _is_zh(user_query)
    question_type = hints.get("question_type")

    # 1. 基础 prompt + 任务特化补丁
    base_prompt = _extract_lang(_load_prompt_text(), prefer_zh=zh)
    system_prompt = _augment_prompt(base_prompt, prefer_zh=zh, question_type=question_type)

    # 2. 压缩输入
    rows_clean = _clean_rows(rows, query_type=hints.get("question_type"))
    analysis_clean = _clean_analysis(analysis)
    time_window_clean = _clean_time_window(time_window)
    all_rooms_clean: List[str] = []
    if topology_all_rooms:
        seen = set()
        for r in topology_all_rooms:
            room_id = _short(
                r.get("room")
                or r.get("Room")
                or r.get("room_id")
                or r.get("space")
                or ""
            )
            if room_id and room_id not in seen:
                seen.add(room_id)
                all_rooms_clean.append(room_id)
    # 3. 构造上下文
    ctx = {
        "question": user_query,
        "hints": {
            k: v for k, v in hints.items()
            if k in ("room", "metric", "need", "question_type", "topology_intent")
        },
        "query_type": hints.get("question_type", "timeseries"),
        "rows": rows_clean,
        "topology_all_rooms": all_rooms_clean,
        "analysis": analysis_clean,
        "time_window": {
            "raw": time_window,
            "clean_dates": time_window_clean
        },
    }

    # 如果是拓扑类问题，只要 rows 非空就算有数据
    if hints.get("question_type") == "topology":
        has_data = bool(rows_clean)
    else:
        has_data = any(
            isinstance(a, dict) and (a.get("n") or 0) > 0
            for a in analysis_clean
        )

    ctx["has_data"] = has_data

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(ctx, ensure_ascii=False, indent=2)},
    ]

    llm = _get_llm()
    try:
        resp = llm.invoke(messages)
        text = getattr(resp, "content", None) or (resp if isinstance(resp, str) else "")
    except Exception:
        text = llm.predict(messages)

    text = re.sub(r"^```(?:\w+)?\s*", "", str(text).strip())
    text = re.sub(r"\s*```$", "", text.strip())
    return text.strip()
