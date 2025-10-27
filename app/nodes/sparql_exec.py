# app/nodes/sparql_exec.py
from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
import time
from rdflib import Graph

# 路径：优先绝对，退回相对
ABS = Path(r"F:\Task\RAG-LangGraph-Demo\data\topology.ttl")
REL = Path(__file__).resolve().parents[2] / "data" / "topology.ttl"
TTL_PATH = ABS if ABS.exists() else REL

_RDF: Graph | None = None

def _get_graph() -> Graph:
    global _RDF
    if _RDF is None:
        g = Graph()
        g.parse(str(TTL_PATH), format="turtle")
        _RDF = g
    return _RDF

# === A-CHANGE === 最小语法校验：括号匹配 + PREFIX 粗检
def _basic_syntax_check(q: str) -> bool:
    if not q or not isinstance(q, str):
        return False
    pairs = {"(": ")", "{": "}", "[": "]"}
    stack = []
    for ch in q:
        if ch in pairs:
            stack.append(ch)
        elif ch in pairs.values():
            if not stack:
                return False
            op = stack.pop()
            if pairs[op] != ch:
                return False
    if stack:
        return False
    # 若使用了前缀（形如 brick:Something），则至少应有 PREFIX 声明
    tokens = q.replace("\n", " ").split()
    uses_prefix = any(":" in t and not t.startswith("http") and not t.upper().startswith("PREFIX") for t in tokens)
    if uses_prefix and "PREFIX" not in q.upper():
        return False
    return True

def _norm_row(item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in item.items():
        out[str(k)] = str(v)  # 值统一为字符串
    # 统一 tsid 字段名
    if "tsid" not in out and "ts_id" in out:
        out["tsid"] = out["ts_id"]
    return out

def execute(query: str) -> List[Dict[str, Any]]:
    # === A-CHANGE === 先做最小语法校验，失败直接返回 []
    if not _basic_syntax_check(query):
        return []

    g = _get_graph()
    t0 = time.perf_counter()
    try:
        qres = g.query(query)
    except Exception:
        return []
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    vars_ = [str(v) for v in getattr(qres, "vars", [])] or None
    rows: List[Dict[str, Any]] = []
    for r in qres:
        if vars_:
            item = {vars_[i]: r[i] for i in range(len(vars_))}
        else:
            item = {f"col{i}": v for i, v in enumerate(r)}
        rows.append(_norm_row(item))

    if rows:
        rows[0]["__elapsed_ms"] = str(elapsed_ms)
    return rows
