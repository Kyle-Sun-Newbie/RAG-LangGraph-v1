from __future__ import annotations
import os, json, re
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# ============== 配置和初始化 ==============
ABS_PROMPT = Path(r"F:\Task\RAG-LangGraph-Demo\prompt\prompt.md")
REL_PROMPT = Path(__file__).resolve().parents[2] / "prompt" / "prompt.md"
_prompt_cache: Optional[str] = None
_LLM = None


def _load_prompt() -> Optional[str]:
    global _prompt_cache
    if _prompt_cache is not None:
        return _prompt_cache
    for path in [ABS_PROMPT, REL_PROMPT]:
        if path.exists():
            _prompt_cache = path.read_text(encoding="utf-8")
            return _prompt_cache
    return None


def _get_llm():
    global _LLM
    if _LLM is not None:
        return _LLM

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        return None

    try:
        from langchain.chat_models import init_chat_model
        _LLM = init_chat_model("deepseek:deepseek-chat", temperature=0, api_key=api_key)
        return _LLM
    except Exception:
        return None


# ============== 工具函数 ==============
def _safe_json_from_text(s: str) -> Optional[Dict]:
    if not s:
        return None
    m = re.search(r"\{.*\}", s, flags=re.S)
    txt = m.group(0) if m else s
    try:
        return json.loads(txt) if isinstance(json.loads(txt), dict) else None
    except Exception:
        return None


def _neutral(question: str, source_reason: str) -> Dict:
    return {
        "question_type": "other", "topology_intent": None, "need_stats": False,
        "need": None, "room": None, "metric": None, "time_range": None,
        "uncertain": True, "ambiguities": [], "_source": source_reason,
        "_prompt": str(ABS_PROMPT if ABS_PROMPT.exists() else REL_PROMPT),
    }


# ============== 语义解析核心 ==============
ALLOWED_NEEDS = {"avg", "max", "min", "trend"}
ALLOWED_QTYPE = {"timeseries", "topology", "other"}
ALLOWED_TOPO_INTENT = {"count_rooms", "list_rooms", "sensor_existence"}


def _normalize_need(x: Any) -> Optional[List[str]]:
    if not x:
        return None
    if isinstance(x, str):
        x = [x]
    if not isinstance(x, list):
        return None
    return [n.strip().lower() for n in x if isinstance(n, str) and n.strip().lower() in ALLOWED_NEEDS]


def llm_parse(question: str) -> Dict:
    prompt = _load_prompt()
    if not prompt:
        return _neutral(question, "no-prompt")

    llm = _get_llm()
    if llm is None:
        return _neutral(question, "no-llm")

    try:
        current_time = datetime.now().strftime("%Y年%m月%d日")
        dynamic_prompt = f"**重要：当前系统时间是 {current_time}。**\n\n{prompt}"
        full_prompt = f"{dynamic_prompt}\n\n用户问题：{question or ''}"

        try:
            resp = llm.invoke(full_prompt)
            text = getattr(resp, "content", None) or (resp if isinstance(resp, str) else "")
        except Exception:
            text = llm.predict(full_prompt)

        data = _safe_json_from_text(text)
        if not data:
            return _neutral(question, "parse-error")

        # 字段清洗
        qtype = str(data.get("question_type", "other")).lower()
        qtype = qtype if qtype in ALLOWED_QTYPE else "other"

        topo_intent = data.get("topology_intent")
        if isinstance(topo_intent, str):
            topo_intent = topo_intent.lower().strip()
            topo_intent = topo_intent if topo_intent in ALLOWED_TOPO_INTENT else None

        need = _normalize_need(data.get("need"))
        need_stats = bool(data.get("need_stats")) or bool(need)

        room = data.get("room")
        if isinstance(room, str):
            mm = re.search(r"(?<!\d)(\d{1,4})(?!\d)", room)
            room = mm.group(1) if mm else None

        metric = data.get("metric")
        metric_allow = ("temp", "rh", "lux", "co2", "pm25")
        metric = metric if metric in metric_allow else None

        time_range = data.get("time_range")
        if not isinstance(time_range, dict) or "kind" not in time_range:
            time_range = None

        uncertain = bool(data.get("uncertain", False))
        ambiguities = data.get("ambiguities") or []
        if not isinstance(ambiguities, list):
            ambiguities = [str(ambiguities)]

        return {
            "question_type": qtype, "topology_intent": topo_intent, "need_stats": need_stats,
            "need": need, "room": room, "metric": metric, "time_range": time_range,
            "uncertain": uncertain, "ambiguities": ambiguities, "_source": "llm",
            "_prompt": str(ABS_PROMPT if ABS_PROMPT.exists() else REL_PROMPT),
        }
    except Exception:
        return _neutral(question, "llm-error")


# ============== Graph 接口 ==============
def get_hints(question: str) -> Dict:
    return llm_parse(question)


def need_stats(question: str) -> bool:
    try:
        return bool(llm_parse(question).get("need_stats"))
    except Exception:
        return False


# ============== RAG 功能 ==============
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
from rdflib import Graph

_FAISS_INDEX: Optional[faiss.IndexFlatIP] = None
_FAISS_TEXTS: List[str] = []
_SBERT_MODEL: Optional[SentenceTransformer] = None


def _load_sbert_model() -> SentenceTransformer:
    global _SBERT_MODEL
    if _SBERT_MODEL is None:
        _SBERT_MODEL = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    return _SBERT_MODEL


def _auto_corpus_from_topology(limit: int = 500) -> List[str]:
    ttl_path = Path(__file__).resolve().parents[2] / "data" / "topology.ttl"
    if not ttl_path.exists():
        return [
            "Room 1205 has three temperature sensors.",
            "Room 2201 has two humidity sensors.",
            "Illuminance sensors measure light intensity."
        ]
    g = Graph()
    g.parse(str(ttl_path), format="turtle")
    return [f"{s} {p} {o}" for s, p, o in g][:limit]


def _load_faiss_index() -> faiss.IndexFlatIP:
    global _FAISS_INDEX, _FAISS_TEXTS
    if _FAISS_INDEX is not None:
        return _FAISS_INDEX

    base_dir = Path(__file__).resolve().parents[2] / "data" / "faiss_index"
    index_path = base_dir / "index.faiss"
    text_path = base_dir / "texts.json"

    model = _load_sbert_model()
    if not base_dir.exists():
        base_dir.mkdir(parents=True, exist_ok=True)

    if index_path.exists() and text_path.exists():
        _FAISS_TEXTS = json.loads(text_path.read_text(encoding="utf-8"))
        _FAISS_INDEX = faiss.read_index(str(index_path))
        return _FAISS_INDEX

    corpus = _auto_corpus_from_topology()
    _FAISS_TEXTS = corpus
    embeddings = model.encode(corpus, convert_to_numpy=True, normalize_embeddings=True)
    _FAISS_INDEX = faiss.IndexFlatIP(embeddings.shape[1])
    _FAISS_INDEX.add(embeddings)
    faiss.write_index(_FAISS_INDEX, str(index_path))
    text_path.write_text(json.dumps(corpus, ensure_ascii=False, indent=2), encoding="utf-8")
    return _FAISS_INDEX


def search(question: str, k: int = 5) -> List[Dict]:
    index = _load_faiss_index()
    model = _load_sbert_model()
    q_emb = model.encode([question], convert_to_numpy=True, normalize_embeddings=True)
    D, I = index.search(q_emb, k)
    return [{"text": _FAISS_TEXTS[idx], "score": float(score)}
            for idx, score in zip(I[0], D[0]) if idx < len(_FAISS_TEXTS)]


def build_context(chunks: List[Dict]) -> str:
    if not chunks:
        return ""
    parts = [f"[{i + 1}] {c['text']} (score={c['score']:.3f})" for i, c in enumerate(chunks)]
    return "以下是与问题相关的建筑知识片段：\n" + "\n".join(parts)


# ============== 第二级回退方法 ==============
def advanced_text_to_sparql(question: str, retrieved_context: str = "", hints: Dict | None = None) -> str:
    hints = hints or {}

    prompt = f"""
你是一个建筑领域SPARQL专家。基于以下信息生成精确的SPARQL查询：

知识图谱模式：
PREFIX brick: <https://brickschema.org/schema/Brick#>
PREFIX ref:   <https://brickschema.org/schema/Brick/ref#>
PREFIX bldg:  <urn:demo-building#>

核心关系：
- 房间：?room a brick:Room .
- 传感器：?sensor a ?sensorType ; brick:isPointOf ?room .
- 时序数据：?sensor ref:hasTimeseriesReference [ ref:hasTimeseriesId ?tsid ] .

传感器类型：
- 温度：brick:Air_Temperature_Sensor
- 湿度：brick:Relative_Humidity_Sensor  
- 照度：brick:Illuminance_Sensor
- CO2：brick:CO2_Level_Sensor
- PM2.5：brick:PM2.5_Sensor

检索到的建筑知识：
{retrieved_context}

用户问题：{question}
问题类型：{hints.get('question_type', 'unknown')}
房间号：{hints.get('room', '未指定')}
监测指标：{hints.get('metric', '未指定')}
时间范围：{json.dumps(hints.get('time_range', {}), ensure_ascii=False)}
统计需求：{hints.get('need', [])}

请只输出SPARQL查询：
"""

    llm = _get_llm()
    if llm is None:
        return """
PREFIX brick: <https://brickschema.org/schema/Brick#>
PREFIX ref:   <https://brickschema.org/schema/Brick/ref#>
PREFIX bldg:  <urn:demo-building#>

SELECT ?room ?pt ?ptType ?tsid WHERE {
  ?room a brick:Room .
  ?pt a ?ptType ;
      brick:isPointOf ?room ;
      ref:hasTimeseriesReference [ ref:hasTimeseriesId ?tsid ] .
} LIMIT 50
""".strip()

    try:
        response = llm.invoke(prompt)
        sparql = getattr(response, "content", None) or (response if isinstance(response, str) else "")
        return _clean_sparql_response(sparql)
    except Exception:
        return "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 20"


def _clean_sparql_response(sparql: str) -> str:
    sparql = sparql.strip()
    for block in ["```sparql", "```sql", "```"]:
        if block in sparql:
            parts = sparql.split(block)
            if len(parts) >= 2:
                sparql = parts[1].split("```")[0].strip()
                break

    if "PREFIX brick:" not in sparql:
        basic_prefixes = "PREFIX brick: <https://brickschema.org/schema/Brick#>\nPREFIX ref: <https://brickschema.org/schema/Brick/ref#>\nPREFIX bldg: <urn:demo-building#>"
        sparql = f"{basic_prefixes}\n\n{sparql}"

    return sparql