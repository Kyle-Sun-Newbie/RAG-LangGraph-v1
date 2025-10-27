from __future__ import annotations
from typing import TypedDict, List, Dict, Any
from langgraph.graph import StateGraph, END

from nodes import (
    rag_agent,
    sparql_agent,
    sparql_exec,
    analysis_agent,
    answer_agent,
    normalize_time_agent,
)

USE_RAG = True


class State(TypedDict, total=False):
    question: str
    need_stats: bool
    have_rows: bool
    retries: int
    max_retries: int
    hints: Dict[str, Any]
    time_window: Dict[str, Any]
    context: str
    sparql: str
    rows: List[Dict[str, Any]]
    analysis: List[Dict[str, Any]]
    analysis_error: str
    answer: str
    trace: List[str]
    topology_all_rooms: List[Dict[str, Any]]


def node_intent(state: State) -> State:
    state.setdefault("trace", []).append("intent")
    try:
        state["hints"] = rag_agent.get_hints(state["question"])
        state["need_stats"] = bool(rag_agent.need_stats(state["question"]))
    except Exception:
        state["hints"] = {}
        state["need_stats"] = False

    state["retries"] = 0
    state["max_retries"] = 1
    return state


def node_rag(state: State) -> State:
    state.setdefault("trace", []).append("rag")
    try:
        chunks = rag_agent.search(state["question"])
        state["context"] = rag_agent.build_context(chunks)
    except Exception:
        state["context"] = ""
    return state


def node_normalize_time(state: State) -> State:
    return normalize_time_agent.node_normalize_time(state)


def node_generate_sparql(state: State) -> State:
    state.setdefault("trace", []).append("generate_sparql")

    # 如果已有SPARQL（来自回退），直接返回
    if state.get("sparql"):
        return state

    try:
        h = state.get("hints", {})
        new_sparql = sparql_agent.generate(
            state["question"],
            context=state.get("context", ""),
            hints=h,
        )

        # 记录SPARQL历史
        if "sparql_history" not in state:
            state["sparql_history"] = []
        state["sparql_history"].append(("initial", new_sparql))
        state["sparql"] = new_sparql

    except Exception:
        state["sparql"] = ""
    return state


def node_execute_sparql(state: State) -> State:
    state.setdefault("trace", []).append("execute_sparql")

    try:
        rows = sparql_exec.execute(state.get("sparql", "")) or []
    except Exception:
        rows = []

    state["rows"] = rows
    state["have_rows"] = bool(rows)

    # 拓扑类问题获取所有房间
    hints = state.get("hints", {}) or {}
    if hints.get("question_type") == "topology":
        try:
            all_rooms_sparql = """
PREFIX brick: <https://brickschema.org/schema/Brick#>
PREFIX bldg:  <urn:demo-building#>

SELECT DISTINCT ?room
WHERE {
  ?room a brick:Room .
}
ORDER BY ?room
""".strip()
            all_rooms_rows = sparql_exec.execute(all_rooms_sparql) or []
            state["topology_all_rooms"] = all_rooms_rows
        except Exception:
            state["topology_all_rooms"] = []

    return state


def node_route_zero_rows(state: State) -> State:
    state.setdefault("trace", []).append("route_zero_rows")
    current_retries = int(state.get("retries", 0))
    state["retries"] = current_retries + 1
    retry_count = current_retries + 1

    try:
        if retry_count == 1:
            # 第一级回退：LLM生成SPARQL
            state["sparql"] = sparql_agent.llm_based_sparql_generation(
                state.get("question", ""),
                context=state.get("context", ""),
                hints=state.get("hints", {})
            )
            state["fallback_strategy"] = "level_1"

        elif retry_count == 2:
            # 第二级回退：RAG增强
            try:
                chunks = rag_agent.search(state.get("question", ""))
                new_context = rag_agent.build_context(chunks)
                state["context"] = new_context

                state["sparql"] = rag_agent.advanced_text_to_sparql(
                    state.get("question", ""),
                    retrieved_context=new_context,
                    hints=state.get("hints", {})
                )
                state["fallback_strategy"] = "level_2"

            except Exception as e:
                # RAG增强失败，回退到第一级
                state["sparql"] = sparql_agent.llm_based_sparql_generation(
                    state.get("question", ""),
                    context=state.get("context", ""),
                    hints=state.get("hints", {})
                )
                state["fallback_strategy"] = "level_1_fallback"

        # 记录SPARQL历史
        if "sparql_history" not in state:
            state["sparql_history"] = []
        state["sparql_history"].append((state["fallback_strategy"], state["sparql"]))

    except Exception as e:
        state["sparql"] = "SELECT ?error WHERE { BIND('查询失败' AS ?error) }"
        state["fallback_strategy"] = "error"

    return state


def node_analyze_point_in_time(state: State) -> State:
    state.setdefault("trace", []).append("analyze_point_in_time")
    try:
        state["analysis"] = analysis_agent.analyze_point_in_time_state(state)
        state["analysis_error"] = None
    except Exception as e:
        state["analysis"] = []
        state["analysis_error"] = f"{type(e).__name__}: {e}"
    return state


def node_analyze(state: State) -> State:
    state.setdefault("trace", []).append("analyze")
    try:
        if hasattr(analysis_agent, "analyze_state"):
            state["analysis"] = analysis_agent.analyze_state(state)
        else:
            tsids = [r.get("tsid") for r in state.get("rows", [])
                     if isinstance(r, dict) and r.get("tsid")]
            state["analysis"] = analysis_agent.analyze(tsids=tsids)
        state["analysis_error"] = None
    except Exception as e:
        state["analysis"] = []
        state["analysis_error"] = f"{type(e).__name__}: {e}"
    return state


def node_answer(state: State) -> State:
    state.setdefault("trace", []).append("answer")
    try:
        state["answer"] = answer_agent.compose(
            user_query=state["question"],
            rows=state.get("rows", []),
            analysis=state.get("analysis", []),
            hints=state.get("hints", {}),
            time_window=state.get("time_window", {}),
            topology_all_rooms=state.get("topology_all_rooms", []),
        )
    except Exception:
        state["answer"] = "抱歉，暂时无法生成答案。"
    return state


def route_after_execute(state: State) -> str:
    hints = state.get("hints", {}) or {}
    qtype = hints.get("question_type")
    time_range = hints.get("time_range", {})
    need = hints.get("need")
    need_ok = isinstance(need, list) and len(need) > 0

    if not state.get("have_rows"):
        return "zero"

    if qtype == "topology":
        return "answer"

    if time_range.get("kind") == "point_in_time":
        return "analyze_point_in_time"

    if state.get("need_stats") or need_ok:
        return "analyze"

    return "answer"


def route_retry_or_end(state: State) -> str:
    max_retries = 2
    current_retries = int(state.get("retries", 0))
    should_retry = current_retries < max_retries
    return "retry" if should_retry else "giveup"


# 构建工作流
workflow = StateGraph(State)

workflow.add_node("intent", node_intent)
workflow.add_node("rag", node_rag)
workflow.add_node("normalize_time", node_normalize_time)
workflow.add_node("generate_sparql", node_generate_sparql)
workflow.add_node("execute_sparql", node_execute_sparql)
workflow.add_node("route_zero_rows", node_route_zero_rows)
workflow.add_node("analyze", node_analyze)
workflow.add_node("answer", node_answer)
workflow.add_node("analyze_point_in_time", node_analyze_point_in_time)

workflow.set_entry_point("intent")

if USE_RAG:
    workflow.add_edge("intent", "rag")
    workflow.add_edge("rag", "normalize_time")
else:
    workflow.add_edge("intent", "normalize_time")

workflow.add_edge("normalize_time", "generate_sparql")
workflow.add_edge("generate_sparql", "execute_sparql")

workflow.add_conditional_edges(
    "execute_sparql",
    route_after_execute,
    {
        "zero": "route_zero_rows",
        "analyze": "analyze",
        "analyze_point_in_time": "analyze_point_in_time",
        "answer": "answer",
    },
)

workflow.add_conditional_edges(
    "route_zero_rows",
    route_retry_or_end,
    {
        "retry": "generate_sparql",
        "giveup": "answer",
    },
)

workflow.add_edge("analyze", "answer")
workflow.add_edge("analyze_point_in_time", "answer")
workflow.add_edge("answer", END)

agent = workflow.compile()