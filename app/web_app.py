from __future__ import annotations
import json, time
from typing import Dict, Any, List, Tuple
import streamlit as st
import pandas as pd
from tools.graph import agent
from tools.graph import USE_RAG

st.set_page_config(page_title="Building Q&A (LangGraph)", page_icon="🤖", layout="wide")
st.title("🤖 Building Q&A · RAG & LangGraph")

# ========== 会话内历史 ==========
if "history" not in st.session_state:
    st.session_state["history"] = []


def show_df(name: str, rows: List[Dict[str, Any]]):
    if not rows:
        st.info(f"{name}：无数据")
        return None
    try:
        df = pd.DataFrame(rows)
        st.dataframe(df, width='stretch')
        return df
    except Exception:
        st.code(json.dumps(rows, ensure_ascii=False, indent=2), language="json")
        return None


def build_frames_from_trace(trace: List[str], hints: Dict[str, Any] | None = None,
                            retries: int = 0, fallback_strategy: str = "none") -> List[Dict[str, Any]]:
    alias = {
        "plan()": "intent", "intent": "intent",
        "rag": "rag", "rag()": "rag",
        "normalize_time": "normalize_time", "normalize_time()": "normalize_time",
        "generate_sparql": "generate_sparql", "build_query()": "generate_sparql",
        "execute_sparql": "execute_sparql", "run_query()": "execute_sparql",
        "route_zero_rows": "route_zero_rows",
        "analysis()": "analyze", "analyze": "analyze",
        "analyze_point_in_time": "analyze_point_in_time",
        "answer()": "answer", "answer": "answer",
    }

    seq = [alias.get(str(x), str(x)) for x in trace if x]
    hints = hints or {}

    # 拓扑类问题处理
    if hints.get("question_type") == "topology" and "execute_sparql" in seq and "analyze" not in seq:
        if "topology_answer" not in seq:
            seq.insert(seq.index("execute_sparql") + 1, "topology_answer")

    # 精确时间点问题处理
    time_range = hints.get("time_range")
    if time_range and time_range.get(
            "kind") == "point_in_time" and "execute_sparql" in seq and "analyze_point_in_time" not in seq:
        seq.insert(seq.index("execute_sparql") + 1, "analyze_point_in_time")

    # 回退策略处理
    if retries > 0:
        last_execute_idx = -1
        for i, node in enumerate(seq):
            if node == "execute_sparql":
                last_execute_idx = i

        if last_execute_idx != -1 and "route_zero_rows" not in seq[last_execute_idx:]:
            if fallback_strategy == "level_1":
                seq.insert(last_execute_idx + 1, "fallback_level_1")
                seq.insert(last_execute_idx + 2, "generate_sparql")
            elif fallback_strategy in ["level_2", "level_1_fallback"]:
                seq.insert(last_execute_idx + 1, "fallback_level_2")
                seq.insert(last_execute_idx + 2, "rag")
                seq.insert(last_execute_idx + 3, "generate_sparql")

    frames, active_nodes, active_edges = [], [], []
    for i, node in enumerate(seq):
        if node not in active_nodes:
            active_nodes.append(node)
        if i > 0:
            edge = (seq[i - 1], seq[i])
            if edge not in active_edges:
                active_edges.append(edge)
        frames.append({"nodes": list(active_nodes), "edges": list(active_edges)})

    if not frames and seq:
        frames.append({"nodes": [seq[0]], "edges": []})
    return frames


def build_dot(
        active_nodes: List[str],
        active_edges: List[Tuple[str, str]],
        retries: int = 0,
        fallback_strategy: str = "none"
) -> str:
    core_nodes = [
        "intent", "rag", "normalize_time", "generate_sparql", "execute_sparql",
        "route_zero_rows", "analyze", "analyze_point_in_time", "topology_answer", "answer",
        "fallback_level_1", "fallback_level_2",
    ]

    all_edges: List[Tuple[str, str, str]] = []

    if USE_RAG:
        all_edges += [("intent", "rag", "forward"), ("rag", "normalize_time", "forward")]
    else:
        all_edges += [("intent", "normalize_time", "forward")]

    all_edges += [
        ("normalize_time", "generate_sparql", "forward"),
        ("generate_sparql", "execute_sparql", "forward"),
        ("execute_sparql", "analyze", "forward"),
        ("execute_sparql", "analyze_point_in_time", "forward"),
        ("execute_sparql", "topology_answer", "forward"),
        ("topology_answer", "answer", "forward"),
        ("execute_sparql", "answer", "forward"),
        ("execute_sparql", "route_zero_rows", "forward"),
        ("route_zero_rows", "fallback_level_1", "forward"),
        ("fallback_level_1", "generate_sparql", "forward"),
        ("route_zero_rows", "fallback_level_2", "forward"),
        ("fallback_level_2", "rag", "forward"),
        ("analyze", "answer", "forward"),
        ("analyze_point_in_time", "answer", "forward"),
    ]

    active_nodes_set = set(active_nodes or [])
    active_edges_set = set(active_edges or [])

    if retries > 0:
        if fallback_strategy == "level_1":
            active_nodes_set.add("fallback_level_1")
            active_edges_set.add(("route_zero_rows", "fallback_level_1"))
            active_edges_set.add(("fallback_level_1", "generate_sparql"))
        elif fallback_strategy in ["level_2", "level_1_fallback"]:
            active_nodes_set.add("fallback_level_2")
            active_edges_set.add(("route_zero_rows", "fallback_level_2"))
            active_edges_set.add(("fallback_level_2", "rag"))
            active_edges_set.add(("rag", "generate_sparql"))

    label_map = {
        "intent": "意图解析", "rag": "RAG检索", "normalize_time": "时间归一化",
        "generate_sparql": "生成SPARQL", "execute_sparql": "执行SPARQL",
        "route_zero_rows": "0行回退", "analyze": "统计调度器",
        "analyze_point_in_time": "精确时间点分析", "topology_answer": "结构性回答",
        "answer": "最终回答", "fallback_level_1": "第一级回退\nLLM生成",
        "fallback_level_2": "第二级回退\nRAG增强",
    }

    def node_stmt(n: str) -> str:
        label = label_map.get(n, n).replace('"', "'")
        if n.startswith("fallback_"):
            if n in active_nodes_set:
                return f'"{n}" [shape=box, style=filled, fillcolor="#FBD38D", label="{label}"];'
            else:
                return f'"{n}" [shape=box, color="#F6AD55", fontcolor="#744210", label="{label}"];'
        elif n in active_nodes_set:
            return f'"{n}" [shape=box, style=filled, fillcolor="#C6F6D5", label="{label}"];'
        else:
            return f'"{n}" [shape=box, color="#CBD5E0", fontcolor="#4A5568", label="{label}"];'

    def edge_stmt(a: str, b: str) -> str:
        active = (a, b) in active_edges_set
        color = "#2F855A" if active else "#CBD5E0"
        penwidth = "2.4" if active else "1.0"

        if a.startswith("fallback_") or b.startswith("fallback_"):
            color = "#DD6B20" if active else "#F6AD55"
            penwidth = "2.8" if active else "1.2"

        return f'"{a}" -> "{b}" [color="{color}", penwidth={penwidth}];'

    lines = [
        'digraph G {', 'rankdir=LR;', 'splines=true;', 'nodesep=0.5;', 'ranksep=0.8;'
    ]

    for n in core_nodes:
        lines.append(node_stmt(n))

    for (a, b, _) in all_edges:
        lines.append(edge_stmt(a, b))

    lines.append("}")
    return "\n".join(lines)


# 侧边栏历史
st.sidebar.header("🕘 最近提问历史")
_hist = list(reversed(st.session_state["history"]))[:5]
if not _hist:
    st.sidebar.write("（暂无历史）")
else:
    for i, h in enumerate(_hist, start=1):
        with st.sidebar.expander(f"{i}. {h['q']}", expanded=(i == 1)):
            st.markdown(f"**回答：** {h['answer']}")
            st.markdown(f"**匹配行数：** {h['rows_count']}")
            retries = h.get("retries", 0)
            if retries > 0:
                strategy = h.get("fallback_strategy", "none")
                st.markdown(f"**回退：** {retries}次 ({strategy})")

# 主输入区
with st.form("qa_form"):
    q = st.text_input("问题：", placeholder="例如：前天305房间的平均温度是多少？或 整栋楼有多少个房间？")
    submitted = st.form_submit_button("询问")

if submitted and q.strip():
    with st.spinner("正在查询..."):
        result: Dict[str, Any] = agent.invoke({"question": q})

    # 保存历史
    fallback_strategy = result.get("fallback_strategy", "none")
    retries = result.get("retries", 0)

    st.session_state["history"].append({
        "q": q, "answer": result.get("answer") or "（无）",
        "rows_count": len(result.get("rows") or []),
        "retries": retries, "fallback_strategy": fallback_strategy,
    })

    st.subheader("答案")
    st.write(result.get("answer") or "（无）")

    col1, col2 = st.columns([3, 2], gap="large")
    with col1:
        st.subheader("LangGraph 流程")
        trace = result.get("trace")
        hints = result.get("hints") or {}

        if not isinstance(trace, list) or not trace:
            st.error("后端未返回 trace。")
        else:
            frames = build_frames_from_trace(trace, hints, retries, fallback_strategy)
            graph_placeholder = st.empty()
            for fr in frames:
                dot = build_dot(fr["nodes"], fr["edges"], retries, fallback_strategy)
                graph_placeholder.graphviz_chart(dot, width='stretch')
                time.sleep(0.6)

    with col2:
        st.subheader("运行状态")
        st.markdown(f"- 问题类型：{(hints.get('question_type'))}")
        st.markdown(f"- RAG 分支：{'开启' if USE_RAG else '关闭'}")
        st.markdown(f"- 结果行数：{len(result.get('rows') or [])}")
        st.markdown(f"- 需要统计：{bool(result.get('need_stats'))}")

        if retries > 0:
            st.markdown(f"- 回退策略：{fallback_strategy}")
            st.markdown(f"- 重试次数：{retries}")
            if fallback_strategy == "level_1":
                st.info("🔧 第一级回退：LLM生成SPARQL")
            elif fallback_strategy == "level_2":
                st.warning("🔄 第二级回退：RAG增强生成")
        else:
            st.success("✅ 初始查询成功")

    with st.expander("Hints（语义解析）", expanded=False):
        st.code(json.dumps(result.get("hints") or {}, ensure_ascii=False, indent=2), language="json")

    with st.expander("SPARQL 查询", expanded=True):
        st.code(result.get("sparql") or "（空）", language="sparql")

    sparql_history = result.get("sparql_history", [])
    if sparql_history:
        with st.expander("🔄 SPARQL 查询历史", expanded=False):
            for i, (strategy, sparql) in enumerate(sparql_history):
                st.markdown(f"**{i + 1}. {strategy}**")
                st.code(sparql, language="sparql")

    with st.expander("SPARQL 结果行", expanded=True):
        show_df("rows", result.get("rows") or [])

    with st.expander("统计分析", expanded=False):
        show_df("analysis", result.get("analysis") or [])

else:
    st.info("输入你的问题后点击【询问】。")