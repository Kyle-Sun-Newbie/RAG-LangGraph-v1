r"""
Validate and introspect a Brick/REC building model.

功能：
1. SHACL 结构校验（brickschema.Graph(validate)）
2. 单位(QUDT)与量纲词表的可识别性检查
3. 拓扑规模统计（点位数 / 房间数）
4. TTL <-> CSV 一致性检查（ref:hasTimeseriesId vs timeseries.csv 里的 ts_id）

注意：
- 需要 brickschema、rdflib。
- 需要离线/在线提供 QUDT 词表，不然 SHACL 可能报 hasUnit 类似的假阳性。
"""

import csv
import warnings
from pathlib import Path

# ========== 静音不相关的警告（输出更干净） ==========
warnings.filterwarnings("ignore", category=UserWarning, module="rdflib_sqlalchemy")

# ========== 固定文件路径（按需修改） ==========
TTL_PATH = Path(r"F:\Task\RAG-LangGraph-Demo\data\topology.ttl")
CSV_PATH = Path(r"F:\Task\RAG-LangGraph-Demo\data\timeseries.csv")

# ========== QUDT 词表（本地优先，其次远程） ==========
QUDT_UNIT_LOCAL = Path("qudt-unit.ttl")
QUDT_QK_LOCAL = Path("qudt-quantitykind.ttl")

QUDT_UNIT_VOCABS = [
    "http://qudt.org/2.1/vocab/unit",
    "https://qudt.org/vocab/unit",
    "http://qudt.org/vocab/unit",
]
QUDT_QK_VOCABS = [
    "http://qudt.org/2.1/vocab/quantitykind",
    "https://qudt.org/vocab/quantitykind",
    "http://qudt.org/vocab/quantitykind",
]


def load_vocab(graph, local_path: Path, remotes: list[str], label: str) -> bool:
    """
    按优先级加载 QUDT 之类的词表：本地 -> 远程。
    成功返回 True。
    """
    if local_path and local_path.exists():
        try:
            graph.parse(local_path.as_posix(), format="turtle")
            print(f"[{label}] Loaded local: {local_path}")
            return True
        except Exception as e:
            print(f"[{label}] Failed to load local {local_path}: {e}")
    for uri in remotes:
        try:
            graph.parse(uri, format="turtle")
            print(f"[{label}] Loaded remote: {uri}")
            return True
        except Exception as e:
            print(f"[{label}] Failed to load remote {uri}: {e}")
    return False


def extract_timeseries_ids_from_ttl(g):
    """
    从 TTL 模型里提取所有 ref:hasTimeseriesId 的字符串集合。
    用于和 CSV 的 ts_id 做对照。
    """
    q_tsid = """
    PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
    SELECT ?tsid WHERE {
      ?ref a ref:TimeseriesReference ;
           ref:hasTimeseriesId ?tsid .
    }
    """
    return {str(row.tsid) for row in g.query(q_tsid)}


def extract_ts_ids_from_csv(csv_path: Path):
    """
    从 timeseries.csv 里提取所有 ts_id 的集合
    """
    if not csv_path.exists():
        print(f"[CSV] 未找到 CSV 文件: {csv_path}")
        return set()

    ts_ids = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # 期望列: ts_id, timestamp, value, [unit]
        if "ts_id" not in reader.fieldnames:
            print("[CSV] 警告：CSV 中没有 ts_id 列，无法做一致性检查。")
            return set()
        for row in reader:
            ts_ids.add(row["ts_id"])
    return ts_ids


def main():
    # ----------- 依赖 brickschema -----------
    try:
        from brickschema import Graph
    except ModuleNotFoundError:
        raise SystemExit("缺少依赖：请先安装 → pip install brickschema rdflib")

    if not TTL_PATH.exists():
        raise SystemExit(f"未找到 TTL 文件：{TTL_PATH}")

    print("========== Brick/REC SHACL 校验 ==========")
    print(f"[INFO] 正在校验 TTL: {TTL_PATH}")
    if CSV_PATH.exists():
        print(f"[INFO] 关联 CSV:   {CSV_PATH}")
    else:
        print(f"[INFO] 关联 CSV:   (未找到 {CSV_PATH})")

    # ----------- 1. 载入 Brick 本体 + SHACL 形状，再载入我们的模型 -----------
    g = Graph(load_brick=True)
    g.parse(TTL_PATH, format="turtle")

    # ----------- 2. 补充 QUDT 词表，避免 unit/quantitykind 校验报假阳性 -----------
    load_vocab(g, QUDT_UNIT_LOCAL, QUDT_UNIT_VOCABS, "UNIT")
    load_vocab(g, QUDT_QK_LOCAL, QUDT_QK_VOCABS, "QK")

    # ----------- 3. 跑 SHACL 校验 -----------
    valid, report_graph, report_text = g.validate()
    print("\n[SHACL] Valid?", valid)
    if not valid:
        print("----- SHACL Report (模型不合规) -----")
        print(report_text)
    else:
        print("[SHACL] 通过基本校验")

    # ----------- 4. 拓扑规模统计（房间数 / 点位数） -----------
    q_topo = """
    SELECT (COUNT(?p) AS ?points) (COUNT(DISTINCT ?room) AS ?rooms)
    WHERE {
      ?p a ?ptype ;
         <https://brickschema.org/schema/Brick#isPointOf> ?room .
    }
    """
    for row in g.query(q_topo):
        print(f"\n[STATS] Points(传感器数量): {row.points}")
        print(f"[STATS] Rooms(房间数量):   {row.rooms}")

    # ----------- 5. 单位/量纲识别自检 -----------
    q_units = """
    PREFIX unit: <http://qudt.org/vocab/unit/>
    PREFIX qudt: <http://qudt.org/schema/qudt/>
    SELECT ?u WHERE {
      VALUES ?u { unit:DEG_C unit:PERCENT_RH unit:LUX unit:PPM unit:MicroGM-PER-M3 unit:M2 }
      ?u a qudt:Unit .
    }
    """
    q_qk = """
    PREFIX quantitykind: <http://qudt.org/vocab/quantitykind/>
    PREFIX qudt: <http://qudt.org/schema/qudt/>
    SELECT ?q WHERE {
      VALUES ?q {
        quantitykind:Temperature
        quantitykind:RelativeHumidity
        quantitykind:Illuminance
        quantitykind:Air_Quality
      }
      ?q a qudt:QuantityKind .
    }
    """
    units_found = list(g.query(q_units))
    qk_found = list(g.query(q_qk))
    print(f"\n[QUDT] 识别到 qudt:Unit 的单位数量: {len(units_found)} (期望>=几种我们用到的单位)")
    print(f"[QUDT] 识别到 qudt:QuantityKind 的量纲数量: {len(qk_found)}")

    # ----------- 6. TTL vs CSV 的 ts_id 一致性检查 -----------
    ttl_ids = extract_timeseries_ids_from_ttl(g)
    csv_ids = extract_ts_ids_from_csv(CSV_PATH)

    if ttl_ids or csv_ids:
        print("\n[TSID] Timeseries ID 匹配检查：")
        print(f"  TTL中声明的数量: {len(ttl_ids)}")
        print(f"  CSV中出现的数量: {len(csv_ids)}")

        missing_in_csv = ttl_ids - csv_ids
        missing_in_ttl = csv_ids - ttl_ids

        if missing_in_csv:
            print("  [警告] 这些 ID 在 TTL 里声明了，但 CSV 里没有实际数据：")
            for mid in sorted(missing_in_csv):
                print("   -", mid)
        else:
            print("  OK: TTL -> CSV 一致（所有 ref:hasTimeseriesId 在 CSV 里都找到了）")

        if missing_in_ttl:
            print("  [警告] 这些 ID 出现在 CSV 里，但 TTL 里没有对应的 ref:hasTimeseriesId：")
            for mid in sorted(missing_in_ttl):
                print("   -", mid)
        else:
            print("  OK: CSV -> TTL 一致（所有 CSV ts_id 都能在 TTL 里找到映射）")

    else:
        print("\n[TSID] 没能拿到任一侧的 ID（可能没生成 CSV，或 TTL 没写 ref:hasTimeseriesId）")

    print("\n[DONE] 校验/统计/一致性检查结束。")


if __name__ == "__main__":
    main()
