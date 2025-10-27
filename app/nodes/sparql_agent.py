from __future__ import annotations
import re
import json
from typing import Dict, Optional, List
from functools import lru_cache

# ============ 常量定义 ============
BRICK_PREFIX = "PREFIX brick: <https://brickschema.org/schema/Brick#>"
REF_PREFIX = "PREFIX ref:   <https://brickschema.org/schema/Brick/ref#>"
BLDG_PREFIX = "PREFIX bldg:  <urn:demo-building#>"

_ALL_PREFIX = "\n".join([BRICK_PREFIX, REF_PREFIX, BLDG_PREFIX])

# 传感器类型映射
_METRIC_TO_TYPES = {
    "temp": ["brick:Air_Temperature_Sensor"],
    "rh": ["brick:Relative_Humidity_Sensor"],
    "lux": ["brick:Illuminance_Sensor"],
    "co2": ["brick:CO2_Level_Sensor"],
    "pm25": ["brick:PM2.5_Sensor"],
}

# 关键词到指标的映射
_KEYWORD_TO_METRIC = {
    "co2": ["co2", "二氧化碳", "二氧化碳浓度", "co₂"],
    "pm25": ["pm2.5", "pm25", "pm 2.5", "颗粒", "粉尘"],
    "temp": ["温度", "temperature", "temp"],
    "rh": ["湿度", "humidity", "rh"],
    "lux": ["照度", "光照", "illuminance", "lux"]
}


# ============ 核心工具函数 ============
def _get_metric_types(metric: Optional[str]) -> List[str]:
    """获取指定指标对应的传感器类型列表"""
    if metric and metric in _METRIC_TO_TYPES:
        return _METRIC_TO_TYPES[metric]
    # 默认返回所有传感器类型
    return [item for sublist in _METRIC_TO_TYPES.values() for item in sublist]


def _values_pt_types(metric: Optional[str]) -> str:
    """生成SPARQL VALUES约束块"""
    types = _get_metric_types(metric)
    return f"VALUES ?ptType {{ {' '.join(types)} }}"


def _room_filter(room_no: str) -> str:
    """生成房间过滤条件"""
    return f'FILTER(STRENDS(STR(?room), "_{room_no}"))'


def _extract_room_from_text(text: str) -> Optional[str]:
    """从文本中提取房间号"""
    match = re.search(r"(?<!\d)(\d{1,4})(?!\d)", text or "")
    return match.group(1) if match else None


def _infer_metric_from_text(text: str) -> Optional[str]:
    """从文本中推断指标类型"""
    text_lower = (text or "").lower()
    for metric, keywords in _KEYWORD_TO_METRIC.items():
        if any(keyword in text_lower for keyword in keywords):
            return metric
    return None


def _clean_sparql_response(sparql: str) -> str:
    """清理LLM返回的SPARQL响应"""
    sparql = sparql.strip()

    # 移除代码块标记
    for marker in ["```sparql", "```sql", "```"]:
        if marker in sparql:
            parts = sparql.split(marker)
            if len(parts) >= 2:
                sparql = parts[1].split("```")[0].strip()
                break

    # 确保必要的PREFIX
    if "PREFIX brick:" not in sparql:
        sparql = f"{_ALL_PREFIX}\n\n{sparql}"

    return sparql


# ============ 查询模板 ============
class SPARQLTemplates:
    """SPARQL查询模板集合"""

    @staticmethod
    def room_points_tsid(room_no: str, metric: Optional[str]) -> str:
        """查询指定房间的传感器点位和时序ID"""
        return f"""{_ALL_PREFIX}

SELECT ?room ?pt ?ptType ?tsid WHERE {{
  ?room a brick:Room .
  {_room_filter(room_no)}

  ?pt a ?ptType ;
      brick:isPointOf ?room ;
      ref:hasTimeseriesReference [
        a ref:TimeseriesReference ;
        ref:hasTimeseriesId ?tsid
      ] .

  {_values_pt_types(metric)}
}}
LIMIT 50
""".strip()

    @staticmethod
    def list_points_any(limit: int = 20) -> str:
        """列出所有点位（兜底查询）"""
        return f"""{_ALL_PREFIX}

SELECT ?room ?pt ?ptType ?tsid WHERE {{
  ?room a brick:Room .

  ?pt a ?ptType ;
      brick:isPointOf ?room ;
      ref:hasTimeseriesReference [
        a ref:TimeseriesReference ;
        ref:hasTimeseriesId ?tsid
      ] .

  {_values_pt_types(None)}
}}
LIMIT {limit}
""".strip()

    @staticmethod
    def count_rooms() -> str:
        """统计房间数量"""
        return f"""{_ALL_PREFIX}

SELECT (COUNT(DISTINCT ?room) AS ?roomCount)
WHERE {{
  ?room a brick:Room .
}}
""".strip()

    @staticmethod
    def list_rooms() -> str:
        """列出所有房间"""
        return f"""{_ALL_PREFIX}

SELECT DISTINCT ?room
WHERE {{
  ?room a brick:Room .
}}
ORDER BY ?room
""".strip()

    @staticmethod
    def sensor_existence(room: Optional[str], metric: Optional[str]) -> str:
        """查询传感器存在性"""
        room_condition = _room_filter(room) if room else "?room a brick:Room ."

        return f"""{_ALL_PREFIX}

SELECT DISTINCT ?room ?ptType
WHERE {{
  {room_condition}
  ?pt a ?ptType ;
      brick:isPointOf ?room .
  {_values_pt_types(metric)}
}}
""".strip()


# ============ 查询生成器 ============
class SPARQLGenerator:
    """SPARQL查询生成器"""

    def __init__(self):
        self.templates = SPARQLTemplates()

    def generate_timeseries_query(self, hints: Dict) -> str:
        """生成时间序列类查询"""
        room = hints.get("room") or _extract_room_from_text(hints.get("question", ""))
        metric = hints.get("metric") or _infer_metric_from_text(hints.get("question", ""))

        if room:
            return self.templates.room_points_tsid(room, metric)
        return self.templates.list_points_any(limit=20)

    def generate_topology_query(self, hints: Dict) -> str:
        """生成拓扑类查询"""
        topo_intent = hints.get("topology_intent")
        room = hints.get("room")
        metric = hints.get("metric")

        query_map = {
            "count_rooms": self.templates.count_rooms,
            "list_rooms": self.templates.list_rooms,
            "sensor_existence": lambda: self.templates.sensor_existence(room, metric)
        }

        return query_map.get(topo_intent, self.templates.list_rooms)()

    def generate(self, question: str, context: str = "", hints: Dict | None = None) -> str:
        """主生成函数"""
        hints = hints or {}
        hints["question"] = question  # 确保question在hints中可用

        qtype = hints.get("question_type")

        if qtype == "topology":
            return self.generate_topology_query(hints)
        else:
            return self.generate_timeseries_query(hints)


# ============ LLM回退生成器 ============
class LLMSPARQLGenerator:
    """基于LLM的SPARQL回退生成器"""

    def __init__(self):
        self._llm = None

    @lru_cache(maxsize=1)
    def _get_llm(self):
        """获取LLM实例（带缓存）"""
        if self._llm is None:
            try:
                from nodes.rag_agent import _get_llm
                self._llm = _get_llm()
            except ImportError:
                self._llm = None
        return self._llm

    def _build_prompt(self, question: str, hints: Dict) -> str:
        """构建LLM提示词"""
        return f"""
你是一个SPARQL查询专家，专门针对建筑信息模型(BIM)和楼宇自动化系统(BAS)的知识图谱。

知识图谱信息：
- 前缀：
PREFIX brick: <https://brickschema.org/schema/Brick#>
PREFIX ref:   <https://brickschema.org/schema/Brick/ref#>
PREFIX bldg:  <urn:demo-building#>

- 核心模式：
?room a brick:Room .                    # 房间
?pt a ?ptType ;                         # 点位/传感器
    brick:isPointOf ?room ;             # 属于某个房间
    ref:hasTimeseriesReference [        # 有时序数据引用
        ref:hasTimeseriesId ?tsid       # 时序数据ID
    ] .

用户问题：{question}

已解析的语义信息：
- 问题类型：{hints.get('question_type', 'unknown')}
- 拓扑意图：{hints.get('topology_intent', 'N/A')}
- 房间号：{hints.get('room', '未指定')}
- 指标类型：{hints.get('metric', '未指定')}
- 时间范围：{json.dumps(hints.get('time_range', {}), ensure_ascii=False)}
- 统计需求：{hints.get('need', [])}

请基于以上信息，生成一个精确的SPARQL查询。
重点考虑：
1. 如果是拓扑问题（count_rooms/list_rooms/sensor_existence），生成相应的计数或列表查询
2. 如果是时序问题，确保返回?tsid字段用于后续数据分析
3. 如果有房间号，使用FILTER(STRENDS(STR(?room), "_{hints.get('room')}"))进行过滤
4. 如果有指标类型，使用VALUES ?ptType约束传感器类型

只输出SPARQL查询，不要其他任何文字。
"""

    def generate(self, question: str, context: str = "", hints: Dict | None = None) -> str:
        """使用LLM生成SPARQL查询"""
        hints = hints or {}

        llm = self._get_llm()
        if llm is None:
            return SPARQLTemplates.list_points_any(limit=50)

        try:
            prompt = self._build_prompt(question, hints)
            response = llm.invoke(prompt)
            sparql = response.content if hasattr(response, 'content') else str(response)

            cleaned_sparql = _clean_sparql_response(sparql)
            print(f"[SPARQL-LEVEL1] 第一级回退生成查询: {cleaned_sparql}")
            return cleaned_sparql

        except Exception as e:
            print(f"[SPARQL-LEVEL1] Error: {e}")
            return SPARQLTemplates.list_points_any(limit=50)


# ============ 模块接口函数 ============
# 创建全局实例
_sparql_generator = SPARQLGenerator()
_llm_generator = LLMSPARQLGenerator()


def generate(question: str, context: str = "", hints: Dict | None = None) -> str:
    """主接口：生成SPARQL查询"""
    return _sparql_generator.generate(question, context, hints)


def llm_based_sparql_generation(question: str, context: str = "", hints: Dict | None = None) -> str:
    """回退接口：使用LLM生成SPARQL查询"""
    return _llm_generator.generate(question, context, hints)


# 保持向后兼容的别名
_clean_sparql_response = _clean_sparql_response