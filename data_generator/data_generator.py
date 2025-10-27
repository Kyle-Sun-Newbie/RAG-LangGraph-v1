from __future__ import annotations
import os
import math
import csv
import random
import argparse
from datetime import datetime, timedelta
from typing import Tuple, List, Optional

try:
    from dateutil import tz  # timezone handling
except Exception:
    tz = None  # guarded later


# --------------------------- Defaults ---------------------------
DEFAULT_TZ = "Asia/Shanghai"
DEFAULT_POINTS_PER_DAY = 24
DEFAULT_NUM_ROOMS = 500
DEFAULT_DAYS_BACK = 7
DEFAULT_OUT_DIR = r"F:\Task\RAG-LangGraph-Demo\data"
DEFAULT_SEED: Optional[int] = None

# Brick-friendly subclasses for rooms (randomly assign for realism)
ROOM_SUBCLASSES = [
    "Office",               # 办公室
    "Conference_Room",      # 会议室
    "Laboratory",           # 实验室
    "Mechanical_Room",      # 机电间
    "Office_Kitchen",       # 茶水间/茶水吧
]

# Rooms of these types will get IAQ (CO2/PM2.5) sensors
IAQ_ELIGIBLE_TYPES = {"Office", "Conference_Room", "Office_Kitchen"}


# --------------------------- Time helpers ---------------------------
def ensure_tz(tz_name: str):
    if tz is None:
        raise RuntimeError("python-dateutil 未安装。请先 pip install python-dateutil，或移除时区相关特性。")
    zone = tz.gettz(tz_name)
    if zone is None:
        raise ValueError(f"Unknown timezone: {tz_name}")
    return zone


def local_range_from_today_back(tz_name: str, days_back: int) -> Tuple[datetime, datetime]:
    """
    返回 [start, end) 的本地时间区间：
    start = 今天0点往回days_back天
    end   = 明天0点
    这样覆盖 days_back+1 整天，方便"昨天""前天"类问题。
    """
    zone = ensure_tz(tz_name)
    now_local = datetime.now(zone)
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    start = today_start - timedelta(days=days_back)
    end = today_start + timedelta(days=1)
    return start, end


def gen_time_points_multiday(start_local: datetime, end_local: datetime, points_per_day: int) -> List[datetime]:
    if points_per_day <= 0:
        raise ValueError("points_per_day must be > 0")
    total_days = (end_local - start_local).total_seconds() / 86400.0
    total_points = int(round(total_days * points_per_day))
    step_seconds = 86400.0 / points_per_day
    return [start_local + timedelta(seconds=i * step_seconds) for i in range(total_points)]


# --------------------------- Synthetic signals ---------------------------
def daily_shapes(room_idx: int, t_norm: float):
    """
    给定房间索引和一天中的归一化位置(0~1)，返回基础值(温度/湿度/照度/CO2/PM2.5)的平滑日变化趋势。
    """
    # 温度：围绕23°C，带日波动+房间偏移
    temp_base = 23.0 + 2.0 * math.sin(2 * math.pi * (t_norm - 0.25)) + 0.1 * (room_idx % 5)

    # 湿度：围绕50%RH，反相波动
    rh_base = 50.0 - 5.0 * math.sin(2 * math.pi * (t_norm - 0.25)) + 2.0 * ((room_idx % 3) - 1)

    # 照度：白天高，夜里低
    hour = t_norm * 24.0
    if 7 <= hour <= 18:
        lux_base = 300.0 + 600.0 * math.cos((hour - 12) * math.pi / 11)
    else:
        lux_base = 20.0 + 10.0 * (room_idx % 4)

    # IAQ：上班时间 (9~18) CO2/PM2.5 上升
    if 9 <= hour <= 18:
        co2_base = 600.0 + 400.0 * math.sin((hour - 9) * math.pi / 9) + 5.0 * (room_idx % 7)
        pm25_base = 15.0 + 10.0 * math.sin((hour - 9) * math.pi / 9) + 0.5 * (room_idx % 11)
    else:
        co2_base = 500.0 + 10.0 * (room_idx % 7)
        pm25_base = 8.0 + 0.5 * (room_idx % 11)

    return (
        temp_base,
        rh_base,
        max(lux_base, 0.0),
        max(co2_base, 350.0),
        max(pm25_base, 2.0),
    )


def jitter(val: float, sigma: float) -> float:
    """高斯抖动，让数据不像死公式"""
    return float(val + random.gauss(0.0, sigma))


# --------------------------- TTL helpers ---------------------------
def ttl_header(building_urn: str = "urn:demo-building#", use_rec: bool = False) -> str:
    """
    Turtle prefix 区，包含 Brick/REF/QUDT/quantitykind/rdfs 以及我们的 bldg: 命名空间
    (注意我们新增了 rdfs: 以支持 rdfs:subClassOf)
    """
    rec_prefix = "@prefix rec:   <https://w3id.org/rec#> .\n" if use_rec else ""
    return f"""@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ref:   <https://brickschema.org/schema/Brick/ref#> .
@prefix unit:  <http://qudt.org/vocab/unit/> .
@prefix qudt:  <http://qudt.org/schema/qudt/> .
@prefix quantitykind: <http://qudt.org/vocab/quantitykind/> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
{rec_prefix}@prefix bldg:  <{building_urn}> .

"""


def _room_type_for_index(idx: int, rng: random.Random) -> str:
    """
    给房间选一个 Brick 子类（Office / Conference_Room / ...）
    使用传入的rng决定它，这样可复现，不污染全局random。
    """
    return rng.choice(ROOM_SUBCLASSES)


def _random_area_m2(idx: int, rng: random.Random) -> float:
    """
    给房间或楼层生成一个面积，稳定但带一点波动。
    """
    base = 30.0 + (idx % 10) * 3.5  # 大约30~65
    return round(base + rng.uniform(-5, 5), 2)


def write_topology_ttl(
    out_path: str,
    num_rooms: int,
    use_rec: bool = False,
    add_site: bool = True,
    include_iaq: bool = True,
    seed_val=None,
):
    """
    生成 Brick/REC 拓扑 + 元信息 + 传感器定义 + ref:hasTimeseriesId

    方案B:
    - 我们在图里声明 bldg:CO2_Level / bldg:PM25_Level 这两个"测量量"
      并且把它们标成 brick:Quantity ，再用 rdfs:subClassOf 指到 brick:Air_Quality
    - IAQ 传感器不直接 measures brick:Air_Quality
      而是 measures bldg:CO2_Level / bldg:PM25_Level
    """

    rng_meta = random.Random(str(seed_val) + "_meta")
    lines: List[str] = [ttl_header(use_rec=use_rec)]

    # (0) 定义可测量量，挂到 Air_Quality 上
    lines += [
        "bldg:CO2_Level a brick:Quantity ;\n"
        "  rdfs:subClassOf brick:Air_Quality .\n\n",

        "bldg:PM25_Level a brick:Quantity ;\n"
        "  rdfs:subClassOf brick:Air_Quality .\n\n",
    ]

    # (1) 空间层级：Site -> Building -> Floor -> Room_xxx
    if use_rec:
        lines += [
            "bldg:CampusA a rec:Campus .\n",
            "bldg:BuildingA a rec:Building ;\n  rec:hasPart bldg:F1 .\n",
            "bldg:F1 a rec:Level .\n\n",
        ]
        rel_hasPart = "rec:hasPart"
        room_superclass = "rec:Room"
    else:
        lines += [
            "bldg:CampusA a brick:Site ;\n  brick:hasPart bldg:BuildingA .\n",
            "bldg:BuildingA a brick:Building ;\n  brick:hasPart bldg:F1 .\n",
            "bldg:F1 a brick:Floor .\n\n",
        ]
        rel_hasPart = "brick:hasPart"
        room_superclass = "brick:Room"

    # Floor 面积
    floor_area_val = _random_area_m2(999, rng_meta)
    lines.append(f"""bldg:F1 brick:area [
  brick:value {floor_area_val} ;
  brick:hasUnit unit:M2
] .\n\n""")

    # (2) Room 循环
    for i in range(1, num_rooms + 1):
        room_name = f"Room_{i:03d}"
        room_uri = f"bldg:{room_name}"

        # 每个房间独立rng决定 subtype / area，保证可复现
        rng_room = random.Random(f"{seed_val}_{i}_room")
        subtype = _room_type_for_index(i, rng_room)
        area_val = _random_area_m2(i, rng_room)

        # 2.1 拓扑关系
        lines.append(f"bldg:F1 {rel_hasPart} {room_uri} .\n")

        # 2.2 房间类型
        if use_rec:
            lines.append(f"{room_uri} a {room_superclass} .\n")
        else:
            lines.append(f"{room_uri} a {room_superclass} , brick:{subtype} .\n")

        # 2.3 房间面积
        lines.append(f"""{room_uri} brick:area [
  brick:value {area_val} ;
  brick:hasUnit unit:M2
] .\n\n""")

        # (3) 常规传感器 (Temp / RH / Lux)
        lines.append(f"""{room_uri}_Temp a brick:Air_Temperature_Sensor ;
  brick:isPointOf {room_uri} ;
  brick:hasUnit unit:DEG_C ;
  ref:hasTimeseriesReference [
    a ref:TimeseriesReference ;
    ref:hasTimeseriesId "{room_name.lower()}.temp" ;
    ref:storedAt bldg:TSDB
  ] .
""")

        lines.append(f"""{room_uri}_RH a brick:Relative_Humidity_Sensor ;
  brick:isPointOf {room_uri} ;
  brick:hasUnit unit:PERCENT_RH ;
  ref:hasTimeseriesReference [
    a ref:TimeseriesReference ;
    ref:hasTimeseriesId "{room_name.lower()}.rh" ;
    ref:storedAt bldg:TSDB
  ] .
""")

        lines.append(f"""{room_uri}_Lux a brick:Illuminance_Sensor ;
  brick:isPointOf {room_uri} ;
  brick:hasUnit unit:LUX ;
  ref:hasTimeseriesReference [
    a ref:TimeseriesReference ;
    ref:hasTimeseriesId "{room_name.lower()}.lux" ;
    ref:storedAt bldg:TSDB
  ] .
""")

        # (4) IAQ 传感器 (CO2 / PM2.5)
        if include_iaq and (subtype in IAQ_ELIGIBLE_TYPES):
            lines.append(f"""{room_uri}_CO2 a brick:CO2_Level_Sensor ;
  brick:isPointOf {room_uri} ;
  brick:hasUnit unit:PPM ;
  brick:measures bldg:CO2_Level ;
  ref:hasTimeseriesReference [
    a ref:TimeseriesReference ;
    ref:hasTimeseriesId "{room_name.lower()}.co2" ;
    ref:storedAt bldg:TSDB
  ] .
""")

            lines.append(f"""{room_uri}_PM25 a brick:PM2.5_Sensor ;
  brick:isPointOf {room_uri} ;
  brick:hasUnit unit:MicroGM-PER-M3 ;
  brick:measures bldg:PM25_Level ;
  ref:hasTimeseriesReference [
    a ref:TimeseriesReference ;
    ref:hasTimeseriesId "{room_name.lower()}.pm25" ;
    ref:storedAt bldg:TSDB
  ] .
""")

    # (5) TSDB节点 + 单位/量纲声明
    lines.append("bldg:TSDB a ref:ExternalReference .\n\n")

    lines += [
        # 舒适环境指标
        "unit:DEG_C a qudt:Unit .\n",
        "unit:PERCENT_RH a qudt:Unit .\n",
        "unit:LUX a qudt:Unit .\n",
        # 面积
        "unit:M2 a qudt:Unit .\n",
        # IAQ
        "unit:PPM a qudt:Unit .\n",
        "unit:MicroGM-PER-M3 a qudt:Unit .\n",
        # 量纲
        "quantitykind:Temperature a qudt:QuantityKind .\n",
        "quantitykind:RelativeHumidity a qudt:QuantityKind .\n",
        "quantitykind:Illuminance a qudt:QuantityKind .\n",
        "quantitykind:Air_Quality a qudt:QuantityKind .\n",
    ]

    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


# --------------------------- CSV writer ---------------------------
def generate_timeseries_csv(
    out_path: str,
    tz_name: str,
    num_rooms: int,
    points_per_day: int,
    days_back: int,
    seed_val=None,
    include_unit_in_csv: bool = True,
    include_iaq: bool = True,
):
    """
    写出 long-form CSV:
    ts_id,timestamp,value,unit
    room_001.temp,2025-10-20T00:00:00+08:00,23.15,DEG_C
    ...
    对 IAQ (co2 / pm25): 只在有 IAQ 传感器的房间才写。
    """

    # 这个全局random我们拿来抖动每个采样点，不要用它决定房间类型
    if seed_val is not None:
        random.seed(seed_val)

    start_local, end_local = local_range_from_today_back(tz_name, days_back)
    times_local = gen_time_points_multiday(start_local, end_local, points_per_day)

    fieldnames = ["ts_id", "timestamp", "value"]
    if include_unit_in_csv:
        fieldnames.append("unit")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, num_rooms + 1):
            room_id = f"room_{i:03d}"

            # 和 TTL 保持一致的房间subtype/是否IAQ逻辑
            rng_room = random.Random(f"{seed_val}_{i}_room")
            subtype = _room_type_for_index(i, rng_room)
            has_iaq = include_iaq and (subtype in IAQ_ELIGIBLE_TYPES)

            for idx, t in enumerate(times_local):
                t_norm = (idx % points_per_day) / points_per_day
                temp_b, rh_b, lux_b, co2_b, pm25_b = daily_shapes(i, t_norm)

                temp = jitter(temp_b, 0.25)
                rh = max(0.0, min(100.0, jitter(rh_b, 1.5)))
                lux = max(0.0, jitter(lux_b, 25.0))
                co2 = max(350.0, jitter(co2_b, 30.0))
                pm25 = max(2.0, jitter(pm25_b, 2.5))

                ts_iso = t.isoformat()

                base_rows = [
                    {
                        "ts_id": f"{room_id}.temp",
                        "timestamp": ts_iso,
                        "value": round(temp, 2),
                        **({"unit": "DEG_C"} if include_unit_in_csv else {}),
                    },
                    {
                        "ts_id": f"{room_id}.rh",
                        "timestamp": ts_iso,
                        "value": round(rh, 2),
                        **({"unit": "PERCENT_RH"} if include_unit_in_csv else {}),
                    },
                    {
                        "ts_id": f"{room_id}.lux",
                        "timestamp": ts_iso,
                        "value": round(lux, 1),
                        **({"unit": "LUX"} if include_unit_in_csv else {}),
                    },
                ]

                if has_iaq:
                    base_rows.append(
                        {
                            "ts_id": f"{room_id}.co2",
                            "timestamp": ts_iso,
                            "value": round(co2, 1),
                            **({"unit": "PPM"} if include_unit_in_csv else {}),
                        }
                    )
                    base_rows.append(
                        {
                            "ts_id": f"{room_id}.pm25",
                            "timestamp": ts_iso,
                            "value": round(pm25, 1),
                            **({"unit": "MicroGM-PER-M3"} if include_unit_in_csv else {}),
                        }
                    )

                for r in base_rows:
                    writer.writerow(r)


# --------------------------- CLI ---------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate Brick/REC-style topology.ttl + realistic timeseries.csv (temp/rh/lux/co2/pm25) with SHACL-friendly units, room metadata, and IAQ semantics."
    )
    parser.add_argument("--out-dir", type=str, default=DEFAULT_OUT_DIR,
                        help="Output directory (will be created).")
    parser.add_argument("--timezone", type=str, default=DEFAULT_TZ,
                        help="IANA timezone, e.g., Asia/Shanghai")
    parser.add_argument("--num-rooms", type=int, default=DEFAULT_NUM_ROOMS,
                        help="Number of rooms")
    parser.add_argument("--points-per-day", type=int, default=DEFAULT_POINTS_PER_DAY,
                        help="Samples per day (default 24 => every hour)")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK,
                        help="From today back N days (inclusive range, plus today)")
    parser.add_argument("--seed", type=str, default=os.getenv("SEED", DEFAULT_SEED),
                        help="Random seed (int or str) for reproducibility")
    parser.add_argument("--no-unit-in-csv", action="store_true",
                        help="Do not include 'unit' column in CSV output")
    parser.add_argument("--use-rec", action="store_true",
                        help="Use REC-like location classes instead of Brick's Room/Floor/etc")
    parser.add_argument("--no-iaq", action="store_true",
                        help="Disable CO2 / PM2.5 sensors altogether")
    args = parser.parse_args()

    # init tz
    ensure_tz(args.timezone)

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # 规范 seed：我们允许 int 或 str
    if args.seed is None:
        seed_val = None
    else:
        try:
            seed_val = int(args.seed)
        except Exception:
            seed_val = str(args.seed)

    # 写 TTL
    ttl_path = os.path.join(out_dir, "topology.ttl")
    write_topology_ttl(
        ttl_path,
        num_rooms=args.num_rooms,
        use_rec=args.use_rec,
        add_site=True,
        include_iaq=(not args.no_iaq),
        seed_val=seed_val,
    )

    # 写 CSV
    csv_path = os.path.join(out_dir, "timeseries.csv")
    generate_timeseries_csv(
        csv_path,
        args.timezone,
        args.num_rooms,
        args.points_per_day,
        days_back=args.days_back,
        seed_val=seed_val,
        include_unit_in_csv=not args.no_unit_in_csv,
        include_iaq=(not args.no_iaq),
    )

    print(f"Wrote:\n  {ttl_path}\n  {csv_path}")


if __name__ == "__main__":
    main()