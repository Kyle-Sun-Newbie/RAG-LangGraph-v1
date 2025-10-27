
# 新版数据生成脚本相对于旧版脚本的优势说明

## 1. 建筑语义模型更完整，而不是“散点+随机数”
**旧版：**
- 只建了 `bldg:Room_{n} a brick:Room`，再用 `brick:hasPoint` 挂一堆传感器节点。
- 没有 Campus / Building / Floor / Room 的层级关系。
- 没有房间面积、房间用途、楼层信息这些真实元数据。

**新版：**
- 显式建模了分层空间结构：
  - `bldg:CampusA` → `bldg:BuildingA` → `bldg:F1` → `bldg:Room_001`。
  - Brick 模式下用 `brick:hasPart`，REC 模式下用 `rec:hasPart`。
- 楼层和房间包含面积属性：
  ```turtle
  bldg:F1 brick:area [
    brick:value 123.45 ;
    brick:hasUnit unit:M2
  ] .
  ```
- 房间还带类型（如 `brick:Office`, `brick:Conference_Room`, `brick:Laboratory` 等），从而能区分“办公室 vs 实验室 vs 机电间”等真实用途。
- 支持 `--use-rec` 切换到 REC (`rec:Campus`, `rec:Building`, `rec:Level`, `rec:Room`) 的空间类，这非常贴近真实楼宇数据资产里 Brick 和 REC 并存的情况。

> 结论：新版数据不只是“有很多房间”，而是“有一栋结构化的楼宇知识图谱”。这让它直接能用于空间层级相关的问答，比如“F1 上有哪些会议室？面积多大？”


## 2. 传感器与时序数据的绑定方式从“自定义字符串”升级为 Brick 推荐模式
**旧版：**
- 每个传感器只是自定义属性 `bldg:ts_id "room3_temp"`。
- CSV 里 `measure_id` 只是和这个 ts_id 对应。
- 这种写法是临时约定，Brick SHACL/标准工具未必认识。

**新版：**
- 为每个传感器创建了 `ref:hasTimeseriesReference` 结构：
  ```turtle
  bldg:Room_001_Temp a brick:Air_Temperature_Sensor ;
    brick:isPointOf bldg:Room_001 ;
    brick:hasUnit unit:DEG_C ;
    ref:hasTimeseriesReference [
      a ref:TimeseriesReference ;
      ref:hasTimeseriesId "room_001.temp" ;
      ref:storedAt bldg:TSDB
    ] .
  ```
- 引入了 Brick 官方 `ref:` 命名空间 (`https://brickschema.org/schema/Brick/ref#`) 来描述：
  - 这个传感器对应哪条时间序列 (`ref:hasTimeseriesId`)
  - 这条时间序列存在哪个外部系统 (`ref:storedAt bldg:TSDB`)
- `bldg:TSDB a ref:ExternalReference .` 把存储端也语义化成一个节点，而不是裸字符串。

> 结论：新版的 TTL 能被 Brick 社区常用工具理解，也能通过 SHACL 校验，而旧版只是“我个人的ts_id约定”。


## 3. 单位、量纲、QUDT：从“值=23.5”到“带单位且可被推理”
**旧版：**
- 没显式声明单位。传感器叫 `Temperature_Sensor`，但机器并不知道它是摄氏度。
- CSV 也没有“单位”列。

**新版：**
- 每个传感器在 TTL 里显式 `brick:hasUnit unit:DEG_C`、`unit:PERCENT_RH`、`unit:LUX`、`unit:PPM`、`unit:MicroGM-PER-M3` 等。
- 这些 `unit:*` 又被声明成 `qudt:Unit`；量纲也加了，例如：
  ```turtle
  unit:DEG_C a qudt:Unit .
  quantitykind:Temperature a qudt:QuantityKind .
  ```
- 生成的 CSV（可选）直接包含 `unit` 列，和 TTL 对齐：
  ```csv
  ts_id,timestamp,value,unit
  room_001.temp,2025-10-20T00:00:00+08:00,23.15,DEG_C
  ```

> 结论：Brick + QUDT 的语义闭环建立了。SHACL 能认、后续推理/聚合也能认。旧版缺失这一层，语义更“脆”。


## 4. IAQ/环境质量的语义建模更加贴近真实场景
**旧版：**
- 有温度、湿度、照度、噪声，还有两个开关（灯/空调），但没有空气质量。
- 所有房间的传感器配置基本一致，缺少“哪个房间更关键”的场景差异。

**新版：**
- 引入了 CO₂ 和 PM2.5 传感器：
  - `brick:CO2_Level_Sensor`
  - `brick:PM2.5_Sensor`
- 定义了可测量量并建立继承关系：
  ```turtle
  bldg:CO2_Level a brick:Quantity ;
    rdfs:subClassOf brick:Air_Quality .

  bldg:PM25_Level a brick:Quantity ;
    rdfs:subClassOf brick:Air_Quality .
  ```
  然后传感器用 `brick:measures bldg:CO2_Level` / `bldg:PM25_Level`。
- 用 `rdfs:subClassOf brick:Air_Quality` 这一点非常关键：
  - 这让 SPARQL/推理器可以把 CO₂ 和 PM2.5 都自动归类为空气质量指标。
  - 可以直接问“给我所有空气质量相关传感器”，得到两种传感器的合集。
- 不是每个房间都有 IAQ 传感器。只有特定房间类型（办公室、会议室、茶水间等）才会生成 CO₂ / PM2.5。
  - `IAQ_ELIGIBLE_TYPES = {"Office", "Conference_Room", "Office_Kitchen"}`

> 结论：新版不仅支持“空气质量”语义查询，还体现了真实部署：并不是每个房间都装 IAQ 传感器。旧版是完全均一、理想化、假数据分布。


## 5. 时序数据更像真实BMS：多天、多分辨率、昼夜/上班模式清晰
**旧版：**
- 只生成“昨天”的 24 个时间点（整点），也就是每小时1个值。
- 各指标基本是随机均匀采样，除了照度和开关稍微参考了上班时间。
- 没有时区信息；时间戳是 naive 的 ISO 字符串。

**新版：**
- 支持多天窗口：`[today - N 天, tomorrow)`，通过 `--days-back` 控制，默认 7 天。
- 支持可调采样密度：`--points-per-day`，可以是 96（每15分钟），而不是死板的 24。
- 生成逻辑是“平滑日型曲线 + 抖动”而不是单纯随机：
  - 温度围绕 23℃，带有昼夜波动 + 每个房间自己的轻微偏移。
  - 湿度随时间反相波动。
  - 照度：白天高、夜间低。
  - CO₂ / PM2.5：工作时间（9~18点）上升，非工作时间回落。
- 每个点再加 `jitter()` 高斯扰动，避免数据太完美。
- 时间戳是带时区偏移的 ISO 格式，比如 `2025-10-20T00:00:00+08:00`，严格对齐生成时所用的 IANA 时区（默认 `Asia/Shanghai`）。

> 结论：这些数据可以直接用来回答“昨天 vs 前天”、“上班时间的平均 CO₂ 是多少”、“最近一周温度趋势如何”这种自然语言问题。旧版因为只有 1 天随机值，无法支持趋势/对比类问答。


## 6. CLI 参数化 & 可复现性：从“一次性脚本”变成“通用数据生成器”
**旧版：**
- 常量都硬编码在脚本里（房间数量、间隔、时区等）。
- 没有 seed 控制，没法复现同一数据集。

**新版：**
- 用 `argparse` 做成真正的命令行工具：
  - `--out-dir` 指定输出目录
  - `--timezone` 指定 IANA 时区（默认 `Asia/Shanghai`）
  - `--num-rooms`、`--points-per-day`、`--days-back`
  - `--seed` 控制随机性（可以是 int 或 str）
  - `--use-rec` 切换 Brick vs REC 空间类体系
  - `--no-iaq` 可一键禁用 IAQ 传感器
  - `--no-unit-in-csv` 控制 CSV 是否包含单位列
- 随机性被拆成两层：
  - 房间的元数据（房间类型、面积等）靠 `random.Random(f"{seed}_{i}_room")`，所以在同一个 seed 下，Room_042 永远是同一个类型/面积。
  - 全局 `random.seed(seed_val)` 负责时序抖动。
- 输出是两个明确定义的文件：
  - `topology.ttl`：楼宇/房间/传感器/时序绑定的知识图谱（Brick/REC兼容）
  - `timeseries.csv`：长表格式的时序值 + 单位列

> 结论：新版是“我可以交付给别人、他们一键生成同样/可控变化的数据集”的工具。旧版更像“我本地临时跑一下而已”。


## 7. SHACL 友好、对标官方 examples
**新版的一个关键目标就是：**
- 参考了 Brick 官方仓库 `examples/` 的结构和命名习惯（`brick:isPointOf`、`ref:hasTimeseriesReference`、`qudt:Unit`、`brick:area [...]`等）。
- 数据可以通过 Brick 的 SHACL 校验，而不会报出严重的语义错误（比如缺单位、未声明量纲、时序参考没有挂到传感器上等）。

这点在科研/面试里很重要，因为可以证明两件事：
1. 你不是在造一个“看起来像 Brick 的私有格式”，你是在对齐 Brick 社区通用的建模手法。
2. 你的数据集已经可以作为基准数据集，用来测试：
   - LangGraph + RAG 的问答
   - 自然语言 → SPARQL 查询
   - 时序统计（均值、最大值、趋势等）
   - 传感器部署稀疏性推理（哪些房间没有 IAQ？）

旧版达不到这一点。


## 8. 用一句话总结
- **旧版**：一份 demo 级的“房间+随机传感器值”脚本，结构单一，主要用来喂本地测试。
- **新版**：一套接近真实楼宇知识图谱的数据生成管线。它：
  - 建模了 Campus/Building/Floor/Room/Area/Usage 的空间层级；
  - 标准化了传感器-时序的绑定（`ref:hasTimeseriesReference`）；
  - 显式给出了单位、量纲、QUDT 对象，满足 SHACL；
  - 引入空气质量（CO₂ / PM2.5）并把它们语义地挂到 `brick:Air_Quality`；
  - 生成多天、多分辨率、带工作日行为模式的时序曲线；
  - 通过 CLI 参数化 + seed，实现可复现、可扩展的基准数据集。

> 直接拿去给老师/面试官说的话就是：
> “我现在不只是做了一个玩具脚本，我实现了一个可复现的数据集生成器，它输出的 Brick/REC 拓扑和时序数据基本符合社区标准、能过 SHACL，而且可以支持自然语言→SPARQL→统计问答这种完整闭环。”


