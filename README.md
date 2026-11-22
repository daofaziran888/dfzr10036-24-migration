# 流变工坊 · 实时CDC与流式数据同步平台

面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/views/CommitMigration.vue    提交清单迁移面板（恢复/比对/回执核对）
│   ├── src/data/modules.ts              业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/commit.py    提交清单迁移专用接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/commit_recovery.py  提交失败恢复与数据比对
│   ├── app/services/base.py     共享服务骨架
│   ├── app/fixtures/commit_manifest.py  迁移夹具（A/BC 快照与 a/b/c/d 证据）
│   ├── app/db.py                SQLite 持久化
│   ├── app/store.py             可重启的业务仓库（种子 + 夹具）
│   ├── scripts/demo_commit_migration.py  迁移流程演示脚本
│   └── tests/                   迁移回归测试
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| CDC数据源 | `cdc_source` | 链路源端 | 源端编号、数据库类型、连接地址 |
| 日志解析 | `binlog_parse` | 解析规则 | 规则编号、源端、日志格式 |
| 事件转换 | `event_trans` | 转换管道 | 管道编号、源端、事件类型 |
| 维表关联 | `dim_join` | 关联配置 | 配置编号、事实表、维度表 |
| 水位管理 | `watermark_mgr` | 水位线 | 水位编号、数据流、当前位点 |
| 乱序处理 | `out_of_order` | 乱序策略 | 策略编号、数据流、时间字段 |
| 结果落盘 | `sink_writer` | 落盘管道 | 管道编号、数据流、目标载体 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标编号、数据流、源端位点 |
| DDL处理 | `ddl_handle` | DDL适配 | 适配编号、源端、DDL类型 |
| 断点恢复 | `checkpoint` | 检查点 | 检查点编号、数据流、保存位点 |
| Schema演进 | `schema_evo` | 演进记录 | 记录编号、数据流、变更列 |
| 流去重 | `dedup_stream` | 去重窗口 | 窗口编号、数据流、去重键 |
| 湖仓提交 | `iceberg_commit` | 提交快照 | 快照编号、目标表、数据文件/数据文件清单 |
| 文件证据 | `file_evidence` | 文件证据 | 文件路径、端、证据类型、记录数 |
| 快照文件 | `commit_file` | 快照文件 | 文件路径、快照编号、清单版本、比对状态 |
| 确认回执 | `commit_receipt` | 确认回执 | 回执编号、快照编号、文件路径、状态 |
| 差异行 | `data_diff_row` | 差异行 | 快照编号、清单版本、文件路径、业务键 |
| 恢复窗口 | `commit_recovery_run` | 恢复窗口 | 窗口、快照编号、实际处理范围、延后缺证范围 |
| 小文件合并 | `compaction` | 合并任务 | 任务编号、目标表、文件数量 |
| 快照过期 | `expire_snap` | 过期策略 | 策略编号、目标表、保留数量 |
| 流聚合 | `metric_aggr` | 聚合窗口 | 窗口编号、数据流、聚合键 |
| 增量回填 | `backfill_run` | 回填任务 | 任务编号、目标表、起始位点 |
| 告警规则 | `alert_rule` | 告警策略 | 策略编号、监控指标、告警条件 |
| 数据比对 | `data_diff` | 比对任务 | 任务编号、源端、目标端 |
| 编排调度 | `task_schedule` | 调度计划 | 计划编号、关联链路、Cron表达式 |
| 流审计 | `stream_audit` | 审计记录 | 记录编号、数据流、操作类型 |
| 健康报告 | `stream_health` | 健康评估 | 评估编号、数据流、健康分数 |

## 提交文件清单迁移（v1 单路径 -> v2 清单）

湖仓提交数据文件从旧版单路径 `数据文件` 迁移成 v2 `数据文件清单`，回退期间
同一存储必须同时容纳两种格式。迁移夹具 `app/fixtures/commit_manifest.py`
预置了订单旧快照 A（`a.parquet`，v1）与新快照 BC（`b.parquet`、`c.parquet`，v2），
并在源端/目标端注入可区分记录，使 a、b、c、d 分别产出不同业务键的差异行。

存储默认落在 `backend/var/flowforge.sqlite3`（可用 `APP_DB_PATH` 覆盖），
进程重启后快照、证据、回执和差异行原样可读。首次启动写入种子 + 夹具，
重复启动只加载既有数据。

迁移接口（`app/routers/commit.py`）：

| 接口 | 说明 |
| --- | --- |
| `GET /api/commit/state` | 写入端版本、启动次数、快照与清单版本 |
| `POST /api/commit/recovery?window=W-0001&commit_entry_id=2` | 提交失败恢复；同一窗口每份快照只有一次机会，重复调用回放首次结果 |
| `GET /api/commit/comparison` | 逐文件比对结果与差异行（关联快照/清单版本/文件/首次出现时间） |
| `POST /api/commit/evidence` | 补交某端证据；只接受快照清单里记录过的路径 |
| `POST /api/commit/receipts` | 登记单文件确认回执 |
| `POST /api/commit/writer-version?version=v1` | 回退/切换写入端版本 |
| `POST /api/commit/reset` | 恢复种子 + 夹具初始状态 |

行为约束：

- 恢复只按快照自身记录的路径取数，不接受当前管道里的同名文件。
- 已生效的单文件确认回执优先：该文件不再消费，也不会重复写差异行。
- 同一快照只有所有文件都完成比对才标记 `比对完成`；缺证/回执跳过时为 `部分完成`。
- 旧版入口（只给 `数据文件`）仍登记为 v1 单路径快照；多路径清单登记为 v2。

演示与回归：

```bash
cd backend
.venv/bin/python -m scripts.demo_commit_migration   # 按题面顺序跑一遍
.venv/bin/python -m unittest discover -s tests      # 迁移回归测试
```

前端“提交清单迁移”页面（`/commit_migration`）展示快照、逐文件消费次数、
恢复窗口与差异行。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
