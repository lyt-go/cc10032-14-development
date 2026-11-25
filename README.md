# 流批一体数据集成平台

面向实时流接入、批流合并、变更捕获与分区治理的流批一体数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
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
| 流式数据源 | `stream_source` | 流式源配置 | 源标识、消息中间件、Topic名称 |
| 批次分片 | `batch_slice` | 分片批次 | 批次编号、源表名称、分片键列 |
| 变更捕获 | `change_capture` | 变更捕获配置 | 配置编号、源数据库、目标表 |
| Schema治理 | `schema_govern` | Schema版本 | 主题名称、Schema定义、版本号 |
| 水位管理 | `watermark` | 消费水位 | 分区标识、消费组、当前位点 |
| 迟到数据 | `late_data` | 迟到数据策略 | 策略编号、关联源、允许延迟秒数 |
| 流式关联 | `stream_lookup` | 关联规则 | 规则编号、左流来源、右表来源 |
| 合并策略 | `merge_plan` | 合并计划 | 计划编号、流式来源、批式来源 |
| 分区扩缩 | `partition_scale` | 分区策略 | 策略编号、目标表、分区字段 |
| 快照管理 | `snapshot_mgr` | 数据快照 | 快照编号、源表名称、快照时间 |
| 数据对账 | `data_reconcile` | 对账任务 | 任务编号、左源表、右源表 |
| 回填作业 | `backfill_job` | 回填任务 | 任务编号、目标表、回填起始时间 |
| 冷热分层 | `cold_archive` | 归档策略 | 策略编号、源表名称、热数据天数 |
| 压缩合并 | `compaction` | 压缩任务 | 任务编号、目标表、目标分区 |
| 血缘图 | `lineage` | 血缘关系 | 关系编号、上游表、下游表 |
| 变更日志 | `changelog` | 变更记录 | 记录编号、目标表、变更类型 |
| 保留管理 | `retention` | 保留策略 | 策略编号、目标表、保留天数 |
| 表格式 | `table_format` | 表格式配置 | 配置编号、目标表、表格式类型 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 变更捕获：事务级批量投递

`change_capture` 支持按源库、日志段、事务标识整笔接收变更（多条事件一次提交），
替代逐事件写日志的旧链路（`capture_changes` 保留兼容）。固定前缀 `/api/cdc`：

| 接口 | 说明 |
| --- | --- |
| `POST /api/cdc/configs/{id}/transactions` | 整笔事务接收，回执持久化提交位置 `commit_pos`、日志段与事务内顺序 `seq`，逐条事件/逐表写入确认回写投递清单 |
| `POST /api/cdc/configs/{id}/retransmit` | 重传原事务，只补齐未确认事件与表写入，已确认部分跳过，变更日志按事件幂等不重复 |
| `GET /api/cdc/configs/{id}/deliveries` | 投递清单；半笔事务为「部分确认」，不会被列为成功 |
| `GET /api/cdc/configs/{id}/deliveries/{txn_id}` | 单笔事务逐条确认状态复核 |
| `GET /api/cdc/configs/{id}/progress` | 确认水位：只按连续完成的事务推进，返回最早缺口 |
| `GET /api/cdc/configs/{id}/resume` | 续采位置：后事务先送达也不跳过较早未确认事务 |
| `POST /api/cdc/configs/{id}/log-index` | 登记源日志段内按提交位置排序的事务索引 |
| `GET /api/cdc/configs/{id}/conflicts` | 同事务标识不同内容的冲突记录（保留冲突，独立配置交付不受影响） |

事件级故障注入：在 event 上带 `fail_tables` 可模拟该表写入失败，用于验证半笔
事务查询、重传补齐与水位停驻。验收测试见
`backend/tests/test_cdc_transaction_delivery.py`。

领域：数据工程
子方向：数仓与数据湖
