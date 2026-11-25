"""change_capture 领域服务：变更数据捕获（CDC）管理。

业务逻辑：
1. CDC配置：源数据库Binlog位点→过滤规则→目标Topic输出
2. 变更捕获链路：change_capture → schema_govern（变更Schema演进）
3. 失败场景：Binlog位点丢失、过滤规则误配、写入目标Topic失败、延迟追赶
4. 与上下游流水线：为 schema_govern 提供变更元信息

跨模块数据流：
  change_capture → schema_govern（捕获到的变更驱动Schema演进）
  change_capture → changelog（变更记录写入全局变更日志）

事务级投递（多条事件一次提交）：
- 按源库、日志段、事务标识整笔接收，发送回执持久化提交位置与事务内顺序；
- 逐事件、逐表记录写入确认，确认状态回写投递清单；
- 投递失败重试同一事务时只补齐未确认部分，已确认事件/表不重复写；
- 相同事务标识收到不同内容时登记冲突并保留，独立配置的交付不受影响；
- 捕获确认位置只按连续完成的事务推进，后续事务先送达也不跳过较早缺口。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ChangeCaptureService(BaseModuleService):
    MODULE = "change_capture"
    ENTITY = "变更捕获配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置编号", "源数据库", "目标表", "捕获模式", "Binlog位点", "过滤规则", "变更类型", "目标Topic"]
    REQUIRED_FIELDS = ["配置编号", "源数据库", "目标表"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已暂停"]
    ACTION_RULES = {"提交验证": "待验证", "启动捕获": "运行中", "暂停捕获": "已暂停"}
    NEGATIVE_ACTIONS = []

    # 跨模块流水线
    PIPELINE = "cdc_capture_pipeline"
    DOWNSTREAM_MODULES = ["schema_govern", "changelog"]
    PRODUCES_ARTIFACT = "CDC变更事件流"

    # 已捕获位置跟踪（保留旧的逐事件捕获进度）
    _capture_progress: dict[int, dict[str, Any]] = {}

    # ============================================================
    # 旧的逐事件捕获（保留兼容，新链路走 receive_transaction）
    # ============================================================
    def capture_changes(self, entry_id: int) -> dict[str, Any]:
        """执行CDC变更捕获：模拟从Binlog读取变更事件。

        失败场景：
        - 源连接断开 → Binlog位点丢失
        - 过滤规则错误 → 捕获到不应捕获的表
        - 写入目标Topic失败 → 中断并重试
        - 延迟追赶 → 位点落后
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}
        if entry.get("status") != "运行中":
            return {"ok": False, "reason": f"捕获未运行，当前状态: {entry.get('status')}"}

        # 获取上次捕获位点
        progress = self._capture_progress.get(entry_id, {"binlog_pos": 0, "captured_count": 0})
        current_pos = progress.get("binlog_pos", 0)
        target_table = str(entry.get("目标表", ""))

        # 模拟捕获10个变更事件
        changes = []
        dirty_events = []
        new_pos = current_pos + 1000

        for i in range(10):
            change_type = ["INSERT", "UPDATE", "DELETE"][i % 3]
            event = {
                "binlog_pos": new_pos + i * 100,
                "table": target_table,
                "type": change_type,
                "ts": time.time() - i * 2,
                "source_db": entry.get("源数据库"),
            }
            # 模拟模拟过滤规则误配
            if i == 5 and "exclude" in str(entry.get("过滤规则", "")).lower():
                dirty_events.append({"event": event, "issue": "过滤规则误配: 排除了应捕获的变更"})
                continue
            changes.append(event)

        # 模拟写入目标Topic失败
        topic = str(entry.get("目标Topic", ""))
        if not topic:
            return {"ok": False, "reason": "目标Topic为空，写入失败"}

        # 更新进度
        self._capture_progress[entry_id] = {"binlog_pos": new_pos + 1000, "captured_count": progress["captured_count"] + len(changes), "last_ts": time.time()}

        # 写入全局变更日志
        for ch in changes:
            store.append_changelog({
                "目标表": target_table,
                "变更类型": ch["type"],
                "变更内容": str(ch),
                "操作时间": ch["ts"],
                "操作来源": "CDC_capture",
                "影响行数": 1,
                "回滚版本": "",
            })

        # 注册Schema变更（如果捕获到新字段）
        store.register_schema(target_table, {"fields": ["id", "name", "ts", "status"], "version": "v1"}, f"v1_{new_pos}")

        batch_id = f"CDC_{entry_id}_{int(time.time())}"
        self.produce_artifact(changes, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "captured": len(changes),
            "dirty": len(dirty_events),
            "new_binlog_pos": new_pos + 1000,
            "target_table": target_table,
            "schema_updated": True,
        }

    def get_capture_lag(self, entry_id: int) -> dict[str, Any]:
        """获取捕获延迟信息"""
        progress = self._capture_progress.get(entry_id)
        if progress is None:
            return {"ok": False, "reason": "无捕获记录"}
        lag = time.time() - progress.get("last_ts", 0)
        return {
            "ok": True,
            "entry_id": entry_id,
            "lag_seconds": round(lag, 1),
            "captured_count": progress.get("captured_count", 0),
            "last_binlog_pos": progress.get("binlog_pos", 0),
        }

    # ============================================================
    # 事务级批量投递：整笔事务一次接收，回执持久化逐条/逐表确认
    # ============================================================
    @staticmethod
    def _fingerprint(payload: Any) -> str:
        """对事务/事件内容计算稳定指纹，用于识别同事务标识下的内容冲突。"""
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _validate_txn_payload(self, entry: dict[str, Any], payload: dict[str, Any]) -> str | None:
        if entry is None:
            return "捕获配置不存在"
        if entry.get("status") != "运行中":
            return f"捕获未运行，当前状态: {entry.get('status')}"
        if not str(payload.get("txn_id") or "").strip():
            return "缺少事务标识 txn_id"
        events = payload.get("events")
        if not isinstance(events, list) or not events:
            return "事务必须携带至少一条事件 events"
        for idx, ev in enumerate(events):
            if not str(ev.get("table") or "").strip():
                return f"事件 {idx} 缺少所在表 table"
            if not str(ev.get("type") or "").strip():
                return f"事件 {idx} 缺少变更类型 type"
        return None

    def receive_transaction(self, entry_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """按源库、日志段、事务标识批量接收一整笔变更。

        多条事件一次提交；提交位置（commit_pos）与事务内顺序（seq）持久化
        在发送回执（投递清单）中，并逐条事件、逐张表记录写入确认。
        重试同一事务只补齐未确认部分；同事务标识不同内容登记冲突并保留。
        """
        entry = store.find(self.MODULE, entry_id)
        error = self._validate_txn_payload(entry, payload)
        if error:
            return {"ok": False, "reason": error}

        txn_id = str(payload["txn_id"]).strip()
        log_segment = str(payload.get("log_segment") or "")
        commit_pos = int(payload.get("commit_pos", 0))
        source_db = str(entry.get("源数据库", ""))
        incoming_events = sorted(payload["events"], key=lambda ev: int(ev.get("seq", 0)))
        incoming_fp = self._fingerprint([
            {k: ev.get(k) for k in ("seq", "table", "type", "data")} for ev in incoming_events
        ])

        existing = store.find_cdc_delivery(entry_id, txn_id)
        now = time.time()

        # 同一事务标识重复送达：内容一致走重试补齐，内容不同保留冲突
        if existing is not None:
            if existing["content_fingerprint"] != incoming_fp:
                conflict = {
                    "config_id": entry_id,
                    "txn_id": txn_id,
                    "log_segment": log_segment,
                    "commit_pos": commit_pos,
                    "original_delivery_id": existing["delivery_id"],
                    "original_fingerprint": existing["content_fingerprint"],
                    "incoming_fingerprint": incoming_fp,
                    "incoming_events": incoming_events,
                    "ts": now,
                }
                store.record_cdc_conflict(conflict)
                existing["conflict"] = True
                return {
                    "ok": False,
                    "reason": "事务内容冲突：相同事务标识携带不同内容，已保留冲突记录",
                    "txn_id": txn_id,
                    "conflict": True,
                    "original_delivery_id": existing["delivery_id"],
                }
            return self._retransmit(existing, incoming_events, now)

        delivery_id = f"DLV-{entry_id}-{txn_id}-{int(now * 1000)}"
        manifest: dict[str, Any] = {
            "delivery_id": delivery_id,
            "config_id": entry_id,
            "source_db": source_db,
            "txn_id": txn_id,
            "log_segment": log_segment,
            "commit_pos": commit_pos,
            "content_fingerprint": incoming_fp,
            "received_ts": now,
            "updated_ts": now,
            "status": "部分确认",
            "conflict": False,
            "events": [],          # 逐条事件的写入确认槽位
            "tables": {},          # 每张涉及表的写入确认
            "attempts": 1,
        }
        store.save_cdc_delivery(manifest)

        # 先建好全部事件槽位（含事务内顺序），保证半笔事务也可被查询识别
        for seq, ev in enumerate(incoming_events, start=1):
            event_key = self._fingerprint(
                {k: ev.get(k) for k in ("table", "type", "data")}
            )
            manifest["events"].append({
                "seq": int(ev.get("seq", seq)),
                "event_key": event_key,
                "table": str(ev["table"]),
                "type": str(ev["type"]),
                "data": ev.get("data"),
                "changelog_written": False,
                "table_write_confirmed": False,
                "confirmed": False,
                "fail_reason": "",
            })
            manifest["tables"].setdefault(str(ev["table"]), {
                "table": str(ev["table"]), "confirmed": False,
                "confirmed_events": 0, "total_events": 0, "fail_reason": "",
            })
            manifest["tables"][str(ev["table"])]["total_events"] += 1

        self._apply_events(manifest, incoming_events, now)
        store.save_cdc_delivery(manifest)
        return self._receipt(manifest)

    def _apply_events(self, manifest: dict[str, Any], events: list[dict[str, Any]], now: float) -> None:
        """逐事件核对并保存：已确认的跳过（幂等），未确认的补齐写入确认。"""
        slots = {slot["seq"]: slot for slot in manifest["events"]}
        attempts = manifest.get("attempts", 1)
        for ev in events:
            seq = int(ev.get("seq", 0))
            slot = slots.get(seq)
            if slot is None:
                continue
            if slot["confirmed"]:
                # 重试只补齐未确认部分，已确认事件不重复落库、不重复记日志
                continue

            fail_tables = {str(t) for t in (ev.get("fail_tables") or [])}
            table_name = slot["table"]
            table_state = manifest["tables"][table_name]

            # 表写入失败：变更未落地，不写变更日志，等待重传补齐
            if table_name in fail_tables:
                slot["table_write_confirmed"] = False
                slot["changelog_written"] = False
                slot["fail_reason"] = f"目标表 {table_name} 写入失败"
                table_state["confirmed"] = False
                table_state["fail_reason"] = slot["fail_reason"]
                slot["confirmed"] = False
                continue

            # 表写入成功后，变更日志逐条、幂等保存（重传不会产生第二条）
            dedup_key = (manifest["delivery_id"], slot["event_key"])
            written = store.append_changelog_once({
                "目标表": table_name,
                "变更类型": slot["type"],
                "变更内容": json.dumps(slot.get("data"), ensure_ascii=False, default=str),
                "操作时间": now,
                "操作来源": "CDC_txn_delivery",
                "影响行数": 1,
                "回滚版本": "",
                "事务标识": manifest["txn_id"],
                "投递编号": manifest["delivery_id"],
                "事务内顺序": slot["seq"],
                "日志段": manifest["log_segment"],
                "提交位置": manifest["commit_pos"],
            }, dedup_key)
            slot["changelog_written"] = slot["changelog_written"] or written
            slot["table_write_confirmed"] = True
            slot["fail_reason"] = ""
            table_state["confirmed_events"] += 1
            if table_state["confirmed_events"] >= table_state["total_events"]:
                table_state["confirmed"] = True
                table_state["fail_reason"] = ""

            slot["confirmed"] = bool(slot["changelog_written"] and slot["table_write_confirmed"])

        manifest["attempts"] = attempts + 1
        manifest["updated_ts"] = now
        all_confirmed = all(slot["confirmed"] for slot in manifest["events"])
        manifest["status"] = "已确认" if all_confirmed else "部分确认"

    def _retransmit(self, manifest: dict[str, Any], events: list[dict[str, Any]], now: float) -> dict[str, Any]:
        """投递失败后重试原事务：只补齐未确认的事件与表写入。"""
        before = sum(1 for slot in manifest["events"] if slot["confirmed"])
        self._apply_events(manifest, events, now)
        after = sum(1 for slot in manifest["events"] if slot["confirmed"])
        store.save_cdc_delivery(manifest)
        receipt = self._receipt(manifest)
        receipt["retry"] = True
        receipt["replayed_events"] = len(events)
        receipt["newly_confirmed"] = after - before
        receipt["already_confirmed_skipped"] = before
        return receipt

    def retransmit_transaction(self, entry_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """重传入口：按配置与事务标识找到原投递清单后补齐未确认部分。"""
        txn_id = str(payload.get("txn_id") or "").strip()
        if not txn_id:
            return {"ok": False, "reason": "缺少事务标识 txn_id"}
        manifest = store.find_cdc_delivery(entry_id, txn_id)
        if manifest is None:
            return {"ok": False, "reason": f"事务 {txn_id} 无历史投递记录，无法重传，请走整笔接收"}
        events = payload.get("events")
        if not isinstance(events, list) or not events:
            return {"ok": False, "reason": "重传必须携带原事务事件 events"}
        if self._fingerprint([
            {k: ev.get(k) for k in ("seq", "table", "type", "data")}
            for ev in sorted(events, key=lambda ev: int(ev.get("seq", 0)))
        ]) != manifest["content_fingerprint"]:
            store.record_cdc_conflict({
                "config_id": entry_id,
                "txn_id": txn_id,
                "log_segment": manifest["log_segment"],
                "commit_pos": manifest["commit_pos"],
                "original_delivery_id": manifest["delivery_id"],
                "original_fingerprint": manifest["content_fingerprint"],
                "incoming_fingerprint": self._fingerprint(events),
                "incoming_events": events,
                "ts": time.time(),
            })
            manifest["conflict"] = True
            return {
                "ok": False,
                "reason": "重传内容与原事务不一致，已保留冲突记录",
                "txn_id": txn_id,
                "conflict": True,
            }
        return self._retransmit(manifest, sorted(events, key=lambda ev: int(ev.get("seq", 0))), time.time())

    # ============================================================
    # 投递查询：半笔事务不得被列为成功
    # ============================================================
    def query_delivery(self, entry_id: int, txn_id: str) -> dict[str, Any]:
        manifest = store.find_cdc_delivery(entry_id, str(txn_id))
        if manifest is None:
            return {"ok": False, "reason": f"事务 {txn_id} 未投递"}
        return {
            "ok": True,
            "delivery_id": manifest["delivery_id"],
            "txn_id": manifest["txn_id"],
            "log_segment": manifest["log_segment"],
            "commit_pos": manifest["commit_pos"],
            "status": manifest["status"],
            "fully_confirmed": manifest["status"] == "已确认",
            "confirmed_events": sum(1 for s in manifest["events"] if s["confirmed"]),
            "total_events": len(manifest["events"]),
            "tables": list(manifest["tables"].values()),
            "events": [
                {
                    "seq": s["seq"], "table": s["table"], "type": s["type"],
                    "changelog_written": s["changelog_written"],
                    "table_write_confirmed": s["table_write_confirmed"],
                    "confirmed": s["confirmed"], "fail_reason": s["fail_reason"],
                }
                for s in manifest["events"]
            ],
            "attempts": manifest["attempts"],
            "conflict": manifest["conflict"],
        }

    def list_deliveries(self, entry_id: int) -> dict[str, Any]:
        rows = store.cdc_deliveries_for_config(entry_id)
        return {
            "ok": True,
            "config_id": entry_id,
            "total": len(rows),
            "items": [
                {
                    "delivery_id": m["delivery_id"], "txn_id": m["txn_id"],
                    "log_segment": m["log_segment"], "commit_pos": m["commit_pos"],
                    "status": m["status"],
                    "confirmed_events": sum(1 for s in m["events"] if s["confirmed"]),
                    "total_events": len(m["events"]),
                    "tables": list(m["tables"].values()),
                    "conflict": m["conflict"],
                }
                for m in rows
            ],
        }

    def query_conflicts(self, entry_id: int) -> dict[str, Any]:
        conflicts = store.cdc_conflicts_for_config(entry_id)
        return {
            "ok": True,
            "config_id": entry_id,
            "conflict_count": len(conflicts),
            "items": [
                {
                    "txn_id": c["txn_id"], "log_segment": c["log_segment"],
                    "commit_pos": c["commit_pos"],
                    "original_delivery_id": c["original_delivery_id"],
                    "original_fingerprint": c["original_fingerprint"],
                    "incoming_fingerprint": c["incoming_fingerprint"],
                    "ts": c["ts"],
                }
                for c in conflicts
            ],
        }

    # ============================================================
    # 源日志段事务索引 + 连续确认水位 + 缺口续采
    # ============================================================
    def stage_log_transactions(self, entry_id: int, transactions: list[dict[str, Any]]) -> dict[str, Any]:
        """登记源库日志段中按提交位置排序的事务，用于识别正确缺口。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}
        added = store.stage_cdc_transactions(entry_id, transactions)
        return {
            "ok": True,
            "config_id": entry_id,
            "indexed": len(store.cdc_log_index(entry_id)),
            "newly_added": added,
        }

    def get_delivery_progress(self, entry_id: int) -> dict[str, Any]:
        """捕获确认位置：只按连续完成（已确认）的事务推进。

        后一个事务即使已送达/已确认，只要较早事务缺失或未确认，水位停在缺口之前。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"配置 {entry_id} 不存在"}

        manifests = {m["txn_id"]: m for m in store.cdc_deliveries_for_config(entry_id)}
        index = store.cdc_log_index(entry_id)

        # 未登记源日志索引时，按已投递事务的提交位置排序推导
        ordered = index or [
            {"txn_id": m["txn_id"], "log_segment": m["log_segment"],
             "commit_pos": m["commit_pos"], "event_count": len(m["events"])}
            for m in manifests.values()
        ]
        ordered = sorted(ordered, key=lambda item: item["commit_pos"])

        watermark_pos = 0
        watermark_txn = ""
        gap: dict[str, Any] | None = None
        delivered = 0
        confirmed = 0
        for item in ordered:
            manifest = manifests.get(item["txn_id"])
            if manifest is None:
                if gap is None:
                    gap = {
                        "txn_id": item["txn_id"], "log_segment": item["log_segment"],
                        "commit_pos": item["commit_pos"], "reason": "事务尚未送达",
                    }
                continue
            delivered += 1
            if manifest["status"] == "已确认":
                confirmed += 1
            if gap is None and manifest["status"] == "已确认":
                # 仅当位于连续前缀上时才推进水位
                watermark_pos = item["commit_pos"]
                watermark_txn = item["txn_id"]
            elif gap is None:
                gap = {
                    "txn_id": item["txn_id"], "log_segment": item["log_segment"],
                    "commit_pos": item["commit_pos"], "reason": "事务仅部分确认",
                }

        return {
            "ok": True,
            "config_id": entry_id,
            "source_db": entry.get("源数据库"),
            "confirmed_watermark_pos": watermark_pos,
            "confirmed_watermark_txn": watermark_txn,
            "gap": gap,
            "indexed_transactions": len(ordered),
            "delivered_transactions": delivered,
            "confirmed_transactions": confirmed,
        }

    def resume_capture(self, entry_id: int) -> dict[str, Any]:
        """读取捕获进度并从正确缺口续采；后事务先送达不得跳过较早缺口。"""
        progress = self.get_delivery_progress(entry_id)
        if not progress["ok"]:
            return progress
        gap = progress["gap"]
        if gap is None:
            return {
                "ok": True,
                "config_id": entry_id,
                "resume_from_pos": progress["confirmed_watermark_pos"],
                "resume_from_txn": progress["confirmed_watermark_txn"],
                "message": "无缺口，从当前确认水位继续",
            }
        return {
            "ok": True,
            "config_id": entry_id,
            "resume_from_pos": gap["commit_pos"],
            "resume_from_txn": gap["txn_id"],
            "resume_from_segment": gap["log_segment"],
            "gap_reason": gap["reason"],
            "watermark_pos": progress["confirmed_watermark_pos"],
            "message": "从最早未确认缺口接回，后续已送达事务不跳过该缺口",
        }

    # ============================================================
    # 回执与快照
    # ============================================================
    def _receipt(self, manifest: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": True,
            "delivery_id": manifest["delivery_id"],
            "config_id": manifest["config_id"],
            "source_db": manifest["source_db"],
            "txn_id": manifest["txn_id"],
            "log_segment": manifest["log_segment"],
            "commit_pos": manifest["commit_pos"],
            "status": manifest["status"],
            "attempts": manifest["attempts"],
            "events": [
                {
                    "seq": s["seq"], "table": s["table"], "type": s["type"],
                    "changelog_written": s["changelog_written"],
                    "table_write_confirmed": s["table_write_confirmed"],
                    "confirmed": s["confirmed"], "fail_reason": s["fail_reason"],
                }
                for s in manifest["events"]
            ],
            "tables": list(manifest["tables"].values()),
            "confirmed_events": sum(1 for s in manifest["events"] if s["confirmed"]),
            "total_events": len(manifest["events"]),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["capture_progress_tracking"] = len(self._capture_progress)
        base["global_changelog_entries"] = len(store._global_changelog)
        base["txn_delivery_count"] = len(store._cdc_deliveries)
        base["txn_conflict_count"] = len(store._cdc_conflicts)
        return base
