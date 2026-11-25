"""CDC 事务级批量投递验收测试。

覆盖需求：
1. 整笔事务多事件一次提交，提交位置与事务内顺序持久化在发送回执；
   逐事件、逐表写入确认回写投递清单；失败重试只补齐未确认部分；
   半笔事务第一次查询不得被列为成功；补齐后两表按同一确认位置复核；
   变更日志每条事件只记一次。
2. 同配置后一个事务先送达时，确认水位不跳过较早未确认事务，
   续采从正确缺口接回。
3. 相同事务标识不同内容保留冲突，独立配置仍可继续交付。
"""
from __future__ import annotations

import pytest

from app.services import service_for
from app.store import store


@pytest.fixture(autouse=True)
def reset_cdc_state():
    """每个用例前清空事务投递相关状态，避免共享内存仓库跨用例污染。"""
    store._cdc_deliveries.clear()
    store._cdc_conflicts.clear()
    store._cdc_log_index.clear()
    store._changelog_event_keys.clear()
    yield


def _running_config(config_id: int) -> dict:
    entry = store.find("change_capture", config_id)
    assert entry is not None
    entry["status"] = "运行中"
    return entry


def _order_txn(order_id: str = "O-1001") -> list[dict]:
    """同一笔订单事务：先写订单主表，再写订单明细表。"""
    return [
        {
            "seq": 1, "table": "orders", "type": "INSERT",
            "data": {"order_id": order_id, "amount": 200},
        },
        {
            "seq": 2, "table": "order_items", "type": "INSERT",
            "data": {"order_id": order_id, "item": "book", "qty": 2},
        },
    ]


class TestTransactionalDelivery:
    def test_half_txn_not_success_then_retransmit_completes(self):
        """场景一：第二张表写入失败 → 首次查询不是成功 → 重传补齐 → 同位置复核。"""
        cc = service_for("change_capture")
        _running_config(3)

        events = _order_txn()
        # 第一次投递：明细表写入失败（模拟半笔事务）
        events[1]["fail_tables"] = ["order_items"]
        receipt = cc.receive_transaction(3, {
            "txn_id": "TXN-ORDER-1",
            "log_segment": "mysql-bin.000100",
            "commit_pos": 8800,
            "events": events,
        })

        # 回执持久化了提交位置与事务内顺序
        assert receipt["ok"] is True
        assert receipt["commit_pos"] == 8800
        assert [e["seq"] for e in receipt["events"]] == [1, 2]
        assert receipt["status"] == "部分确认"
        assert receipt["confirmed_events"] == 1
        assert receipt["total_events"] == 2

        # 第一次查询：半笔事务绝不能被列为成功
        query = cc.query_delivery(3, "TXN-ORDER-1")
        assert query["ok"] is True
        assert query["fully_confirmed"] is False
        assert query["status"] != "已确认"
        tables = {t["table"]: t for t in query["tables"]}
        assert tables["orders"]["confirmed"] is True
        assert tables["order_items"]["confirmed"] is False
        slot_by_table = {e["table"]: e for e in query["events"]}
        assert slot_by_table["orders"]["table_write_confirmed"] is True
        assert slot_by_table["order_items"]["table_write_confirmed"] is False
        assert slot_by_table["order_items"]["fail_reason"]

        # 变更日志：首次只成功写入一条（每条事件最多一条日志）
        keys = [
            (receipt["delivery_id"], e["event_key"]) for e in
            store.find_cdc_delivery(3, "TXN-ORDER-1")["events"]
        ]
        assert sum(store.changelog_event_key_count(k) for k in keys) == 1

        # 先查询再重传：去掉故障注入，重传原事务
        retry = cc.retransmit_transaction(3, {
            "txn_id": "TXN-ORDER-1",
            "events": _order_txn(),
        })
        assert retry["ok"] is True
        assert retry["retry"] is True
        # 只补齐未确认部分：第一条已确认被跳过，第二条新确认
        assert retry["already_confirmed_skipped"] == 1
        assert retry["newly_confirmed"] == 1
        assert retry["status"] == "已确认"

        # 两张表均能按同一确认位置（同一投递编号/提交位置）复核
        final_query = cc.query_delivery(3, "TXN-ORDER-1")
        assert final_query["fully_confirmed"] is True
        assert final_query["commit_pos"] == 8800
        for table_state in final_query["tables"]:
            assert table_state["confirmed"] is True
        assert all(e["confirmed"] for e in final_query["events"])

        # 日志中每条事件只记一次（重传没有制造重复日志）
        manifest = store.find_cdc_delivery(3, "TXN-ORDER-1")
        keys = [(manifest["delivery_id"], s["event_key"]) for s in manifest["events"]]
        assert sum(store.changelog_event_key_count(k) for k in keys) == 2
        order_logs = [
            row for row in store._global_changelog
            if row.get("事务标识") == "TXN-ORDER-1"
        ]
        assert len(order_logs) == 2
        assert {row["目标表"] for row in order_logs} == {"orders", "order_items"}
        # 日志条目携带提交位置与事务内顺序
        assert all(row["提交位置"] == 8800 for row in order_logs)
        assert sorted(row["事务内顺序"] for row in order_logs) == [1, 2]

    def test_retry_is_idempotent_on_duplicate_delivery(self):
        """整笔事务重复送达（内容一致）等价重试，不产生重复日志、不重复确认。"""
        cc = service_for("change_capture")
        _running_config(3)
        payload = {
            "txn_id": "TXN-ORDER-DUP",
            "log_segment": "mysql-bin.000100",
            "commit_pos": 8900,
            "events": _order_txn("O-1002"),
        }
        first = cc.receive_transaction(3, payload)
        assert first["status"] == "已确认"
        second = cc.receive_transaction(3, dict(payload))
        assert second["ok"] is True
        assert second["retry"] is True
        assert second["newly_confirmed"] == 0
        assert second["already_confirmed_skipped"] == 2

        manifest = store.find_cdc_delivery(3, "TXN-ORDER-DUP")
        keys = [(manifest["delivery_id"], s["event_key"]) for s in manifest["events"]]
        assert sum(store.changelog_event_key_count(k) for k in keys) == 2
        assert len([
            r for r in store._global_changelog
            if r.get("事务标识") == "TXN-ORDER-DUP"
        ]) == 2


class TestOutOfOrderGap:
    def test_later_txn_delivered_first_watermark_waits_for_gap(self):
        """场景二：后一个事务先送达，水位不跳过较早事务，续采从缺口接回。"""
        cc = service_for("change_capture")
        _running_config(3)

        # 登记源日志段：T1(9100) → T2(9200) → T3(9300)
        staged = cc.stage_log_transactions(3, [
            {"txn_id": "TXN-GAP-1", "log_segment": "bin.000200", "commit_pos": 9100, "event_count": 2},
            {"txn_id": "TXN-GAP-2", "log_segment": "bin.000200", "commit_pos": 9200, "event_count": 1},
            {"txn_id": "TXN-GAP-3", "log_segment": "bin.000200", "commit_pos": 9300, "event_count": 1},
        ])
        assert staged["ok"] is True
        assert staged["indexed"] == 3

        # 后一个事务 T2、T3 先送达并全部确认，T1 完全缺失
        for txn_id, pos in (("TXN-GAP-2", 9200), ("TXN-GAP-3", 9300)):
            r = cc.receive_transaction(3, {
                "txn_id": txn_id, "log_segment": "bin.000200", "commit_pos": pos,
                "events": [{"seq": 1, "table": "orders", "type": "UPDATE",
                            "data": {"order_id": txn_id}}],
            })
            assert r["status"] == "已确认"

        # 读取捕获进度：已送达的后事务不能把较早未确认事务跳过
        progress = cc.get_delivery_progress(3)
        assert progress["confirmed_watermark_pos"] == 0
        assert progress["gap"] is not None
        assert progress["gap"]["txn_id"] == "TXN-GAP-1"
        assert progress["gap"]["commit_pos"] == 9100
        assert progress["delivered_transactions"] == 2
        assert progress["confirmed_transactions"] == 2

        # 续采必须从正确缺口接回
        resume = cc.resume_capture(3)
        assert resume["ok"] is True
        assert resume["resume_from_txn"] == "TXN-GAP-1"
        assert resume["resume_from_pos"] == 9100

        # 补齐缺口 T1 后，水位沿连续事务一次性推进到 T3
        cc.receive_transaction(3, {
            "txn_id": "TXN-GAP-1", "log_segment": "bin.000200", "commit_pos": 9100,
            "events": [
                {"seq": 1, "table": "orders", "type": "INSERT", "data": {"order_id": "g1"}},
                {"seq": 2, "table": "order_items", "type": "INSERT", "data": {"order_id": "g1"}},
            ],
        })
        progress2 = cc.get_delivery_progress(3)
        assert progress2["gap"] is None
        assert progress2["confirmed_watermark_pos"] == 9300
        assert progress2["confirmed_watermark_txn"] == "TXN-GAP-3"
        resume2 = cc.resume_capture(3)
        assert resume2["resume_from_pos"] == 9300

    def test_partial_txn_holds_watermark_until_retransmit(self):
        """较早事务仅部分确认时，水位同样停在该事务，重试补齐后推进。"""
        cc = service_for("change_capture")
        _running_config(3)
        cc.stage_log_transactions(3, [
            {"txn_id": "TXN-HOLD-1", "log_segment": "bin.000300", "commit_pos": 9400, "event_count": 2},
            {"txn_id": "TXN-HOLD-2", "log_segment": "bin.000300", "commit_pos": 9500, "event_count": 1},
        ])

        bad = _order_txn("O-HOLD")
        bad[1]["fail_tables"] = ["order_items"]
        cc.receive_transaction(3, {
            "txn_id": "TXN-HOLD-1", "log_segment": "bin.000300", "commit_pos": 9400,
            "events": bad,
        })
        cc.receive_transaction(3, {
            "txn_id": "TXN-HOLD-2", "log_segment": "bin.000300", "commit_pos": 9500,
            "events": [{"seq": 1, "table": "orders", "type": "DELETE", "data": {"order_id": "x"}}],
        })
        progress = cc.get_delivery_progress(3)
        assert progress["confirmed_watermark_pos"] == 0
        assert progress["gap"]["txn_id"] == "TXN-HOLD-1"
        assert progress["gap"]["reason"] == "事务仅部分确认"

        cc.retransmit_transaction(3, {"txn_id": "TXN-HOLD-1", "events": _order_txn("O-HOLD")})
        progress2 = cc.get_delivery_progress(3)
        assert progress2["gap"] is None
        assert progress2["confirmed_watermark_pos"] == 9500


class TestConflictIsolation:
    def test_same_txn_different_content_kept_as_conflict(self):
        """相同事务标识收到不同内容：保留冲突，原投递不被覆盖。"""
        cc = service_for("change_capture")
        _running_config(3)
        first = cc.receive_transaction(3, {
            "txn_id": "TXN-CONFLICT", "log_segment": "bin.000400", "commit_pos": 9600,
            "events": [{"seq": 1, "table": "orders", "type": "INSERT",
                        "data": {"amount": 100}}],
        })
        assert first["status"] == "已确认"
        original_fp = store.find_cdc_delivery(3, "TXN-CONFLICT")["content_fingerprint"]

        clash = cc.receive_transaction(3, {
            "txn_id": "TXN-CONFLICT", "log_segment": "bin.000400", "commit_pos": 9600,
            "events": [{"seq": 1, "table": "orders", "type": "INSERT",
                        "data": {"amount": 999}}],
        })
        assert clash["ok"] is False
        assert clash["conflict"] is True

        conflicts = cc.query_conflicts(3)
        assert conflicts["conflict_count"] >= 1
        assert any(c["txn_id"] == "TXN-CONFLICT" for c in conflicts["items"])
        # 原投递内容指纹保持不变
        manifest = store.find_cdc_delivery(3, "TXN-CONFLICT")
        assert manifest["content_fingerprint"] == original_fp
        assert manifest["conflict"] is True
        # 原事务仍可查询且确认状态不丢
        assert cc.query_delivery(3, "TXN-CONFLICT")["fully_confirmed"] is True

    def test_conflict_on_one_config_does_not_block_other_config(self):
        """一个配置出现冲突，独立配置的事务交付不受影响。"""
        cc = service_for("change_capture")
        _running_config(3)
        _running_config(1)

        # 配置3制造冲突
        cc.receive_transaction(3, {
            "txn_id": "TXN-ISO", "log_segment": "bin.a", "commit_pos": 100,
            "events": [{"seq": 1, "table": "t1", "type": "INSERT", "data": {"v": 1}}],
        })
        cc.receive_transaction(3, {
            "txn_id": "TXN-ISO", "log_segment": "bin.a", "commit_pos": 100,
            "events": [{"seq": 1, "table": "t1", "type": "INSERT", "data": {"v": 2}}],
        })
        assert cc.query_conflicts(3)["conflict_count"] >= 1

        # 独立配置1继续正常交付
        other = cc.receive_transaction(1, {
            "txn_id": "TXN-ISO", "log_segment": "bin.b", "commit_pos": 200,
            "events": [{"seq": 1, "table": "cdc_orders", "type": "INSERT",
                        "data": {"v": 1}}],
        })
        assert other["ok"] is True
        assert other["status"] == "已确认"
        assert cc.query_conflicts(1)["conflict_count"] == 0
        assert cc.get_delivery_progress(1)["confirmed_watermark_pos"] == 200
