"""CDC 事务级投递接口：整笔事务接收、重传补齐、投递查询、进度与缺口续采。

与通用资源路由分开，路径固定在 /api/cdc 下，避免被 /api/{module} 动态路由吞掉。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/cdc", tags=["变更捕获事务投递"])


class CdcEvent(BaseModel):
    seq: int = Field(..., description="事务内顺序，从1开始")
    table: str
    type: str
    data: dict[str, Any] | None = None
    fail_tables: list[str] | None = Field(
        default=None, description="故障注入：本次投递中写入失败的表"
    )


class CdcTransactionPayload(BaseModel):
    txn_id: str
    log_segment: str | None = None
    commit_pos: int = 0
    events: list[CdcEvent]


class CdcRetransmitPayload(BaseModel):
    txn_id: str
    events: list[CdcEvent]


class CdcStagePayload(BaseModel):
    transactions: list[dict[str, Any]]


def _capture_service():
    return service_for("change_capture")


@router.post("/configs/{config_id}/transactions")
def receive_transaction(config_id: int, payload: CdcTransactionPayload) -> dict[str, Any]:
    """批量接收一整笔事务变更，返回带逐条/逐表确认的发送回执。"""
    return _capture_service().receive_transaction(config_id, payload.model_dump())


@router.post("/configs/{config_id}/retransmit")
def retransmit_transaction(config_id: int, payload: CdcRetransmitPayload) -> dict[str, Any]:
    """投递失败后重传原事务，只补齐未确认的事件与表写入。"""
    return _capture_service().retransmit_transaction(config_id, payload.model_dump())


@router.get("/configs/{config_id}/deliveries")
def list_deliveries(config_id: int) -> dict[str, Any]:
    """投递清单：半笔事务显示为部分确认，不会被列为成功。"""
    return _capture_service().list_deliveries(config_id)


@router.get("/configs/{config_id}/deliveries/{txn_id}")
def query_delivery(config_id: int, txn_id: str) -> dict[str, Any]:
    """查询单笔事务的逐条事件与逐表写入确认状态。"""
    return _capture_service().query_delivery(config_id, txn_id)


@router.get("/configs/{config_id}/progress")
def delivery_progress(config_id: int) -> dict[str, Any]:
    """确认水位按连续完成的事务推进，并返回最早缺口。"""
    return _capture_service().get_delivery_progress(config_id)


@router.get("/configs/{config_id}/resume")
def resume_capture(config_id: int) -> dict[str, Any]:
    """读取捕获进度并给出从正确缺口接回的续采位置。"""
    return _capture_service().resume_capture(config_id)


@router.post("/configs/{config_id}/log-index")
def stage_log_index(config_id: int, payload: CdcStagePayload) -> dict[str, Any]:
    """登记源库日志段内按提交位置排序的事务索引。"""
    return _capture_service().stage_log_transactions(config_id, payload.transactions)


@router.get("/configs/{config_id}/conflicts")
def query_conflicts(config_id: int) -> dict[str, Any]:
    """查询同事务标识不同内容的冲突记录（冲突保留，不影响其他配置）。"""
    return _capture_service().query_conflicts(config_id)
