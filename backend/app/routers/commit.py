"""提交清单迁移的专用端点：恢复、比对、证据补交、回执与写入端版本。

这些固定路径必须在通用资源路由 ``/api/{module}/...`` 之前注册，否则会被
动态模块/条目路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.services import commit_recovery
from app.store import store

router = APIRouter(prefix="/api/commit", tags=["提交清单迁移"])


def _snapshot_or_404(entry_id: int | None) -> dict[str, Any] | None:
    if entry_id is None:
        return None
    snapshot = commit_recovery.find_snapshot(entry_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"提交快照 {entry_id} 不存在或已归档")
    return snapshot


@router.get("/state")
def commit_state() -> dict[str, Any]:
    """迁移现场概览：写入端版本、启动次数、快照与证据规模。"""
    snapshots = []
    for snapshot in store.rows("iceberg_commit"):
        commit_recovery.refresh_snapshot(snapshot)
        snapshots.append({
            "id": snapshot.get("id"),
            "快照编号": snapshot.get("快照编号"),
            "快照别名": snapshot.get("快照别名"),
            "清单版本": commit_recovery.manifest_version(snapshot),
            "数据文件清单": commit_recovery.snapshot_files(snapshot),
            "比对状态": snapshot.get("比对状态"),
            "比对完成": snapshot.get("比对完成"),
        })
    return {
        "ok": True,
        "写入端版本": store.writer_version(),
        "启动次数": int(store.meta("boot_count") or "0"),
        "迁移夹具": store.meta("fixture"),
        "数据库": str(store.db.path),
        "快照": snapshots,
        "文件证据数": len(store.rows("file_evidence")),
        "快照文件数": len(store.rows("commit_file")),
        "差异行数": len(store.rows("data_diff_row")),
        "回执数": len(store.rows("commit_receipt")),
    }


@router.get("/recovery")
def list_recovery_runs(
    commit_entry_id: int | None = Query(default=None),
) -> dict[str, Any]:
    runs = list(store.rows("commit_recovery_run"))
    if commit_entry_id is not None:
        snapshot = _snapshot_or_404(commit_entry_id)
        runs = [row for row in runs if row.get("快照编号") == snapshot.get("快照编号")]
    return {"ok": True, "items": runs, "total": len(runs)}


@router.post("/recovery")
def run_recovery(
    commit_entry_id: int | None = Query(default=None),
    window: str | None = Query(default=None),
) -> dict[str, Any]:
    """提交失败恢复：缺省处理全部未完成快照；同一窗口重复调用只回放首次结果。"""
    if commit_entry_id is not None:
        _snapshot_or_404(commit_entry_id)
    return commit_recovery.recover(commit_entry_id, window)


@router.get("/comparison")
def get_comparison(
    commit_entry_id: int | None = Query(default=None),
) -> dict[str, Any]:
    """数据比对结果：每份差异行关联快照、清单版本、文件、首次出现时间。"""
    _snapshot_or_404(commit_entry_id)
    return commit_recovery.comparison(commit_entry_id)


@router.post("/evidence")
def supply_evidence(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """补交某端证据（只接受快照清单里记录过的路径）。"""
    result = commit_recovery.supply_evidence(
        path=str(payload.get("文件路径") or payload.get("path") or ""),
        side=str(payload.get("端") or payload.get("side") or ""),
        records=payload.get("记录", payload.get("records")),
        evidence_type=str(
            payload.get("证据类型") or payload.get("evidence_type") or "文件内容"
        ),
        readable=bool(payload.get("可读", True)),
        note=str(payload.get("说明") or payload.get("note") or ""),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message"))
    return result


@router.post("/receipts")
def create_receipt(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """登记单文件确认回执；生效后该文件不再重复比对，也不重复写差异行。"""
    from app.services import service_for

    service = service_for("commit_receipt")
    try:
        entry, missing = service.create_entry(payload)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    if missing:
        raise HTTPException(status_code=400, detail=f"缺少必填字段：{'、'.join(missing)}")
    return {"ok": True, "message": "确认回执已登记", "entry": entry}


@router.get("/writer-version")
def get_writer_version() -> dict[str, Any]:
    return {"ok": True, "写入端版本": store.writer_version()}


@router.post("/writer-version")
def set_writer_version(version: str = Query(...)) -> dict[str, Any]:
    result = commit_recovery.set_writer_version(version)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message"))
    return result


@router.post("/reset")
def reset_fixture() -> dict[str, Any]:
    """把数据库恢复为种子 + 迁移夹具的初始状态。"""
    store.reset()
    return {"ok": True, "message": "迁移夹具已重置", "启动次数": int(store.meta("boot_count") or "0")}
