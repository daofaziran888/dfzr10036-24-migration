"""单文件确认回执：登记后该文件不再重复参与比对，也不会再写差异行。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services import commit_recovery
from app.store import store


class CommitReceiptService(BaseModuleService):
    MODULE = "commit_receipt"
    ENTITY = "确认回执"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["回执编号", "快照编号", "文件路径", "清单版本", "回执类型", "状态", "生成时间", "生效时间"]
    REQUIRED_FIELDS = ["快照编号", "文件路径"]
    STATUS_ORDER = ["待生效", "已生效"]
    ACTION_RULES = {"确认生效": "已生效", "撤销回执": "待生效"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        path = str(values.get("文件路径")).strip()
        if path not in commit_recovery.known_paths():
            raise ValueError("回执路径不在任何快照清单中，不能为管道里的同名文件登记回执")
        number = str(values.get("快照编号")).strip()
        snapshot = commit_recovery.snapshot_by_number(number)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = store.rows(self.MODULE)
        row = {
            "id": max((int(item.get("id", 0)) for item in rows), default=0) + 1,
            "回执编号": values.get("回执编号") or f"RCPT-{len(rows) + 1:04d}",
            "快照编号": number,
            "快照别名": values.get("快照别名")
            or (snapshot.get("快照别名") if snapshot else number),
            "文件路径": path,
            "文件名": commit_recovery.file_name(path),
            "清单版本": values.get("清单版本")
            or (commit_recovery.manifest_version(snapshot) if snapshot else "v1"),
            "回执类型": values.get("回执类型") or "单文件确认",
            "状态": values.get("状态") or "已生效",
            "来源": values.get("来源") or "人工确认",
            "生成时间": values.get("生成时间") or now,
            "生效时间": values.get("生效时间") or now,
        }
        row["status"] = row["状态"]
        row["pending"] = row["状态"] != "已生效"
        row["abnormal"] = False
        rows.append(row)
        store.flush(self.MODULE)
        return row, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is not None:
            entry["状态"] = entry.get("status")
        return entry, message
