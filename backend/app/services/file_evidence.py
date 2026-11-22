"""源端/目标端文件证据：按快照记录的完整路径登记可读文件内容或目录证据。

证据是数据比对与提交失败恢复的唯一取数来源。登记入口只接受快照清单中出现
过的路径，避免拿当前管道里的同名文件顶替迁移现场的文件。
"""
from __future__ import annotations

from typing import Any

from app.services import commit_recovery
from app.services.base import BaseModuleService
from app.store import store


class FileEvidenceService(BaseModuleService):
    MODULE = "file_evidence"
    ENTITY = "文件证据"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["文件路径", "端", "证据类型", "记录数", "记录摘要", "说明"]
    REQUIRED_FIELDS = ["文件路径", "端"]
    STATUS_ORDER = ["文件内容", "目录证据", "缺失"]
    ACTION_RULES = {"标记缺失": "缺失", "确认为目录证据": "目录证据"}
    NEGATIVE_ACTIONS = ["标记缺失"]

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """按路径+端补交证据；已存在的同一路径同一端就地更新。"""
        path = str(values.get("文件路径") or "").strip()
        side = str(values.get("端") or "").strip()
        missing = [field for field in self.REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        result = commit_recovery.supply_evidence(
            path=path,
            side=side,
            records=values.get("记录"),
            evidence_type=str(values.get("证据类型") or "文件内容"),
            note=str(values.get("说明") or ""),
        )
        if not result.get("ok"):
            raise ValueError(str(result.get("message") or "证据补交被拒绝"))
        return result["证据"], []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["证据类型"] = target
        entry["status"] = target
        entry["可读"] = target != "缺失"
        entry["pending"] = target == "缺失"
        entry["abnormal"] = target == "缺失"
        for commit_file in store.rows("commit_file"):
            if str(commit_file.get("文件路径")) != str(entry.get("文件路径")):
                continue
            if str(entry.get("端")) == commit_recovery.SOURCE:
                commit_file["源端证据"] = target
            else:
                commit_file["目标端证据"] = target
        store.flush()
        return entry, f"{self.ENTITY}已{action}"
