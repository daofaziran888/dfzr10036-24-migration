"""快照文件行：每个快照清单里的每个文件一行，记录证据、消费次数和比对状态。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import commit_recovery
from app.store import store


class CommitFileService(BaseModuleService):
    MODULE = "commit_file"
    ENTITY = "快照文件"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["文件路径", "快照编号", "清单版本", "源端证据", "目标端证据", "消费次数", "差异行数", "比对状态"]
    REQUIRED_FIELDS = []
    STATUS_ORDER = ["待比对", "待证据", "已比对"]
    ACTION_RULES = {"标记待证据": "待证据", "确认已比对": "已比对"}
    NEGATIVE_ACTIONS = []

    def list_entries(self, *, filters=None, keyword=None, status=None, page=1, size=20):
        # 读取前把快照清单里新增的文件补齐成行，保证列表与恢复范围一致。
        for snapshot in store.rows("iceberg_commit"):
            commit_recovery.file_rows(snapshot)
        return super().list_entries(
            filters=filters, keyword=keyword, status=status, page=page, size=size
        )
