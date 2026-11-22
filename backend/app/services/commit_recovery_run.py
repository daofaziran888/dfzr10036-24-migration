"""提交失败恢复窗口：可回看每个窗口实际处理的范围与结果。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class CommitRecoveryRunService(BaseModuleService):
    MODULE = "commit_recovery_run"
    ENTITY = "恢复窗口"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["窗口", "快照编号", "清单版本", "实际处理范围", "跳过回执范围", "延后缺证范围", "差异行数", "比对状态"]
    REQUIRED_FIELDS = []
    STATUS_ORDER = ["部分完成", "已完成"]
    ACTION_RULES = {"确认完成": "已完成"}
    NEGATIVE_ACTIONS = []
