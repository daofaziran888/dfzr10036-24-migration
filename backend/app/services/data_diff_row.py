"""差异行：每条差异记录都关联消费它的快照、清单版本和文件。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class DataDiffRowService(BaseModuleService):
    MODULE = "data_diff_row"
    ENTITY = "差异行"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "清单版本", "文件路径", "业务键", "源端值", "目标端值", "差异类型", "首次出现时间"]
    REQUIRED_FIELDS = []
    STATUS_ORDER = ["存在差异", "已修复"]
    ACTION_RULES = {"标记已修复": "已修复"}
    NEGATIVE_ACTIONS = []
