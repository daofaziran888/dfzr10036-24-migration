"""增量回填服务：差异行产生后按快照记录路径重放差异行。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import now_text, store


class BackfillRunService(BaseModuleService):
    MODULE = "backfill_run"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "目标表", "起始位点", "截止位点", "回填模式", "并行粒度", "当前进度", "回填速率"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "起始位点"]
    STATUS_ORDER = ["待执行", "回填中", "已完成", "速率过缓"]
    ACTION_RULES = {"启动回填": "回填中", "暂停回填": "已完成", "标记过缓": "速率过缓"}
    NEGATIVE_ACTIONS = []

    def trigger_from_diff(self, diff_total: int) -> dict[str, Any] | None:
        """数据比对产出差异行后，生成按快照记录路径重放的回填任务。

        同一差异规模只保留一条待执行任务，重复进入链路不会堆重复任务。
        """
        rows = store.rows(self.MODULE)
        for row in rows:
            if (
                str(row.get("回填模式")) == "按快照记录路径重放差异行"
                and int(row.get("差异行数") or 0) == diff_total
                and row.get("status") == "待执行"
            ):
                return row
        new_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        task = {
            "id": new_id,
            "任务编号": f"BF-{new_id:04d}",
            "目标表": "ads.order_events_iceberg",
            "起始位点": "按差异行首次出现时间",
            "截止位点": "按快照清单最新一次提交",
            "回填模式": "按快照记录路径重放差异行",
            "并行粒度": "按文件",
            "当前进度": "0%",
            "回填速率": "待启动",
            "差异行数": diff_total,
            "创建时间": now_text(),
            "status": "待执行",
            "pending": True,
            "abnormal": False,
        }
        rows.append(task)
        store.flush(self.MODULE)
        return task
