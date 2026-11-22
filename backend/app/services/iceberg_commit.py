"""冰山上层提交：提交完成后触发compaction小文件合并和expire_snap过期清理。

本模块同时负责兼容两种写入端的快照登记：

* 旧版（v1）写入端只提交 ``数据文件`` 单路径；
* 新版（v2）写入端提交 ``数据文件清单`` 多路径，并显式记录清单版本。

回退期间同一存储要同时容纳两种快照，因此登记入口按写入端版本归一化字段，
但已经存在的多路径快照不会在重启或读取时被改写。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import now_text, store


class IcebergCommitService(BaseModuleService):
    MODULE = "iceberg_commit"
    ENTITY = "提交快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "目标表", "数据文件", "清单文件", "快照ID", "提交状态", "影响行数", "提交时间"]
    REQUIRED_FIELDS = ["快照编号", "目标表"]
    STATUS_ORDER = ["待提交", "提交中", "已提交", "提交冲突"]
    ACTION_RULES = {"启动提交": "提交中", "重试提交": "已提交", "标记冲突": "提交冲突"}
    NEGATIVE_ACTIONS = []

    # ---- 登记（兼容 v1 单路径 / v2 文件清单） -------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        paths = self._normalise_paths(values)
        if not paths:
            missing.append("数据文件/数据文件清单")
        if missing:
            return None, missing

        writer = store.writer_version()
        explicit = str(values.get("清单版本") or "").strip()
        has_manifest = "数据文件清单" in values and bool(values.get("数据文件清单"))
        if explicit in ("v1", "v2"):
            version = explicit
        elif len(paths) > 1 or has_manifest:
            version = "v2"
        else:
            # 只给了单路径、也没有清单字段：旧版入口，按 v1 单路径快照接纳。
            version = "v1"

        rows = store.rows(self.MODULE)
        entry: dict[str, Any] = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        }
        for field in self.LIST_FIELDS:
            entry[field] = values.get(field)
        entry["快照别名"] = values.get("快照别名") or str(values.get("快照编号"))
        entry["数据文件"] = ",".join(paths)
        entry["数据文件清单"] = paths
        entry["数据文件数量"] = len(paths)
        entry["清单版本"] = version
        entry["写入端版本"] = writer
        entry["清单文件"] = values.get("清单文件") or f"metadata/manifest-{entry['id']}.avro"
        entry["快照ID"] = values.get("快照ID") or f"{entry['id'] * 1000 + 500}"
        entry["提交状态"] = values.get("提交状态") or "PENDING"
        entry["影响行数"] = values.get("影响行数") or 0
        entry["提交时间"] = values.get("提交时间") or now_text()
        entry["比对状态"] = "未开始"
        entry["比对完成"] = False
        entry["已比对文件数"] = 0
        entry["待比对文件数"] = len(paths)
        entry["差异行合计"] = 0
        entry["最近恢复窗口"] = None
        entry["最近恢复时间"] = None
        entry["status"] = "待提交"
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)

        # 同步补齐该快照的文件行，后续比对/恢复按清单范围处理。
        from app.services import commit_recovery

        for path in paths:
            commit_recovery.ensure_file_row(entry, path)
        store.flush(self.MODULE)
        store.flush("commit_file")
        return entry, []

    @staticmethod
    def _normalise_paths(values: dict[str, Any]) -> list[str]:
        manifest = values.get("数据文件清单")
        candidates: list[str] = []
        if isinstance(manifest, list):
            candidates = [str(item).strip() for item in manifest]
        elif isinstance(manifest, str) and manifest.strip():
            candidates = [item.strip() for item in manifest.split(",")]
        single = values.get("数据文件")
        if isinstance(single, list):
            candidates.extend(str(item).strip() for item in single)
        elif single:
            candidates.extend(item.strip() for item in str(single).split(","))
        seen: list[str] = []
        for item in candidates:
            if item and item not in seen:
                seen.append(item)
        return seen

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is not None:
            store.flush(self.MODULE)
        return entry, message

    def trigger_compaction_and_expire(self, entry_id: int) -> dict[str, Any] | None:
        """Iceberg提交完成后触发compaction小文件合并和expire_snap过期快照清理。

        数据流链路: iceberg_commit -> compaction + expire_snap
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已提交":
            return None

        table = str(entry.get("目标表", "unknown"))
        results: dict[str, Any] = {"compaction": None, "expire_snap": None}

        # 检查文件数，触发 compaction
        comp_rows = store.rows("compaction")
        existing_comp = [r for r in comp_rows if str(r.get("目标表", "")) == table]
        if not existing_comp:
            new_comp_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
            comp = {
                "id": new_comp_id,
                "任务编号": f"COMP-{new_comp_id:04d}",
                "目标表": table,
                "文件数量": 42,
                "合并策略": "BinPack-128MB",
                "触发阈值": "文件数 >= 30",
                "执行内存": "1GB",
                "合并耗时": "12.5s",
                "释放空间": "450MB",
                "status": "已完成",
                "pending": False,
                "abnormal": False,
            }
            comp_rows.append(comp)
            results["compaction"] = comp
        else:
            existing_comp[0]["文件数量"] += 1
            results["compaction"] = existing_comp[0]

        # 触发 expire_snap
        exp_rows = store.rows("expire_snap")
        existing_exp = [r for r in exp_rows if str(r.get("目标表", "")) == table]
        if existing_exp:
            existing_exp[0]["status"] = "生效中"
            existing_exp[0]["pending"] = True
            existing_exp[0]["abnormal"] = False
            results["expire_snap"] = existing_exp[0]
        else:
            new_exp_id = max((int(r.get("id", 0)) for r in exp_rows), default=0) + 1
            expire = {
                "id": new_exp_id,
                "策略编号": f"EXPI-{new_exp_id:04d}",
                "目标表": table,
                "保留数量": 10,
                "保留天数": "7天",
                "清理模式": "按快照时间过期",
                "预演结果": f"待清理{new_exp_id * 2}个历史快照",
                "下次执行": "2026-11-19 02:00:00",
                "上次清理": "2026-11-18 02:00:00",
                "status": "生效中",
                "pending": True,
                "abnormal": False,
            }
            exp_rows.append(expire)
            results["expire_snap"] = expire

        store.flush("compaction")
        store.flush("expire_snap")
        return results

    def handle_commit_conflict(self, entry_id: int) -> dict[str, Any] | None:
        """提交冲突时触发数据比对校验和告警。

        数据流链路: iceberg_commit(提交冲突) -> data_diff + alert_rule

        与旧实现不同，这里不再为整张快照写一条“差异行数=影响行数”的粗粒度
        记录，而是交给 ``commit_recovery`` 按快照记录的文件路径逐份比对；
        没有证据的文件保持待证据，不会把整份快照标成比对完成。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "提交冲突":
            return None

        from app.services import commit_recovery

        comparison = commit_recovery.comparison(entry_id)
        diff_total = sum(
            item.get("差异行数", 0)
            for snapshot in comparison.get("快照", [])
            for item in snapshot.get("文件", [])
        )

        table = str(entry.get("目标表", "unknown"))
        results: dict[str, Any] = {
            "data_diff": None,
            "alert": None,
            "comparison": comparison,
        }
        diff_rows = store.rows("data_diff")
        number = str(entry.get("快照编号"))
        # 同一快照的冲突比对只保留一条摘要：重复进入链路时更新而不是堆行。
        summary = next(
            (row for row in diff_rows
             if str(row.get("快照编号")) == number and row.get("汇总")),
            None,
        )
        if summary is None:
            summary = {
                "id": max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1,
                "任务编号": f"DATA-{len(diff_rows) + 1:04d}",
                "源端": "CDC实时流",
                "目标端": table,
                "比对字段": "全字段",
                "差异类型": "数据不一致",
                "修复建议": "按快照记录路径重放差异行",
                "快照编号": entry.get("快照编号"),
                "快照别名": entry.get("快照别名"),
                "清单版本": entry.get("清单版本"),
                "汇总": True,
                "status": "存在差异",
                "pending": True,
            }
            diff_rows.append(summary)
        summary.update({
            "差异行数": diff_total,
            "比对时间": now_text(),
            "比对完成": bool(entry.get("比对完成")),
            "比对状态": entry.get("比对状态"),
            "abnormal": diff_total > 0,
        })
        results["data_diff"] = summary

        alert_rows = store.rows("alert_rule")
        existing_alert = next(
            (row for row in alert_rows if str(row.get("告警条件")) == f"表{table}提交冲突"),
            None,
        )
        if existing_alert is not None:
            existing_alert["触发次数"] = int(existing_alert.get("触发次数") or 0) + 1
            results["alert"] = existing_alert
            store.flush("data_diff")
            store.flush("alert_rule")
            store.flush("iceberg_commit")
            return results

        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "Iceberg提交冲突",
            "告警条件": f"表{table}提交冲突",
            "告警级别": "P1-紧急",
            "通知渠道": "电话+飞书+钉钉",
            "聚合窗口": "不聚合",
            "静默时段": "无静默",
            "触发次数": 1,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
        }
        alert_rows.append(alert)
        results["alert"] = alert

        store.flush("data_diff")
        store.flush("alert_rule")
        store.flush("iceberg_commit")
        return results
