"""提交失败恢复与数据比对：兼容 v1 单路径旧快照与 v2 提交文件清单。

迁移背景
--------
旧版写入端每个快照只记录一个 ``数据文件``；新版把同一快照的多个文件写进
``数据文件清单``。迁移夹具里有两种快照并存：

* 旧快照 A / D：``数据文件`` 单路径，``清单版本`` 为 v1；
* 新快照 BC：``数据文件清单`` 多路径，``清单版本`` 为 v2。

恢复入口 ``recover()`` 只按快照自身记录的路径确定范围，绝不按文件名从当前
管道目录里取同名文件。数据比对入口 ``comparison()`` 读取这些路径的证据内容，
并把每条差异行都关联到消费它的快照、清单版本和文件。

幂等约束
--------
* 同一 ``(窗口, 快照)`` 只给一次恢复机会；重复调用返回首次结果，不再消费。
* 已生效的单文件确认回执优先：有回执的文件不再参与比对，也不会重复写差异行。
* 差异行按 ``(快照编号, 文件路径, 业务键)`` 去重：重复比对只更新“最近出现时间”，
  保留“首次出现时间”。
* 只有全部文件都完成比对时，快照才会被标记为 ``比对完成``；缺证或回执跳过的
  部分结果仍是“部分完成”。
"""
from __future__ import annotations

import json
from typing import Any

from app.store import now_text, store

SOURCE = "源端"
TARGET = "目标端"
READABLE_EVIDENCE_TYPES = ("文件内容", "目录证据")
DONE = "已比对"


# ---------------------------------------------------------------------------
# 快照与文件的基础读取
# ---------------------------------------------------------------------------
def snapshot_files(snapshot: dict[str, Any]) -> list[str]:
    """读取快照实际记录的文件路径；v2 清单优先，v1 回退到单路径。"""
    manifest = snapshot.get("数据文件清单")
    if isinstance(manifest, list):
        paths = [str(item).strip() for item in manifest if str(item).strip()]
        if paths:
            return paths
    if isinstance(manifest, str) and manifest.strip():
        paths = [item.strip() for item in manifest.split(",") if item.strip()]
        if paths:
            return paths
    single = str(snapshot.get("数据文件") or "").strip()
    if not single:
        return []
    return [item.strip() for item in single.split(",") if item.strip()]


def manifest_version(snapshot: dict[str, Any]) -> str:
    """清单版本：v2 快照带显式版本，旧快照缺省视为 v1。"""
    version = str(snapshot.get("清单版本") or "").strip()
    if version:
        return version
    if isinstance(snapshot.get("数据文件清单"), list):
        return "v2"
    return "v1"


def file_name(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def find_snapshot(entry_id: int) -> dict[str, Any] | None:
    return store.find("iceberg_commit", entry_id)


def snapshot_by_number(number: str) -> dict[str, Any] | None:
    for row in store.rows("iceberg_commit"):
        if str(row.get("快照编号")) == number:
            return row
    return None


# ---------------------------------------------------------------------------
# 证据读取（只按快照记录的完整路径，不做同名替换）
# ---------------------------------------------------------------------------
def evidence_rows(path: str, side: str) -> list[dict[str, Any]]:
    return [
        row
        for row in store.rows("file_evidence")
        if str(row.get("文件路径")) == path
        and str(row.get("端")) == side
        and bool(row.get("可读"))
    ]


def evidence_status(path: str, side: str) -> dict[str, Any]:
    rows = evidence_rows(path, side)
    if not rows:
        return {"可读": False, "证据类型": "缺失", "记录数": 0, "记录": [], "摘要": "无证据"}
    row = rows[-1]
    return {
        "可读": True,
        "证据类型": str(row.get("证据类型") or "文件内容"),
        "记录数": int(row.get("记录数") or len(row.get("记录") or [])),
        "记录": list(row.get("记录") or []),
        "摘要": str(row.get("记录摘要") or ""),
    }


def read_path_content(path: str, side: str) -> list[dict[str, Any]] | None:
    """读取该路径在某一端的内容记录；只认快照里记录的完整路径。"""
    status = evidence_status(path, side)
    if not status["可读"]:
        return None
    if status["证据类型"] not in READABLE_EVIDENCE_TYPES:
        return None
    return status["记录"]


# ---------------------------------------------------------------------------
# commit_file 行
# ---------------------------------------------------------------------------
def ensure_file_row(snapshot: dict[str, Any], path: str) -> dict[str, Any]:
    version = manifest_version(snapshot)
    number = str(snapshot.get("快照编号"))
    for row in store.rows("commit_file"):
        if str(row.get("快照编号")) == number and str(row.get("文件路径")) == path:
            # 旧夹具/旧快照可能缺少版本信息，读取时补齐但不改路径。
            row.setdefault("清单版本", version)
            return row

    source = evidence_status(path, SOURCE)
    target = evidence_status(path, TARGET)
    row = {
        "id": _next_id("commit_file"),
        "文件路径": path,
        "文件名": file_name(path),
        "快照编号": number,
        "快照别名": snapshot.get("快照别名") or number,
        "清单版本": version,
        "源端证据": source["证据类型"],
        "目标端证据": target["证据类型"],
        "消费次数": 0,
        "差异行数": 0,
        "首次差异时间": None,
        "最近消费时间": None,
        "比对状态": "待比对" if source["可读"] and target["可读"] else "待证据",
        "回执编号": None,
        "status": "待比对" if source["可读"] and target["可读"] else "待证据",
        "pending": True,
        "abnormal": False,
    }
    store.rows("commit_file").append(row)
    return row


def file_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [ensure_file_row(snapshot, path) for path in snapshot_files(snapshot)]


# ---------------------------------------------------------------------------
# 回执
# ---------------------------------------------------------------------------
def effective_receipt(number: str, path: str) -> dict[str, Any] | None:
    for row in store.rows("commit_receipt"):
        if (
            str(row.get("快照编号")) == number
            and str(row.get("文件路径")) == path
            and str(row.get("状态")) == "已生效"
        ):
            return row
    return None


def _receipt_audit(number: str, path: str) -> dict[str, Any] | None:
    receipt = effective_receipt(number, path)
    if receipt is None:
        return None
    rows = diff_rows_for(number, path)
    return {
        "文件路径": path,
        "文件名": file_name(path),
        "回执编号": receipt.get("回执编号"),
        "状态": receipt.get("状态"),
        "来源": receipt.get("来源"),
        "生效时间": receipt.get("生效时间"),
        "独立差异行数": len(rows),
        "业务键": [row.get("业务键") for row in rows],
        "首次出现时间": min(
            (str(row.get("首次出现时间")) for row in rows), default=None
        ),
    }


# ---------------------------------------------------------------------------
# 差异行
# ---------------------------------------------------------------------------
def diff_rows_for(number: str, path: str) -> list[dict[str, Any]]:
    return [
        row
        for row in store.rows("data_diff_row")
        if str(row.get("快照编号")) == number and str(row.get("文件路径")) == path
    ]


def _diff_index(number: str, path: str, key: str) -> dict[str, Any] | None:
    for row in store.rows("data_diff_row"):
        if (
            str(row.get("快照编号")) == number
            and str(row.get("文件路径")) == path
            and str(row.get("业务键")) == key
        ):
            return row
    return None


def _record_map(records: list[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for record in records:
        key = record.get("业务键")
        if key is None:
            continue
        result[str(key)] = str(record.get("值", ""))
    return result


def compare_contents(
    snapshot: dict[str, Any],
    path: str,
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    window: str,
) -> list[dict[str, Any]]:
    """比对两端记录并落差异行，返回本次新增的差异行。"""
    number = str(snapshot.get("快照编号"))
    version = manifest_version(snapshot)
    now = now_text()
    source_map = _record_map(source_records)
    target_map = _record_map(target_records)

    produced: list[dict[str, Any]] = []
    newly_inserted: list[dict[str, Any]] = []
    for key in sorted(set(source_map) | set(target_map)):
        if key in source_map and key in target_map:
            if source_map[key] == target_map[key]:
                continue
            diff_type = "值不一致"
        elif key in source_map:
            diff_type = "目标端缺失"
        else:
            diff_type = "源端缺失"

        existing = _diff_index(number, path, key)
        if existing is not None:
            existing["最近出现时间"] = now
            existing["恢复窗口"] = window
            existing["源端值"] = source_map.get(key)
            existing["目标端值"] = target_map.get(key)
            existing["差异类型"] = diff_type
            produced.append(existing)
            continue

        row = {
            "id": _next_id("data_diff_row"),
            "快照编号": number,
            "快照别名": snapshot.get("快照别名") or number,
            "清单版本": version,
            "文件路径": path,
            "文件名": file_name(path),
            "业务键": key,
            "源端值": source_map.get(key),
            "目标端值": target_map.get(key),
            "差异类型": diff_type,
            "首次出现时间": now,
            "最近出现时间": now,
            "恢复窗口": window,
            "status": "存在差异",
            "pending": False,
            "abnormal": True,
        }
        store.rows("data_diff_row").append(row)
        produced.append(row)
        newly_inserted.append(row)

    # 同步一条 data_diff 任务摘要，便于原有数据比对页面看到本次结果。
    if produced:
        _sync_diff_task(snapshot, path, len(produced), now)
    return newly_inserted


def _sync_diff_task(
    snapshot: dict[str, Any], path: str, diff_count: int, now: str
) -> None:
    number = str(snapshot.get("快照编号"))
    for row in store.rows("data_diff"):
        if str(row.get("快照编号")) == number and str(row.get("文件路径")) == path:
            row["差异行数"] = diff_count
            row["比对时间"] = now
            row["status"] = "存在差异"
            return
    row = {
        "id": _next_id("data_diff"),
        "任务编号": f"DDF-{_next_id('data_diff'):04d}",
        "源端": f"源端文件:{path}",
        "目标端": f"目标端文件:{path}",
        "比对字段": "全字段",
        "差异类型": "数据不一致",
        "差异行数": diff_count,
        "修复建议": "按快照记录路径重放差异行",
        "比对时间": now,
        "快照编号": number,
        "快照别名": snapshot.get("快照别名") or number,
        "清单版本": manifest_version(snapshot),
        "文件路径": path,
        "消费次数": 1,
        "首次差异时间": now,
        "恢复窗口": None,
        "status": "存在差异",
        "pending": True,
        "abnormal": True,
    }
    store.rows("data_diff").append(row)


# ---------------------------------------------------------------------------
# 快照状态收口：部分结果绝不标记完成
# ---------------------------------------------------------------------------
def refresh_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    number = str(snapshot.get("快照编号"))
    rows = [row for row in store.rows("commit_file") if str(row.get("快照编号")) == number]
    total = len(snapshot_files(snapshot))
    done = sum(1 for row in rows if str(row.get("比对状态")) == DONE)
    diff_total = sum(
        len(diff_rows_for(number, str(row.get("文件路径")))) for row in rows
    )
    first_times = [
        str(diff.get("首次出现时间"))
        for row in rows
        for diff in diff_rows_for(number, str(row.get("文件路径")))
        if diff.get("首次出现时间")
    ]
    complete = total > 0 and done >= total
    snapshot["比对完成"] = complete
    snapshot["已比对文件数"] = done
    snapshot["待比对文件数"] = max(total - done, 0)
    snapshot["差异行合计"] = diff_total
    snapshot["首次差异时间"] = min(first_times) if first_times else None
    if complete:
        snapshot["比对状态"] = "已比对"
    elif done > 0:
        snapshot["比对状态"] = "部分完成"
    else:
        snapshot["比对状态"] = "未开始"
    snapshot["status"] = snapshot.get("status") or "提交冲突"
    return snapshot


# ---------------------------------------------------------------------------
# 恢复窗口
# ---------------------------------------------------------------------------
def _next_window() -> str:
    seq = int(store.meta("recovery_window_seq") or "0") + 1
    store.set_meta("recovery_window_seq", str(seq))
    return f"W-{seq:04d}"


def _reserve_window(window: str) -> None:
    """显式指定窗口时同步推进序号，避免后续自动窗口与之撞号。"""
    if not window.startswith("W-"):
        return
    tail = window[2:]
    if not tail.isdigit():
        return
    seq = int(store.meta("recovery_window_seq") or "0")
    store.set_meta("recovery_window_seq", str(max(seq, int(tail))))


def _cached_run(window: str, number: str) -> dict[str, Any] | None:
    for row in store.rows("commit_recovery_run"):
        if str(row.get("窗口")) == window and str(row.get("快照编号")) == number:
            return row
    return None


def _pending_paths(snapshot: dict[str, Any]) -> list[str]:
    return [
        str(row.get("文件路径"))
        for row in file_rows(snapshot)
        if str(row.get("比对状态")) != DONE
    ]


def recover_snapshot(
    snapshot: dict[str, Any], window: str
) -> dict[str, Any]:
    number = str(snapshot.get("快照编号"))
    version = manifest_version(snapshot)
    scope = snapshot_files(snapshot)

    consumed: list[str] = []
    receipt_skipped: list[str] = []
    already_done: list[str] = []
    deferred: list[dict[str, Any]] = []
    new_rows: list[dict[str, Any]] = []
    receipt_audits: list[dict[str, Any]] = []

    for path in scope:
        row = ensure_file_row(snapshot, path)
        source = evidence_status(path, SOURCE)
        target = evidence_status(path, TARGET)
        row["源端证据"] = source["证据类型"]
        row["目标端证据"] = target["证据类型"]

        receipt = effective_receipt(number, path)
        if receipt is not None:
            # 回执优先：该文件已由独立比对确认，不重复消费、不再写差异行。
            row["比对状态"] = DONE
            row["status"] = DONE
            row["pending"] = False
            row["回执编号"] = receipt.get("回执编号")
            existing = diff_rows_for(number, path)
            row["差异行数"] = len(existing)
            if existing:
                row["首次差异时间"] = min(
                    str(item.get("首次出现时间")) for item in existing
                )
            receipt_skipped.append(path)
            audit = _receipt_audit(number, path)
            if audit is not None:
                receipt_audits.append(audit)
            continue

        if str(row.get("比对状态")) == DONE:
            already_done.append(path)
            continue

        missing = [
            side for side, status in ((SOURCE, source), (TARGET, target))
            if not status["可读"] or status["证据类型"] not in READABLE_EVIDENCE_TYPES
        ]
        if missing:
            row["比对状态"] = "待证据"
            row["status"] = "待证据"
            row["pending"] = True
            deferred.append({
                "文件路径": path,
                "文件名": file_name(path),
                "缺失证据": missing,
                "源端证据": source["证据类型"],
                "目标端证据": target["证据类型"],
            })
            continue

        source_records = read_path_content(path, SOURCE) or []
        target_records = read_path_content(path, TARGET) or []
        produced = compare_contents(
            snapshot, path, source_records, target_records, window
        )
        row["消费次数"] = int(row.get("消费次数") or 0) + 1
        row["最近消费时间"] = now_text()
        row["比对状态"] = DONE
        row["status"] = DONE
        row["pending"] = False
        row["差异行数"] = len(diff_rows_for(number, path))
        if produced:
            row["首次差异时间"] = min(
                str(item.get("首次出现时间")) for item in produced
            )
        consumed.append(path)
        new_rows.extend(produced)

    refresh_snapshot(snapshot)
    pending = _pending_paths(snapshot)

    report = {
        "窗口": window,
        "快照编号": number,
        "快照别名": snapshot.get("快照别名") or number,
        "清单版本": version,
        "清单范围": scope,
        "实际处理范围": consumed,
        "跳过回执范围": receipt_skipped,
        "已完成跳过范围": already_done,
        "延后缺证范围": [item["文件路径"] for item in deferred],
        "延后明细": deferred,
        "待处理路径": pending,
        "新差异行": new_rows,
        "差异行数": sum(len(diff_rows_for(number, path)) for path in scope),
        "比对完成": bool(snapshot.get("比对完成")),
        "比对状态": snapshot.get("比对状态"),
        "独立回执核查": receipt_audits,
        "消费次数": {
            path: int((ensure_file_row(snapshot, path)).get("消费次数") or 0)
            for path in scope
        },
    }
    return report


def _store_run(snapshot: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    number = str(snapshot.get("快照编号"))
    run = {
        "id": _next_id("commit_recovery_run"),
        "窗口": report["窗口"],
        "快照编号": number,
        "快照别名": report["快照别名"],
        "清单版本": report["清单版本"],
        "清单范围": report["清单范围"],
        "实际处理范围": report["实际处理范围"],
        "跳过回执范围": report["跳过回执范围"],
        "延后缺证范围": report["延后缺证范围"],
        "待处理路径": report["待处理路径"],
        "新差异行数": len(report["新差异行"]),
        "差异行数": report["差异行数"],
        "比对完成": report["比对完成"],
        "比对状态": report["比对状态"],
        "结果": report,
        "创建时间": now_text(),
        "status": "已完成" if report["比对完成"] else "部分完成",
        "pending": not report["比对完成"],
        "abnormal": bool(report["待处理路径"]),
    }
    store.rows("commit_recovery_run").append(run)
    snapshot["最近恢复窗口"] = report["窗口"]
    snapshot["最近恢复时间"] = now_text()
    return run


def recover(
    commit_entry_id: int | None = None,
    window: str | None = None,
) -> dict[str, Any]:
    """提交失败恢复入口。

    * ``commit_entry_id`` 为空时处理所有尚未比对完成的快照，确定实际处理范围；
    * ``window`` 为空时自动开一个新窗口；显式传入时同一 ``(窗口, 快照)``
      只有一次恢复机会，重复调用返回首次结果。
    """
    if commit_entry_id is not None:
        snapshot = find_snapshot(commit_entry_id)
        if snapshot is None:
            return {"ok": False, "message": f"提交快照 {commit_entry_id} 不存在或已归档"}
        targets = [snapshot]
    else:
        targets = [
            row for row in store.rows("iceberg_commit")
            if not row.get("比对完成")
        ]

    target_window = window or _next_window()
    if window:
        _reserve_window(window)
    reports: list[dict[str, Any]] = []
    replayed = False
    for snapshot in targets:
        number = str(snapshot.get("快照编号"))
        cached = _cached_run(target_window, number)
        if cached is not None:
            stored = cached.get("结果") or {}
            reports.append({**stored, "已重放": True})
            replayed = True
            continue
        report = recover_snapshot(snapshot, target_window)
        _store_run(snapshot, report)
        reports.append({**report, "已重放": False})

    store.flush()
    return {
        "ok": True,
        "窗口": target_window,
        "已重放": replayed,
        "处理快照数": len(reports),
        "处理快照": reports,
    }


# ---------------------------------------------------------------------------
# 比对结果入口
# ---------------------------------------------------------------------------
def comparison(commit_entry_id: int | None = None) -> dict[str, Any]:
    """比对结果入口：汇总每个文件消费了哪些路径、差异行与清单版本。"""
    if commit_entry_id is not None:
        snapshot = find_snapshot(commit_entry_id)
        if snapshot is None:
            return {"ok": False, "message": f"提交快照 {commit_entry_id} 不存在或已归档"}
        targets = [snapshot]
    else:
        targets = list(store.rows("iceberg_commit"))

    snapshots_out: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    for snapshot in targets:
        number = str(snapshot.get("快照编号"))
        version = manifest_version(snapshot)
        files_out: list[dict[str, Any]] = []
        for path in snapshot_files(snapshot):
            row = ensure_file_row(snapshot, path)
            diffs = diff_rows_for(number, path)
            all_rows.extend(diffs)
            source = evidence_status(path, SOURCE)
            target = evidence_status(path, TARGET)
            files_out.append({
                "文件路径": path,
                "文件名": file_name(path),
                "快照编号": number,
                "快照别名": snapshot.get("快照别名") or number,
                "清单版本": version,
                "源端证据": source["证据类型"],
                "目标端证据": target["证据类型"],
                "消费次数": int(row.get("消费次数") or 0),
                "最近消费时间": row.get("最近消费时间"),
                "比对状态": row.get("比对状态"),
                "回执编号": row.get("回执编号"),
                "差异行数": len(diffs),
                "首次差异时间": min(
                    (str(item.get("首次出现时间")) for item in diffs), default=None
                ),
                "差异行": diffs,
            })
        refresh_snapshot(snapshot)
        snapshots_out.append({
            "快照编号": number,
            "快照别名": snapshot.get("快照别名") or number,
            "清单版本": version,
            "数据文件": snapshot.get("数据文件"),
            "数据文件清单": snapshot.get("数据文件清单"),
            "比对状态": snapshot.get("比对状态"),
            "比对完成": snapshot.get("比对完成"),
            "已比对文件数": snapshot.get("已比对文件数"),
            "待比对文件数": snapshot.get("待比对文件数"),
            "差异行合计": snapshot.get("差异行合计"),
            "首次差异时间": snapshot.get("首次差异时间"),
            "待处理路径": _pending_paths(snapshot),
            "文件": files_out,
        })

    store.flush()
    return {
        "ok": True,
        "快照数": len(snapshots_out),
        "快照": snapshots_out,
        "差异行": all_rows,
        "消费次数按文件": {
            item["文件路径"]: item["消费次数"]
            for snapshot in snapshots_out for item in snapshot["文件"]
        },
    }


# ---------------------------------------------------------------------------
# 补齐证据
# ---------------------------------------------------------------------------
CANONICAL_C_SOURCE = [
    {"业务键": "C-3001", "值": "500.00"},
    {"业务键": "C-3002", "值": "650.00"},
]


def known_paths() -> set[str]:
    paths: set[str] = set()
    for snapshot in store.rows("iceberg_commit"):
        paths.update(snapshot_files(snapshot))
    for row in store.rows("commit_file"):
        paths.add(str(row.get("文件路径")))
    return paths


def supply_evidence(
    path: str,
    side: str,
    records: list[dict[str, Any]] | str | None = None,
    evidence_type: str = "文件内容",
    readable: bool = True,
    note: str = "",
) -> dict[str, Any]:
    """补交某一端证据。

    只允许提交快照清单里出现过的路径，防止拿当前管道同名文件顶替。
    """
    path = str(path or "").strip()
    if not path:
        return {"ok": False, "message": "文件路径不能为空"}
    if side not in (SOURCE, TARGET):
        return {"ok": False, "message": f"端只能是 {SOURCE} 或 {TARGET}"}
    if path not in known_paths():
        return {
            "ok": False,
            "message": "路径不在任何快照清单中，迁移恢复不接受管道里的同名文件",
        }

    if records is None:
        records = CANONICAL_C_SOURCE if path.endswith("c.parquet") else []
    if isinstance(records, str):
        try:
            records = json.loads(records)
        except json.JSONDecodeError:
            return {"ok": False, "message": "记录必须是 JSON 数组"}
    if not isinstance(records, list):
        return {"ok": False, "message": "记录必须是 JSON 数组"}

    parsed: list[dict[str, Any]] = []
    for item in records:
        if isinstance(item, dict) and item.get("业务键") is not None:
            parsed.append({"业务键": str(item["业务键"]), "值": str(item.get("值", ""))})

    existing = [
        row for row in store.rows("file_evidence")
        if str(row.get("文件路径")) == path and str(row.get("端")) == side
    ]
    if existing:
        row = existing[-1]
        row.update({
            "证据类型": evidence_type,
            "可读": bool(readable),
            "记录": parsed,
            "记录数": len(parsed),
            "记录摘要": "、".join(item["业务键"] for item in parsed) or "空内容",
            "说明": note or f"补交{side}证据",
            "注入时间": now_text(),
            "status": evidence_type,
            "pending": not readable,
            "abnormal": not readable,
        })
    else:
        row = {
            "id": _next_id("file_evidence"),
            "文件路径": path,
            "文件名": file_name(path),
            "端": side,
            "证据类型": evidence_type,
            "可读": bool(readable),
            "记录": parsed,
            "记录数": len(parsed),
            "记录摘要": "、".join(item["业务键"] for item in parsed) or "空内容",
            "说明": note or f"补交{side}证据",
            "注入时间": now_text(),
            "status": evidence_type,
            "pending": not readable,
            "abnormal": not readable,
        }
        store.rows("file_evidence").append(row)

    # 受影响文件的证据列同步刷新；缺证状态下恢复后可自动进入比对范围。
    for commit_file in store.rows("commit_file"):
        if str(commit_file.get("文件路径")) != path:
            continue
        if side == SOURCE:
            commit_file["源端证据"] = evidence_type
        else:
            commit_file["目标端证据"] = evidence_type
        if str(commit_file.get("比对状态")) == "待证据":
            commit_file["比对状态"] = "待比对"
            commit_file["status"] = "待比对"

    store.flush()
    return {
        "ok": True,
        "message": f"已补交{path}的{side}证据",
        "证据": row,
    }


# ---------------------------------------------------------------------------
# 写入端版本
# ---------------------------------------------------------------------------
def writer_version() -> str:
    return store.writer_version()


def set_writer_version(version: str) -> dict[str, Any]:
    if version not in ("v1", "v2"):
        return {"ok": False, "message": "写入端版本只支持 v1 或 v2"}
    store.set_writer_version(version)
    store.flush()
    return {"ok": True, "message": f"写入端已切换到 {version}", "写入端版本": version}
