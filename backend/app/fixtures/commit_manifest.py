"""提交文件清单迁移夹具（v1 单路径 -> v2 提交文件清单）。

这份夹具只用于本地/演示环境：它把现有种子里的两条湖仓提交快照改造成
迁移现场的 A / BC 两个快照，并在源端、目标端预置可区分的文件记录，
使得后续的数据比对能够为 a、b、c、d 四个文件分别产出不同业务键的差异行。

关键设定：

* 快照 A：旧版单路径快照，只记录 ``数据文件``（清单版本 v1），等待冲突比对。
* 快照 BC：新版多路径快照，``数据文件清单`` 为 [b.parquet, c.parquet]（v2）。
* b.parquet：源端、目标端都有可读文件内容，且已经由新写入端独立比对过一次，
  留有生效的单文件确认回执和差异行；恢复时应识别回执并跳过重复产出。
* c.parquet：初始只有目标端目录证据，源端证据缺失；补齐后恢复才可继续。
* d.parquet：源端、目标端均有可读内容，等旧版写入端登记快照 D 后再消费。
* ``pipeline-current/b.parquet`` 是同名诱饵文件：恢复只能使用快照里记录的
  路径，不能拿当前管道里的同名文件顶替。
"""
from __future__ import annotations

from typing import Any

SNAPSHOT_A_FILE = "s3://warehouse/ods/order_snapshot_a/a.parquet"
SNAPSHOT_BC_B_FILE = "s3://warehouse/ods/order_snapshot_bc/b.parquet"
SNAPSHOT_BC_C_FILE = "s3://warehouse/ods/order_snapshot_bc/c.parquet"
SNAPSHOT_D_FILE = "s3://warehouse/ods/order_snapshot_d/d.parquet"
DECOY_B_FILE = "s3://warehouse/pipeline-current/b.parquet"

INDEPENDENT_B_TIME = "2026-11-18 10:20:00"
EVIDENCE_TIME = "2026-11-18 10:00:00"


def _record(key: str, value: Any) -> dict[str, Any]:
    return {"业务键": key, "值": value}


def _evidence(
    row_id: int,
    path: str,
    side: str,
    kind: str,
    readable: bool,
    records: list[dict[str, Any]],
    note: str,
) -> dict[str, Any]:
    return {
        "id": row_id,
        "文件路径": path,
        "文件名": path.rsplit("/", 1)[-1],
        "端": side,
        "证据类型": kind,
        "可读": readable,
        "记录": records,
        "记录数": len(records),
        "记录摘要": "、".join(str(item["业务键"]) for item in records) or "目录存在，无内容记录",
        "说明": note,
        "注入时间": EVIDENCE_TIME,
        "status": "文件内容" if kind == "文件内容" else kind,
        "pending": not readable,
        "abnormal": not readable,
    }


def _commit_file(
    row_id: int,
    path: str,
    snapshot: str,
    alias: str,
    version: str,
    source_evidence: str,
    target_evidence: str,
    consumed: int,
    diff_count: int,
    compare_status: str,
    first_diff_at: str | None,
    receipt_no: str | None,
) -> dict[str, Any]:
    return {
        "id": row_id,
        "文件路径": path,
        "文件名": path.rsplit("/", 1)[-1],
        "快照编号": snapshot,
        "快照别名": alias,
        "清单版本": version,
        "源端证据": source_evidence,
        "目标端证据": target_evidence,
        "消费次数": consumed,
        "差异行数": diff_count,
        "首次差异时间": first_diff_at,
        "最近消费时间": INDEPENDENT_B_TIME if consumed else None,
        "比对状态": compare_status,
        "回执编号": receipt_no,
        "status": compare_status,
        "pending": compare_status != "已比对",
        "abnormal": diff_count > 0,
    }


def _iceberg_snapshot_a() -> dict[str, Any]:
    return {
        "id": 1,
        "status": "提交冲突",
        "pending": True,
        "abnormal": True,
        "快照编号": "IC-0001",
        "快照别名": "A",
        "目标表": "ads.order_events_iceberg",
        "数据文件": SNAPSHOT_A_FILE,
        "数据文件清单": [SNAPSHOT_A_FILE],
        "数据文件数量": 1,
        "清单文件": "metadata/manifest-1001.avro",
        "清单版本": "v1",
        "写入端版本": "v1",
        "快照ID": "9182736450123456789",
        "提交状态": "CONFLICT",
        "影响行数": 128340,
        "提交时间": "2026-11-18 10:32:45",
        "比对状态": "未开始",
        "比对完成": False,
        "已比对文件数": 0,
        "待比对文件数": 1,
        "差异行合计": 0,
        "最近恢复窗口": None,
        "最近恢复时间": None,
    }


def _iceberg_snapshot_bc() -> dict[str, Any]:
    return {
        "id": 2,
        "status": "提交冲突",
        "pending": True,
        "abnormal": True,
        "快照编号": "IC-0002",
        "快照别名": "BC",
        "目标表": "ads.order_events_iceberg",
        "数据文件": f"{SNAPSHOT_BC_B_FILE},{SNAPSHOT_BC_C_FILE}",
        "数据文件清单": [SNAPSHOT_BC_B_FILE, SNAPSHOT_BC_C_FILE],
        "数据文件数量": 2,
        "清单文件": "metadata/manifest-2005-v2.avro",
        "清单版本": "v2",
        "写入端版本": "v2",
        "快照ID": "冲突-并发写入检测",
        "提交状态": "CONFLICT",
        "影响行数": 96000,
        "提交时间": "2026-11-18 10:31:00",
        # b 已由新写入端独立比对过，c 还缺源端证据：这是部分结果，不能算比对完成。
        "比对状态": "部分完成",
        "比对完成": False,
        "已比对文件数": 1,
        "待比对文件数": 1,
        "差异行合计": 1,
        "最近恢复窗口": None,
        "最近恢复时间": None,
    }


def _evidence_rows() -> list[dict[str, Any]]:
    return [
        # a.parquet：源端、目标端都可读。
        _evidence(
            1, SNAPSHOT_A_FILE, "源端", "文件内容", True,
            [_record("A-1001", "100.00"), _record("A-1002", "200.00")],
            "订单旧快照 a 源端可读",
        ),
        _evidence(
            2, SNAPSHOT_A_FILE, "目标端", "文件内容", True,
            [_record("A-1001", "100.00"), _record("A-1002", "260.00")],
            "订单旧快照 a 目标端可读",
        ),
        # b.parquet：源端、目标端都可读，且已被新写入端独立比对。
        _evidence(
            3, SNAPSHOT_BC_B_FILE, "源端", "文件内容", True,
            [_record("B-2001", "300.00"), _record("B-2002", "400.00")],
            "快照 BC 的 b 源端可读",
        ),
        _evidence(
            4, SNAPSHOT_BC_B_FILE, "目标端", "文件内容", True,
            [_record("B-2001", "350.00"), _record("B-2002", "400.00")],
            "快照 BC 的 b 目标端可读",
        ),
        # c.parquet：初始只有目标端目录证据，源端证据缺失。
        _evidence(
            5, SNAPSHOT_BC_C_FILE, "目标端", "目录证据", True,
            [_record("C-3001", "500.00"), _record("C-3002", "600.00")],
            "快照 BC 的 c 仅有目标端目录证据，内容在目录快照内可读",
        ),
        # d.parquet：源端、目标端都可读，等旧版写入端登记快照 D。
        _evidence(
            6, SNAPSHOT_D_FILE, "源端", "文件内容", True,
            [_record("D-4001", "700.00"), _record("D-4002", "800.00")],
            "快照 D 的 d 源端可读",
        ),
        _evidence(
            7, SNAPSHOT_D_FILE, "目标端", "文件内容", True,
            [_record("D-4001", "760.00"), _record("D-4002", "800.00")],
            "快照 D 的 d 目标端可读",
        ),
        # 同名诱饵：当前管道目录下的 b.parquet，恢复绝不能消费它。
        _evidence(
            8, DECOY_B_FILE, "源端", "文件内容", True,
            [_record("X-9999", "999.00")],
            "当前管道同名文件，仅用于验证恢复按快照记录路径取数",
        ),
        _evidence(
            9, DECOY_B_FILE, "目标端", "文件内容", True,
            [_record("X-9999", "0.00")],
            "当前管道同名文件，仅用于验证恢复按快照记录路径取数",
        ),
    ]


def _commit_file_rows() -> list[dict[str, Any]]:
    return [
        _commit_file(
            1, SNAPSHOT_A_FILE, "IC-0001", "A", "v1",
            "文件内容", "文件内容", 0, 0, "待比对", None, None,
        ),
        _commit_file(
            2, SNAPSHOT_BC_B_FILE, "IC-0002", "BC", "v2",
            "文件内容", "文件内容", 1, 1, "已比对",
            INDEPENDENT_B_TIME, "RCPT-0001",
        ),
        _commit_file(
            3, SNAPSHOT_BC_C_FILE, "IC-0002", "BC", "v2",
            "缺失", "目录证据", 0, 0, "待证据", None, None,
        ),
    ]


def _receipt_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": 1,
            "回执编号": "RCPT-0001",
            "快照编号": "IC-0002",
            "快照别名": "BC",
            "文件路径": SNAPSHOT_BC_B_FILE,
            "文件名": "b.parquet",
            "清单版本": "v2",
            "回执类型": "单文件确认",
            "状态": "已生效",
            "来源": "独立比对",
            "生成时间": INDEPENDENT_B_TIME,
            "生效时间": INDEPENDENT_B_TIME,
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
    ]


def _diff_rows() -> list[dict[str, Any]]:
    return [
        {
            "id": 1,
            "任务编号": "DDF-0002",
            "快照编号": "IC-0002",
            "快照别名": "BC",
            "清单版本": "v2",
            "文件路径": SNAPSHOT_BC_B_FILE,
            "文件名": "b.parquet",
            "业务键": "B-2001",
            "源端值": "300.00",
            "目标端值": "350.00",
            "差异类型": "值不一致",
            "首次出现时间": INDEPENDENT_B_TIME,
            "最近出现时间": INDEPENDENT_B_TIME,
            "恢复窗口": None,
            "status": "存在差异",
            "pending": False,
            "abnormal": True,
        }
    ]


def _diff_task_b() -> dict[str, Any]:
    return {
        "id": 2,
        "status": "存在差异",
        "pending": True,
        "abnormal": True,
        "任务编号": "DDF-0002",
        "源端": f"源端文件:{SNAPSHOT_BC_B_FILE}",
        "目标端": f"目标端文件:{SNAPSHOT_BC_B_FILE}",
        "比对字段": "全字段",
        "差异类型": "数据不一致",
        "差异行数": 1,
        "修复建议": "按快照记录路径重放差异行",
        "比对时间": INDEPENDENT_B_TIME,
        "快照编号": "IC-0002",
        "快照别名": "BC",
        "清单版本": "v2",
        "文件路径": SNAPSHOT_BC_B_FILE,
        "消费次数": 1,
        "首次差异时间": INDEPENDENT_B_TIME,
        "恢复窗口": None,
    }


def apply_fixture(tables: dict[str, list[dict[str, Any]]]) -> None:
    """在种子副本上应用迁移夹具。"""
    tables["iceberg_commit"] = [_iceberg_snapshot_a(), _iceberg_snapshot_bc()]

    data_diff = [row for row in tables.get("data_diff", []) if row.get("任务编号") != "DDF-0002"]
    data_diff.append(_diff_task_b())
    tables["data_diff"] = data_diff

    tables["file_evidence"] = _evidence_rows()
    tables["commit_file"] = _commit_file_rows()
    tables["commit_receipt"] = _receipt_rows()
    tables["data_diff_row"] = _diff_rows()
    tables["commit_recovery_run"] = []
