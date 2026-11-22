"""按任务叙述复现一遍提交清单迁移流程（使用服务层，不启 HTTP）。

运行：
    cd backend && .venv/bin/python -m scripts.demo_commit_migration

流程对应题面：
1. 首个窗口只有一次恢复机会：A 按 v1 单路径比对，BC 因 c 缺证只处理回执外的范围；
2. 据回执核查 b 是否已独立产出差异行；
3. 回退写入端到 v1，登记单路径快照 D；
4. 重启数据库后补交 c 的源端证据；
5. 第二个窗口只处理仍需处理的路径，再从比对入口核对消费次数/首次差异时间/清单版本。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from app.services import commit_recovery, service_for
from app.store import store


def dump(title: str, payload: object) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="commit-migration-")
    db_path = str(Path(workdir) / "migration.sqlite3")
    store.reopen(db_path)

    state = commit_recovery.comparison()
    dump("初始状态", [
        {
            "快照": s["快照别名"],
            "清单版本": s["清单版本"],
            "清单": s["数据文件清单"],
            "比对状态": s["比对状态"],
            "文件": [
                {
                    "文件名": f["文件名"],
                    "源端证据": f["源端证据"],
                    "目标端证据": f["目标端证据"],
                    "消费次数": f["消费次数"],
                    "差异行数": f["差异行数"],
                    "回执": f["回执编号"],
                }
                for f in s["文件"]
            ],
        }
        for s in state["快照"]
    ])

    first = commit_recovery.recover(window="W-0001")
    dump("第一个恢复窗口", [
        {
            "快照": item["快照别名"],
            "清单版本": item["清单版本"],
            "实际处理范围": item["实际处理范围"],
            "回执跳过": item["跳过回执范围"],
            "缺证延后": item["延后缺证范围"],
            "待处理路径": item["待处理路径"],
            "比对完成": item["比对完成"],
            "独立回执核查": item["独立回执核查"],
        }
        for item in first["处理快照"]
    ])

    replay = commit_recovery.recover(window="W-0001")
    print(
        "\n同一窗口重复调用 -> 已重放:",
        replay["已重放"],
        "| 新差异行总数:",
        sum(len(item["新差异行"]) for item in replay["处理快照"]),
    )

    print("\n回退写入端:", commit_recovery.set_writer_version("v1"))
    entry, missing = service_for("iceberg_commit").create_entry({
        "快照编号": "IC-0003",
        "快照别名": "D",
        "目标表": "ads.order_events_iceberg",
        "数据文件": "s3://warehouse/ods/order_snapshot_d/d.parquet",
    })
    dump("旧版入口登记单路径快照 D", {
        "missing": missing,
        "清单版本": entry["清单版本"],
        "数据文件清单": entry["数据文件清单"],
    })

    store.reopen(db_path)  # 模拟重启
    print("\n重启后启动次数:", store.meta("boot_count"), "写入端:", store.writer_version())
    print("重启后 BC 清单原样可读:", commit_recovery.find_snapshot(2)["数据文件清单"])

    print("\n补交 c 源端证据:", commit_recovery.supply_evidence(
        "s3://warehouse/ods/order_snapshot_bc/c.parquet", "源端"))

    second = commit_recovery.recover(window="W-0002")
    dump("第二个恢复窗口", [
        {
            "快照": item["快照别名"],
            "实际处理范围": item["实际处理范围"],
            "回执跳过": item["跳过回执范围"],
            "待处理路径": item["待处理路径"],
            "比对完成": item["比对完成"],
        }
        for item in second["处理快照"]
    ])

    final = commit_recovery.comparison()
    dump("最终比对核对", {
        "消费次数按文件": final["消费次数按文件"],
        "快照": [
            {
                "快照": s["快照别名"],
                "清单版本": s["清单版本"],
                "比对状态": s["比对状态"],
                "文件": [
                    {
                        "文件": f["文件名"],
                        "清单版本": f["清单版本"],
                        "消费次数": f["消费次数"],
                        "比对状态": f["比对状态"],
                        "首次差异时间": f["首次差异时间"],
                        "回执": f["回执编号"],
                    }
                    for f in s["文件"]
                ],
            }
            for s in final["快照"]
        ],
        "差异行": [
            {
                "快照": row["快照别名"],
                "清单版本": row["清单版本"],
                "文件": row["文件名"],
                "业务键": row["业务键"],
                "首次出现时间": row["首次出现时间"],
            }
            for row in final["差异行"]
        ],
    })


if __name__ == "__main__":
    main()
