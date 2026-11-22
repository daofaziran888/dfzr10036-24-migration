"""提交清单迁移的回归测试（只依赖标准库 unittest）。

覆盖：
* 夹具里 v1 单路径快照 A 与 v2 多路径快照 BC 并存；
* 同一快照只有一份文件证据齐全时仍是部分完成；
* 缺证时恢复只处理证据齐全的文件，缺证文件延后；
* 已生效的单文件确认回执阻止重复消费与重复差异行；
* 同一窗口只有一次恢复机会，重复调用回放首次结果；
* 回退写入端后旧版入口仍能登记单路径快照，重启后原样可读；
* 补齐证据后第二次恢复只消费仍需处理的路径；
* 差异行关联快照、清单版本、文件，首次出现时间不被覆盖；
* 恢复只认快照记录路径，拒绝管道里的同名文件。
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services import commit_recovery, service_for
from app.store import store

BC = "IC-0002"
A = "IC-0001"
A_FILE = "s3://warehouse/ods/order_snapshot_a/a.parquet"
B_FILE = "s3://warehouse/ods/order_snapshot_bc/b.parquet"
C_FILE = "s3://warehouse/ods/order_snapshot_bc/c.parquet"
D_FILE = "s3://warehouse/ods/order_snapshot_d/d.parquet"
DECOY = "s3://warehouse/pipeline-current/b.parquet"


class CommitMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "migration.sqlite3")
        store.reopen(self.db_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # ---- 夹具 ---------------------------------------------------------
    def test_fixture_has_v1_and_v2_snapshots(self) -> None:
        snapshot_a = commit_recovery.find_snapshot(1)
        snapshot_bc = commit_recovery.find_snapshot(2)
        self.assertEqual(commit_recovery.manifest_version(snapshot_a), "v1")
        self.assertEqual(commit_recovery.snapshot_files(snapshot_a), [A_FILE])
        self.assertEqual(commit_recovery.manifest_version(snapshot_bc), "v2")
        self.assertEqual(commit_recovery.snapshot_files(snapshot_bc), [B_FILE, C_FILE])
        # b 有独立回执和差异行；c 只有目标端目录证据。
        self.assertIsNotNone(commit_recovery.effective_receipt(BC, B_FILE))
        self.assertTrue(commit_recovery.evidence_status(C_FILE, "目标端")["可读"])
        self.assertFalse(commit_recovery.evidence_status(C_FILE, "源端")["可读"])

    # ---- 缺证恢复 -----------------------------------------------------
    def test_first_window_partial_when_c_missing(self) -> None:
        result = commit_recovery.recover(window="W-0001")
        by_alias = {item["快照别名"]: item for item in result["处理快照"]}

        # A 是旧快照，按单路径比对完成。
        self.assertEqual(by_alias["A"]["实际处理范围"], [A_FILE])
        self.assertTrue(by_alias["A"]["比对完成"])

        # BC 只有 b 因回执跳过、c 缺证延后，整份快照仍是部分完成。
        bc = by_alias["BC"]
        self.assertEqual(bc["实际处理范围"], [])
        self.assertEqual(bc["跳过回执范围"], [B_FILE])
        self.assertEqual(bc["延后缺证范围"], [C_FILE])
        self.assertEqual(bc["待处理路径"], [C_FILE])
        self.assertFalse(bc["比对完成"])
        self.assertEqual(bc["比对状态"], "部分完成")

        snapshot_bc = commit_recovery.find_snapshot(2)
        self.assertFalse(snapshot_bc["比对完成"])
        self.assertEqual(snapshot_bc["已比对文件数"], 1)
        self.assertEqual(snapshot_bc["待比对文件数"], 1)

    def test_receipt_prevents_repeat_diff_rows(self) -> None:
        b_rows_before = len(commit_recovery.diff_rows_for(BC, B_FILE))
        self.assertEqual(b_rows_before, 1)
        commit_recovery.recover(window="W-0001")
        commit_recovery.recover(window="W-0002")
        commit_recovery.recover(window="W-0003")
        self.assertEqual(len(commit_recovery.diff_rows_for(BC, B_FILE)), 1)
        b_file = next(
            row for row in store.rows("commit_file")
            if row["快照编号"] == BC and row["文件路径"] == B_FILE
        )
        self.assertEqual(b_file["消费次数"], 1)
        self.assertEqual(b_file["清单版本"], "v2")

    def test_same_window_replays_without_consuming_again(self) -> None:
        first = commit_recovery.recover(commit_entry_id=1, window="W-0001")
        first_count = len(first["处理快照"][0]["新差异行"])
        a_file = next(
            row for row in store.rows("commit_file")
            if row["快照编号"] == A and row["文件路径"] == A_FILE
        )
        self.assertEqual(a_file["消费次数"], 1)

        second = commit_recovery.recover(commit_entry_id=1, window="W-0001")
        self.assertTrue(second["已重放"])
        again = next(
            row for row in store.rows("commit_file")
            if row["快照编号"] == A and row["文件路径"] == A_FILE
        )
        self.assertEqual(again["消费次数"], 1)
        # 重复窗口不会再产出任何新差异行。
        replay = next(x for x in second["处理快照"] if x["快照别名"] == "A")
        self.assertEqual(len(replay["新差异行"]), first_count)
        self.assertEqual(len(commit_recovery.diff_rows_for(A, A_FILE)), first_count)

    # ---- 回退与重启 ---------------------------------------------------
    def test_legacy_writer_registers_single_path_and_survives_restart(self) -> None:
        commit_recovery.recover(window="W-0001")
        self.assertTrue(commit_recovery.set_writer_version("v1")["ok"])

        entry, missing = service_for("iceberg_commit").create_entry({
            "快照编号": "IC-0003",
            "快照别名": "D",
            "目标表": "ads.order_events_iceberg",
            "数据文件": D_FILE,
        })
        self.assertEqual(missing, [])
        self.assertEqual(entry["清单版本"], "v1")
        self.assertEqual(entry["数据文件清单"], [D_FILE])

        # 模拟进程重启：重新打开同一个数据库文件。
        store.reopen(self.db_path)
        self.assertEqual(store.writer_version(), "v1")
        register = {row["快照编号"]: row for row in store.rows("iceberg_commit")}
        self.assertEqual(register["IC-0003"]["清单版本"], "v1")
        self.assertEqual(register["IC-0003"]["数据文件清单"], [D_FILE])
        # 已确认的多路径快照重启后原样可读。
        self.assertEqual(
            register["IC-0002"]["数据文件清单"], [B_FILE, C_FILE]
        )
        self.assertEqual(commit_recovery.manifest_version(register["IC-0002"]), "v2")

    def test_second_window_after_evidence_only_consumes_c(self) -> None:
        commit_recovery.recover(window="W-0001")
        commit_recovery.set_writer_version("v1")
        service_for("iceberg_commit").create_entry({
            "快照编号": "IC-0003",
            "快照别名": "D",
            "目标表": "ads.order_events_iceberg",
            "数据文件": D_FILE,
        })
        store.reopen(self.db_path)

        supplied = commit_recovery.supply_evidence(C_FILE, "源端")
        self.assertTrue(supplied["ok"])

        result = commit_recovery.recover(window="W-0002")
        by_alias = {item["快照别名"]: item for item in result["处理快照"]}
        self.assertEqual(by_alias["BC"]["实际处理范围"], [C_FILE])
        self.assertEqual(by_alias["BC"]["跳过回执范围"], [B_FILE])
        self.assertTrue(by_alias["BC"]["比对完成"])
        self.assertEqual(by_alias["D"]["实际处理范围"], [D_FILE])
        self.assertTrue(by_alias["D"]["比对完成"])

        comparison = commit_recovery.comparison()
        self.assertEqual(comparison["消费次数按文件"], {
            A_FILE: 1, B_FILE: 1, C_FILE: 1, D_FILE: 1,
        })

    # ---- 差异行关联 ---------------------------------------------------
    def test_diff_rows_carry_snapshot_version_file_and_first_seen(self) -> None:
        before = {
            row["业务键"]: row for row in commit_recovery.diff_rows_for(BC, B_FILE)
        }
        self.assertEqual(before["B-2001"]["清单版本"], "v2")
        self.assertEqual(before["B-2001"]["快照编号"], BC)
        self.assertEqual(before["B-2001"]["文件路径"], B_FILE)
        first_seen_b = before["B-2001"]["首次出现时间"]

        commit_recovery.supply_evidence(C_FILE, "源端")
        commit_recovery.recover(window="W-0002")
        # 回退到旧版写入端并登记单路径快照 D。
        commit_recovery.set_writer_version("v1")
        service_for("iceberg_commit").create_entry({
            "快照编号": "IC-0003",
            "快照别名": "D",
            "目标表": "ads.order_events_iceberg",
            "数据文件": D_FILE,
        })
        commit_recovery.recover(window="W-0003")

        rows = commit_recovery.comparison()["差异行"]
        keys = {row["业务键"] for row in rows}
        self.assertLessEqual({"A-1002", "B-2001", "C-3002", "D-4001"}, keys)
        for row in rows:
            self.assertTrue(row["快照编号"])
            self.assertTrue(row["文件路径"])
            self.assertTrue(row["清单版本"])
            self.assertTrue(row["首次出现时间"])
        after_b = next(row for row in rows if row["业务键"] == "B-2001")
        self.assertEqual(after_b["首次出现时间"], first_seen_b)

        files = {
            item["文件名"]: item
            for snapshot in commit_recovery.comparison()["快照"]
            for item in snapshot["文件"]
        }
        self.assertEqual(files["b.parquet"]["清单版本"], "v2")
        self.assertEqual(files["a.parquet"]["清单版本"], "v1")
        self.assertEqual(files["d.parquet"]["清单版本"], "v1")

    def test_partial_result_never_marks_snapshot_complete(self) -> None:
        commit_recovery.recover(window="W-0001")
        snapshot_bc = commit_recovery.find_snapshot(2)
        self.assertFalse(snapshot_bc["比对完成"])
        self.assertEqual(snapshot_bc["比对状态"], "部分完成")
        # 再次对同一缺证快照开新窗口，也不能被标成完成。
        commit_recovery.recover(window="W-0009")
        self.assertFalse(commit_recovery.find_snapshot(2)["比对完成"])

    # ---- 路径约束 -----------------------------------------------------
    def test_restore_uses_recorded_paths_only(self) -> None:
        rejected = commit_recovery.supply_evidence(DECOY, "源端")
        self.assertFalse(rejected["ok"])
        self.assertIn("同名文件", rejected["message"])

        commit_recovery.recover(window="W-0001")
        comparison = commit_recovery.comparison()
        consumed = comparison["消费次数按文件"]
        self.assertNotIn(DECOY, consumed)
        decoy_rows = [
            row for row in store.rows("data_diff_row")
            if row["文件路径"] == DECOY
        ]
        self.assertEqual(decoy_rows, [])

    def test_unreadable_side_keeps_file_pending(self) -> None:
        commit_recovery.supply_evidence(B_FILE, "目标端", records=[], readable=False,
                                        evidence_type="缺失")
        before = len(commit_recovery.diff_rows_for(BC, B_FILE))
        result = commit_recovery.recover(window="W-0001")
        by_alias = {item["快照别名"]: item for item in result["处理快照"]}
        # b 有回执，即使目标端缺证也不应被重复消费。
        self.assertIn(B_FILE, by_alias["BC"]["跳过回执范围"])
        self.assertEqual(len(commit_recovery.diff_rows_for(BC, B_FILE)), before)


if __name__ == "__main__":
    unittest.main()
