"""持久化业务仓库：种子数据 + 迁移夹具 + SQLite 可重启存储。

首次启动（数据库里还没有任何记录）时会写入 ``app/seed.py`` 的示例数据，
再应用 ``app/fixtures/commit_manifest.py`` 的提交清单迁移夹具。
之后的启动全部从数据库原样加载，因此进程重启不会把已登记的快照、
已补齐的证据或已生效的回执冲掉。

业务服务继续只和 ``rows(module)`` 打交道；需要落盘时调用 ``flush()``。
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from app.db import Database
from app.fixtures.commit_manifest import apply_fixture
from app.seed import SEED_ROWS

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "var" / "flowforge.sqlite3"

# 夹具额外引入的表；这些表不来自 seed.py。
FIXTURE_TABLES = (
    "file_evidence",
    "commit_file",
    "commit_receipt",
    "data_diff_row",
    "commit_recovery_run",
)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Store:
    def __init__(self, path: str | Path | None = None) -> None:
        configured = path or os.environ.get("APP_DB_PATH") or DEFAULT_DB_PATH
        self.db = Database(configured)
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._bootstrap()

    def reopen(self, path: str | Path) -> None:
        """切换到一个新的数据库文件（测试与本地重置用）。"""
        try:
            self.db.close()
        except Exception:  # noqa: BLE001 - 已关闭的连接再关一次不算错误
            pass
        self.db = Database(path)
        self._tables = {}
        self._bootstrap()

    # ---- 初始化 -------------------------------------------------------
    def _bootstrap(self) -> None:
        loaded = self.db.load_records()
        if not loaded:
            tables: dict[str, list[dict[str, Any]]] = {
                name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
            }
            apply_fixture(tables)
            self._tables = tables
            for name in tables:
                self.db.save_module(name, tables[name])
            self.db.set_meta("seeded_at", now_text())
            self.db.set_meta("fixture", "commit_manifest:v1")
        else:
            self._tables = loaded
            for name in FIXTURE_TABLES:
                self._tables.setdefault(name, [])
        if self.db.get_meta("writer_version") is None:
            self.db.set_meta("writer_version", "v2")
        boot_count = int(self.db.get_meta("boot_count") or "0") + 1
        self.db.set_meta("boot_count", str(boot_count))
        self.db.set_meta("last_boot_at", now_text())

    # ---- 读取 ---------------------------------------------------------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def meta(self, key: str) -> str | None:
        return self.db.get_meta(key)

    def set_meta(self, key: str, value: str) -> None:
        self.db.set_meta(key, value)

    def writer_version(self) -> str:
        return self.db.get_meta("writer_version") or "v2"

    def set_writer_version(self, version: str) -> None:
        self.db.set_meta("writer_version", version)

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ---- 写入 ---------------------------------------------------------
    def flush(self, module: str | None = None) -> None:
        names = [module] if module else list(self._tables)
        for name in names:
            self.db.save_module(name, self.rows(name))

    def reset(self) -> None:
        """丢弃全部记录并重新写入种子 + 迁移夹具。"""
        self._tables = {}
        for name in self.db.load_records():
            self.db.save_module(name, [])
        tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        apply_fixture(tables)
        self._tables = tables
        self.flush()
        self.db.set_meta("seeded_at", now_text())
        self.db.set_meta("writer_version", "v2")
        self.db.set_meta("boot_count", "1")
        self.db.set_meta("last_boot_at", now_text())


store = Store()
