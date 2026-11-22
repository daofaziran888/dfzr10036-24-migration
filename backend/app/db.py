"""可重启的本地数据库（SQLite）。

旧实现把业务数据放在进程内存里，进程一退出示例数据就回到种子的初始状态。
提交文件清单迁移需要一个重启后仍然原样可读的存储，所以这里用标准库 sqlite3
做一层很薄的持久化：

* ``records`` 表按模块保存整行 JSON，模块内按 ``seq`` 保持登记顺序；
* ``meta`` 表保存启动计数、数据库结构版本等少量键值。

业务代码仍然通过 ``Store.rows(module)`` 拿到可读写的行列表，不直接碰 SQL，
因此各模块服务不需要为了持久化重写。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS records (
                    module TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    row_id TEXT,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (module, seq)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    # ---- 读取 ---------------------------------------------------------
    def load_records(self) -> dict[str, list[dict[str, Any]]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT module, payload FROM records ORDER BY module, seq"
            ).fetchall()
        result: dict[str, list[dict[str, Any]]] = {}
        for module, payload in rows:
            result.setdefault(module, []).append(json.loads(payload))
        return result

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,)
            ).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    # ---- 写入 ---------------------------------------------------------
    def save_module(self, module: str, rows: list[dict[str, Any]]) -> None:
        """整模块替换；行数很少，比逐行 diff 更不容易出现半截状态。"""
        payloads = [
            (
                module,
                seq,
                str(row.get("id", "")),
                json.dumps(row, ensure_ascii=False, default=str),
            )
            for seq, row in enumerate(rows)
        ]
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM records WHERE module = ?", (module,))
                if payloads:
                    self._conn.executemany(
                        "INSERT INTO records(module, seq, row_id, payload) "
                        "VALUES(?, ?, ?, ?)",
                        payloads,
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()
