"""SQLite 存储：批次、输入文件、事件、快照、水位序列、阶段、合计、运行清单。

约定：

- 金额列一律 TEXT（Decimal 字符串），不落 float；主键 pk 一律 TEXT，前导零与
  超过 2^53 的整数都按原样保存；
- 一次运行的所有行在同一个事务里「先删后插」，失败整体回滚，不留半批数据；
- 页面永远只展示一个批次，所有查询都带 batch_id。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    batch_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    input_digest TEXT NOT NULL,
    watermark_utc TEXT,
    max_event_time TEXT,
    params_json TEXT NOT NULL DEFAULT '{}',
    note TEXT NOT NULL DEFAULT '',
    replay_of TEXT,
    published_seq INTEGER,
    published_event_time TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_key ON batches(batch_key, status);
CREATE TABLE IF NOT EXISTS inputs (
    batch_id TEXT NOT NULL, seq INTEGER NOT NULL, name TEXT NOT NULL,
    bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, declared_rows INTEGER,
    encoding TEXT, declared TEXT, kind TEXT, status TEXT NOT NULL, code TEXT,
    note TEXT, lines INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
    batch_id TEXT NOT NULL, row_ref TEXT NOT NULL, seq INTEGER NOT NULL,
    table_name TEXT NOT NULL, pk TEXT NOT NULL, version INTEGER,
    op TEXT, event_time_utc TEXT, event_time_raw TEXT, schema_version TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}', amount TEXT, qty INTEGER,
    row_class TEXT NOT NULL, reason_code TEXT, field TEXT, raw_value TEXT,
    detail TEXT, late INTEGER NOT NULL DEFAULT 0, flags_json TEXT NOT NULL DEFAULT '[]',
    raw_text TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_class ON events(batch_id, row_class);
CREATE TABLE IF NOT EXISTS snapshots (
    batch_id TEXT NOT NULL, table_name TEXT NOT NULL, pk TEXT NOT NULL,
    version INTEGER NOT NULL, op TEXT NOT NULL, amount TEXT, qty INTEGER,
    event_time_utc TEXT NOT NULL, row_ref TEXT NOT NULL,
    late INTEGER NOT NULL DEFAULT 0, pending INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS watermarks (
    batch_id TEXT NOT NULL, seq INTEGER NOT NULL, row_ref TEXT NOT NULL,
    event_time_utc TEXT NOT NULL, watermark_utc TEXT NOT NULL,
    late INTEGER NOT NULL DEFAULT 0, behind_seconds INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS stages (
    batch_id TEXT NOT NULL, stage TEXT NOT NULL, in_count INTEGER NOT NULL,
    out_count INTEGER NOT NULL, quarantine_count INTEGER NOT NULL,
    status TEXT NOT NULL, elapsed_ms INTEGER NOT NULL, note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS totals (
    batch_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    batch_id TEXT PRIMARY KEY, json TEXT NOT NULL
);
-- M8 保留与回收：消费者确认位置、保留运行、逐事件裁决台账、归档台账。
CREATE TABLE IF NOT EXISTS consumers (
    batch_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    confirmed_seq INTEGER NOT NULL DEFAULT 0,
    confirmed_at TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    note TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (batch_id, consumer_id)
);
CREATE TABLE IF NOT EXISTS retention_runs (
    retention_id TEXT PRIMARY KEY,
    run_key TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    policy TEXT NOT NULL,
    status TEXT NOT NULL,
    published_seq INTEGER NOT NULL DEFAULT 0,
    published_event_time TEXT,
    slowest_seq INTEGER,
    slowest_consumer TEXT,
    boundary_seq INTEGER NOT NULL DEFAULT 0,
    boundary_event_time TEXT,
    ceiling_seq INTEGER NOT NULL DEFAULT 0,
    blocked_json TEXT NOT NULL DEFAULT '[]',
    resume_json TEXT NOT NULL DEFAULT '{}',
    counters_json TEXT NOT NULL DEFAULT '{}',
    reconcile_json TEXT NOT NULL DEFAULT '[]',
    params_json TEXT NOT NULL DEFAULT '{}',
    cursor_seq INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    finished_at TEXT,
    note TEXT NOT NULL DEFAULT '',
    parent_id TEXT
);
-- run_key 不唯一：同一保留键回搬（restored）后再跑要生成新运行；复用只取 active 状态。
CREATE INDEX IF NOT EXISTS idx_retention_run_key ON retention_runs(run_key);
CREATE INDEX IF NOT EXISTS idx_retention_batch ON retention_runs(batch_id, status);
CREATE TABLE IF NOT EXISTS retention_items (
    retention_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    row_ref TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pk TEXT NOT NULL,
    version INTEGER,
    op TEXT,
    event_time_utc TEXT,
    row_class TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    location TEXT NOT NULL DEFAULT 'hot',
    detail TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (retention_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_retention_items_decision
    ON retention_items(retention_id, decision, reason_code);
CREATE TABLE IF NOT EXISTS retention_archive (
    batch_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    retention_id TEXT NOT NULL,
    row_ref TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pk TEXT NOT NULL,
    version INTEGER,
    op TEXT,
    event_time_utc TEXT,
    event_time_raw TEXT,
    schema_version TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    amount TEXT,
    qty INTEGER,
    row_class TEXT NOT NULL,
    reason_code TEXT,
    field TEXT,
    raw_value TEXT,
    detail TEXT,
    late INTEGER NOT NULL DEFAULT 0,
    flags_json TEXT NOT NULL DEFAULT '[]',
    raw_text TEXT NOT NULL DEFAULT '',
    archived_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_retention_archive_batch ON retention_archive(batch_id, seq);
"""


class Store:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._ensure_columns(conn)

    @staticmethod
    def _ensure_columns(conn) -> None:
        """轻量迁移：老库缺列时补上，避免升级后 500。"""
        wanted = {
            "inputs": {"declared": "TEXT"},
            "batches": {"published_seq": "INTEGER",
                        "published_event_time": "TEXT"},
        }
        for table, columns in wanted.items():
            existing = {row["name"] for row in conn.execute(
                f"PRAGMA table_info({table})").fetchall()}
            for column, ddl in columns.items():
                if column not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        # run_key 只需普通索引：回搬（restored）后再跑要能生成同键的新运行
        for index in conn.execute("PRAGMA index_list(retention_runs)").fetchall():
            if index["name"] == "idx_retention_run_key" and index["unique"]:
                conn.execute("DROP INDEX idx_retention_run_key")
                conn.execute("CREATE INDEX idx_retention_run_key "
                             "ON retention_runs(run_key)")

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------- 批次 ----------
    def get_batch(self, batch_id: str) -> dict | None:
        rows = self._all("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        return rows[0] if rows else None

    def find_done_by_key(self, batch_key: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM batches WHERE batch_key=? AND status='done' "
            "ORDER BY created_at LIMIT 1", (batch_key,))
        return rows[0] if rows else None

    def latest_batch(self, status: str = "done") -> dict | None:
        rows = self._all(
            "SELECT * FROM batches WHERE status=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (status,))
        if not rows:
            rows = self._all("SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT 1",
                             ())
        return rows[0] if rows else None

    def list_batches(self, limit: int = 30) -> list[dict]:
        return self._all(
            "SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT ?", (limit,))

    def watermark_high(self) -> str | None:
        rows = self._all(
            "SELECT watermark_utc FROM batches WHERE status='done' "
            "AND watermark_utc IS NOT NULL ORDER BY watermark_utc DESC LIMIT 1", ())
        return rows[0]["watermark_utc"] if rows else None

    # ---------- 一次运行落库（单事务替换） ----------
    def save_run(self, *, batch: dict, inputs: list[dict], events: list[dict],
                 snapshots: list[dict], watermarks: list[dict], stages: list[dict],
                 totals: dict, manifest: dict) -> None:
        with self.connect() as conn:
            for table in ("inputs", "events", "snapshots", "watermarks", "stages", "totals"):
                conn.execute(f"DELETE FROM {table} WHERE batch_id=?", (batch["batch_id"],))
            for i, item in enumerate(inputs, 1):
                conn.execute(
                    """INSERT INTO inputs (batch_id,seq,name,bytes,sha256,declared_rows,
                        encoding,declared,kind,status,code,note,lines)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], i, item["name"], item["bytes"], item["sha256"],
                     item.get("declared_rows"), item.get("encoding"),
                     item.get("declared"), item.get("kind"), item["status"],
                     item.get("code"), item.get("note", ""), item.get("lines", 0)))
            for row in events:
                conn.execute(
                    """INSERT INTO events (batch_id,row_ref,seq,table_name,pk,version,op,
                        event_time_utc,event_time_raw,schema_version,payload_json,amount,qty,
                        row_class,reason_code,field,raw_value,detail,late,flags_json,raw_text)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], row["row_ref"], row["seq"], row["table"], row["pk"],
                     row.get("version"), row.get("op"), row.get("event_time"),
                     row.get("event_time_raw"), row.get("schema"),
                     json.dumps(row.get("payload", {}), ensure_ascii=False,
                                default=str),
                     None if row.get("amount") is None else str(row["amount"]),
                     row.get("qty"), row["row_class"], row.get("reason_code"),
                     row.get("field"), row.get("raw_value"), row.get("detail"),
                     1 if row.get("late") else 0,
                     json.dumps(row.get("flags", []), ensure_ascii=False),
                     row.get("raw_text", "")))
            for s in snapshots:
                conn.execute(
                    """INSERT INTO snapshots (batch_id,table_name,pk,version,op,amount,qty,
                        event_time_utc,row_ref,late,pending,payload_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], s["table"], s["pk"], s["version"], s["op"],
                     None if s.get("amount") is None else str(s["amount"]), s.get("qty"),
                     s["event_time"], s["row_ref"], 1 if s.get("late") else 0,
                     1 if s.get("pending") else 0,
                     json.dumps(s.get("payload", {}), ensure_ascii=False, default=str)))
            for w in watermarks:
                conn.execute(
                    """INSERT INTO watermarks (batch_id,seq,row_ref,event_time_utc,
                        watermark_utc,late,behind_seconds) VALUES (?,?,?,?,?,?,?)""",
                    (batch["batch_id"], w["seq"], w["row_ref"], w["event_time"],
                     w["watermark"], 1 if w.get("late") else 0, w.get("behind_seconds", 0)))
            for s in stages:
                conn.execute(
                    """INSERT INTO stages (batch_id,stage,in_count,out_count,
                        quarantine_count,status,elapsed_ms,note) VALUES (?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], s["stage"], s["in_count"], s["out_count"],
                     s["quarantine_count"], s["status"], s["elapsed_ms"], s.get("note", "")))
            for key, value in totals.items():
                conn.execute("INSERT INTO totals (batch_id,key,value) VALUES (?,?,?)",
                             (batch["batch_id"], key, str(value)))
            conn.execute(
                "INSERT INTO manifests (batch_id,json) VALUES (?,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET json=excluded.json",
                (batch["batch_id"], json.dumps(manifest, ensure_ascii=False, default=str)))
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,source,status,created_at,
                        finished_at,input_digest,watermark_utc,max_event_time,params_json,
                        note,replay_of,published_seq,published_event_time)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     note=excluded.note, input_digest=excluded.input_digest,
                     watermark_utc=excluded.watermark_utc,
                     max_event_time=excluded.max_event_time,
                     published_seq=excluded.published_seq,
                     published_event_time=excluded.published_event_time""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["source"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch.get("watermark_utc"),
                 batch.get("max_event_time"),
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 batch.get("note", ""), batch.get("replay_of"),
                 batch.get("published_seq"), batch.get("published_event_time")))


    # ---------- 快照（可重复物化） ----------
    def replace_snapshot(self, batch_id: str, rows: list[dict], totals: dict) -> None:
        """按同一 as_of 重算快照：先删后插，不新增事件行。"""
        with self.connect() as conn:
            conn.execute("DELETE FROM snapshots WHERE batch_id=?", (batch_id,))
            for s in rows:
                conn.execute(
                    """INSERT INTO snapshots (batch_id,table_name,pk,version,op,amount,qty,
                        event_time_utc,row_ref,late,pending,payload_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, s["table"], s["pk"], s["version"], s["op"],
                     None if s.get("amount") is None else str(s["amount"]), s.get("qty"),
                     s["event_time"], s["row_ref"], 1 if s.get("late") else 0,
                     1 if s.get("pending") else 0,
                     json.dumps(s.get("payload", {}), ensure_ascii=False, default=str)))
            for key, value in totals.items():
                conn.execute("DELETE FROM totals WHERE batch_id=? AND key=?",
                             (batch_id, key))
                conn.execute("INSERT INTO totals (batch_id,key,value) VALUES (?,?,?)",
                             (batch_id, key, str(value)))

    def save_attempt(self, *, batch: dict, inputs: list[dict], note: str = "") -> None:
        """失败尝试：只写批次与输入，不动任何已存在批次的结果行。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,source,status,created_at,
                        finished_at,input_digest,watermark_utc,max_event_time,params_json,
                        note,replay_of)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     note=excluded.note""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["source"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch.get("watermark_utc"),
                 batch.get("max_event_time"),
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 note or batch.get("note", ""), batch.get("replay_of")))
            conn.execute("DELETE FROM inputs WHERE batch_id=?", (batch["batch_id"],))
            for i, item in enumerate(inputs, 1):
                conn.execute(
                    """INSERT INTO inputs (batch_id,seq,name,bytes,sha256,declared_rows,
                        encoding,declared,kind,status,code,note,lines)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], i, item["name"], item["bytes"], item["sha256"],
                     item.get("declared_rows"), item.get("encoding"),
                     item.get("declared"), item.get("kind"), item["status"],
                     item.get("code"), item.get("note", ""), item.get("lines", 0)))

    # ---------- 读取 ----------
    def load_inputs(self, batch_id: str) -> list[dict]:
        return self._all("SELECT * FROM inputs WHERE batch_id=? ORDER BY seq", (batch_id,))

    def load_events(self, batch_id: str, row_class: str | None = None,
                    include_archived: bool = True) -> list[dict]:
        """事件行读取。

        默认把归档表里的历史事件一并返回（热区优先、按到达序号去重），这样回收
        之后事件流、证据导出与快照重算看到的仍是完整历史，记录不会因为回收消失；
        保留页需要区分热区时传 `include_archived=False`。
        """
        if row_class:
            rows = self._all(
                "SELECT * FROM events WHERE batch_id=? AND row_class=? ORDER BY seq",
                (batch_id, row_class))
        else:
            rows = self._all("SELECT * FROM events WHERE batch_id=? ORDER BY seq", (batch_id,))
        seen = {int(r["seq"]) for r in rows}
        if include_archived:
            if row_class:
                archived = self._all(
                    "SELECT * FROM retention_archive WHERE batch_id=? AND row_class=? "
                    "ORDER BY seq", (batch_id, row_class))
            else:
                archived = self._all(
                    "SELECT * FROM retention_archive WHERE batch_id=? ORDER BY seq",
                    (batch_id,))
            rows.extend(r for r in archived if int(r["seq"]) not in seen)
            rows.sort(key=lambda r: int(r["seq"]))
        return [self._event_row(r) for r in rows]

    @staticmethod
    def _event_row(r: dict) -> dict:
        r = dict(r)
        r["payload"] = json.loads(r.pop("payload_json") or "{}")
        r["flags"] = json.loads(r.pop("flags_json") or "[]")
        r["amount"] = None if r["amount"] is None else Decimal(r["amount"])
        r["late"] = bool(r["late"])
        r.pop("archived_at", None)
        return r

    def load_snapshot(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM snapshots WHERE batch_id=? ORDER BY table_name, pk", (batch_id,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json") or "{}")
            r["amount"] = None if r["amount"] is None else Decimal(r["amount"])
            r["late"] = bool(r["late"])
            r["pending"] = bool(r["pending"])
        return rows

    def load_watermarks(self, batch_id: str) -> list[dict]:
        return self._all(
            "SELECT * FROM watermarks WHERE batch_id=? ORDER BY seq", (batch_id,))

    def load_stages(self, batch_id: str) -> list[dict]:
        from . import config
        rows = self._all("SELECT * FROM stages WHERE batch_id=? ORDER BY rowid", (batch_id,))
        for row in rows:
            row["label"] = config.STAGE_LABELS.get(row["stage"], row["stage"])
        return rows

    def load_totals(self, batch_id: str) -> dict:
        rows = self._all("SELECT key, value FROM totals WHERE batch_id=?", (batch_id,))
        return {r["key"]: r["value"] for r in rows}

    def load_manifest(self, batch_id: str) -> dict | None:
        rows = self._all("SELECT json FROM manifests WHERE batch_id=?", (batch_id,))
        return json.loads(rows[0]["json"]) if rows else None

    def count_rows(self, batch_id: str) -> dict:
        out = {}
        for table in ("inputs", "events", "snapshots", "watermarks", "stages"):
            out[table] = self._all(
                f"SELECT COUNT(*) AS n FROM {table} WHERE batch_id=?", (batch_id,))[0]["n"]
        return out

    # ---------- M8 保留与回收 ----------
    def list_consumers(self, batch_id: str) -> list[dict]:
        return self._all(
            "SELECT * FROM consumers WHERE batch_id=? ORDER BY consumer_id", (batch_id,))

    def get_consumer(self, batch_id: str, consumer_id: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM consumers WHERE batch_id=? AND consumer_id=?",
            (batch_id, consumer_id))
        return rows[0] if rows else None

    def save_consumer(self, *, batch_id: str, consumer_id: str, label: str,
                      confirmed_seq: int, confirmed_at: str | None,
                      status: str = "active", note: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO consumers (batch_id,consumer_id,label,confirmed_seq,
                        confirmed_at,status,note)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id,consumer_id) DO UPDATE SET
                     label=excluded.label, confirmed_seq=excluded.confirmed_seq,
                     confirmed_at=excluded.confirmed_at, status=excluded.status,
                     note=excluded.note""",
                (batch_id, consumer_id, label, int(confirmed_seq or 0),
                 confirmed_at, status, note))

    def published_of(self, batch_id: str) -> dict:
        """发布位置：优先取批次落库值。

        老库该列为空时，从事件与归档的并集回推，保证回收之后回推值不回缩
        （热区被摘除的行仍在归档里，max 不变）。
        """
        batch = self.get_batch(batch_id) or {}
        seq = batch.get("published_seq")
        when = batch.get("published_event_time")
        if seq is None:
            row = self._all(
                "SELECT MAX(seq) AS seq FROM ("
                "  SELECT seq FROM events WHERE batch_id=? AND row_class='applied'"
                "  UNION ALL"
                "  SELECT seq FROM retention_archive WHERE batch_id=? AND row_class='applied'"
                ") AS pool", (batch_id, batch_id))[0]
            seq = row["seq"] or 0
        if when is None:
            row = self._all(
                "SELECT MAX(event_time_utc) AS t FROM ("
                "  SELECT event_time_utc FROM events WHERE batch_id=? AND row_class='applied'"
                "  UNION ALL"
                "  SELECT event_time_utc FROM retention_archive "
                "  WHERE batch_id=? AND row_class='applied'"
                ") AS pool", (batch_id, batch_id))[0]
            when = row["t"]
        return {"published_seq": int(seq or 0), "published_event_time": when}

    def find_retention_by_key(self, run_key: str) -> dict | None:
        """按保留键找可复用的运行（done / partial / running）。

        已回搬（`restored`）的运行不参与复用：用户显式恢复过，再跑应当生成新结果。
        """
        rows = self._all(
            "SELECT * FROM retention_runs WHERE run_key=? "
            "AND status IN ('done','partial','running') "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1", (run_key,))
        return rows[0] if rows else None

    def find_retention(self, retention_id: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM retention_runs WHERE retention_id=?", (retention_id,))
        return rows[0] if rows else None

    def list_retentions(self, batch_id: str, limit: int = 30,
                        status: str | None = None) -> list[dict]:
        if status:
            return self._all(
                "SELECT * FROM retention_runs WHERE batch_id=? AND status=? "
                "ORDER BY created_at DESC, rowid DESC LIMIT ?", (batch_id, status, limit))
        return self._all(
            "SELECT * FROM retention_runs WHERE batch_id=? "
            "ORDER BY created_at DESC, rowid DESC LIMIT ?", (batch_id, limit))

    def create_retention(self, row: dict) -> None:
        """一次保留尝试的开头：先落一条 running，后续按游标推进。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO retention_runs (retention_id,run_key,batch_id,policy,status,
                        published_seq,published_event_time,slowest_seq,slowest_consumer,
                        boundary_seq,boundary_event_time,ceiling_seq,blocked_json,
                        resume_json,counters_json,reconcile_json,params_json,cursor_seq,
                        attempts,created_at,updated_at,finished_at,note,parent_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (row["retention_id"], row["run_key"], row["batch_id"], row["policy"],
                 row["status"], row.get("published_seq") or 0,
                 row.get("published_event_time"), row.get("slowest_seq"),
                 row.get("slowest_consumer"), row.get("boundary_seq") or 0,
                 row.get("boundary_event_time"), row.get("ceiling_seq") or 0,
                 json.dumps(row.get("blocked", []), ensure_ascii=False),
                 json.dumps(row.get("resume", {}), ensure_ascii=False),
                 json.dumps(row.get("counters", {}), ensure_ascii=False),
                 json.dumps(row.get("reconcile", []), ensure_ascii=False),
                 json.dumps(row.get("params", {}), ensure_ascii=False),
                 int(row.get("cursor_seq") or 0), int(row.get("attempts") or 1),
                 row["created_at"], row.get("updated_at"), row.get("finished_at"),
                 row.get("note", ""), row.get("parent_id")))

    def update_retention(self, retention_id: str, **fields) -> None:
        if not fields:
            return
        allowed = {"status", "boundary_seq", "boundary_event_time", "ceiling_seq",
                   "published_seq", "published_event_time", "slowest_seq",
                   "slowest_consumer", "blocked_json", "resume_json", "counters_json",
                   "reconcile_json", "cursor_seq", "attempts", "updated_at",
                   "finished_at", "note", "parent_id"}
        sets = []
        args = []
        for key, value in fields.items():
            if key not in allowed:
                raise ValueError(f"非法保留字段 {key}")
            sets.append(f"{key}=?")
            args.append(value)
        args.append(retention_id)
        with self.connect() as conn:
            conn.execute(
                f"UPDATE retention_runs SET {', '.join(sets)} WHERE retention_id=?", args)

    # 仲裁结果整批写入，单事务；重试写同一 retention_id 时先删后插，保证不重复计数
    def save_retention_items(self, retention_id: str, items: list[dict]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM retention_items WHERE retention_id=?",
                         (retention_id,))
            for item in items:
                conn.execute(
                    """INSERT OR REPLACE INTO retention_items (retention_id,batch_id,seq,
                            row_ref,table_name,pk,version,op,event_time_utc,row_class,
                            decision,reason_code,location,detail)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (retention_id, item["batch_id"], item["seq"], item["row_ref"],
                     item["table_name"], item["pk"], item.get("version"), item.get("op"),
                     item.get("event_time_utc"), item["row_class"], item["decision"],
                     item["reason_code"], item.get("location", "hot"),
                     item.get("detail", "")))

    def load_retention_items(self, retention_id: str,
                             decision: str | None = None,
                             reason_code: str | None = None) -> list[dict]:
        sql = "SELECT * FROM retention_items WHERE retention_id=?"
        args: list = [retention_id]
        if decision:
            sql += " AND decision=?"
            args.append(decision)
        if reason_code:
            sql += " AND reason_code=?"
            args.append(reason_code)
        sql += " ORDER BY seq"
        return self._all(sql, tuple(args))

    def count_retention_items(self, retention_id: str) -> dict:
        rows = self._all(
            "SELECT decision, reason_code, COUNT(*) AS n FROM retention_items "
            "WHERE retention_id=? GROUP BY decision, reason_code", (retention_id,))
        out: dict = {}
        for row in rows:
            out.setdefault(row["decision"], {})[row["reason_code"]] = row["n"]
        return out

    def archive_event(self, *, batch_id: str, retention_id: str, row: dict,
                      archived_at: str) -> None:
        """把一条事件原样搬进归档表；已归档则不动（幂等）。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO retention_archive (batch_id,seq,retention_id,
                        row_ref,table_name,pk,version,op,event_time_utc,event_time_raw,
                        schema_version,payload_json,amount,qty,row_class,reason_code,
                        field,raw_value,detail,late,flags_json,raw_text,archived_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (batch_id, row["seq"], retention_id, row["row_ref"], row["table_name"],
                 row["pk"], row.get("version"), row.get("op"), row.get("event_time_utc"),
                 row.get("event_time_raw"), row.get("schema_version"),
                 json.dumps(row.get("payload", {}), ensure_ascii=False, default=str),
                 None if row.get("amount") is None else str(row["amount"]),
                 row.get("qty"), row["row_class"], row.get("reason_code"),
                 row.get("field"), row.get("raw_value"), row.get("detail"),
                 1 if row.get("late") else 0,
                 json.dumps(row.get("flags", []), ensure_ascii=False),
                 row.get("raw_text", ""), archived_at))

    def load_archive(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM retention_archive WHERE batch_id=? ORDER BY seq", (batch_id,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json") or "{}")
            r["flags"] = json.loads(r.pop("flags_json") or "[]")
            r["amount"] = None if r["amount"] is None else Decimal(r["amount"])
            r["late"] = bool(r["late"])
        return rows

    def archive_seqs(self, batch_id: str) -> set[int]:
        return {r["seq"] for r in self._all(
            "SELECT seq FROM retention_archive WHERE batch_id=?", (batch_id,))}

    def hot_events(self, batch_id: str) -> list[dict]:
        """热区事件：尚未归档的行。回收只从热区摘除，归档表永远留在库里。"""
        return self.load_events(batch_id, include_archived=False)

    def delete_events_by_seq(self, batch_id: str, seqs: list[int]) -> int:
        if not seqs:
            return 0
        placeholders = ",".join("?" for _ in seqs)
        with self.connect() as conn:
            cur = conn.execute(
                f"DELETE FROM events WHERE batch_id=? AND seq IN ({placeholders})",
                (batch_id, *seqs))
            return cur.rowcount

    def restore_archive(self, batch_id: str, retention_id: str | None = None) -> int:
        """把归档搬回热区（用于回滚/补数校验）；已存在的行不覆盖。"""
        sql = "SELECT * FROM retention_archive WHERE batch_id=?"
        args: list = [batch_id]
        if retention_id:
            sql += " AND retention_id=?"
            args.append(retention_id)
        archived = self._all(sql, tuple(args))
        restored = 0
        with self.connect() as conn:
            for row in archived:
                exists = conn.execute(
                    "SELECT 1 FROM events WHERE batch_id=? AND seq=?",
                    (batch_id, row["seq"])).fetchone()
                if exists:
                    continue
                conn.execute(
                    """INSERT INTO events (batch_id,row_ref,seq,table_name,pk,version,op,
                            event_time_utc,event_time_raw,schema_version,payload_json,
                            amount,qty,row_class,reason_code,field,raw_value,detail,late,
                            flags_json,raw_text)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["row_ref"], row["seq"], row["table_name"], row["pk"],
                     row["version"], row["op"], row["event_time_utc"],
                     row["event_time_raw"], row["schema_version"], row["payload_json"],
                     row["amount"], row["qty"], row["row_class"], row["reason_code"],
                     row["field"], row["raw_value"], row["detail"], row["late"],
                     row["flags_json"], row["raw_text"]))
                restored += 1
        return restored

    def delete_archive(self, batch_id: str, seqs: list[int] | None = None) -> int:
        with self.connect() as conn:
            if seqs is None:
                cur = conn.execute("DELETE FROM retention_archive WHERE batch_id=?",
                                   (batch_id,))
            else:
                if not seqs:
                    return 0
                placeholders = ",".join("?" for _ in seqs)
                cur = conn.execute(
                    f"DELETE FROM retention_archive WHERE batch_id=? AND seq IN "
                    f"({placeholders})", (batch_id, *seqs))
            return cur.rowcount

    def _all(self, sql: str, args: tuple) -> list[dict]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
