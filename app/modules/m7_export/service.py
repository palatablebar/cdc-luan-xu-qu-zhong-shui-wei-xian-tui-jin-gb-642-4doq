"""导出编排：把一次运行的证据行、隔离、冲突、快照、保留结果与清单拼成可下载文件。"""
from __future__ import annotations

from ... import errors, reasons
from ..m5_snapshot import service as m5_service
from ..m8_retention import service as retention_service
from . import csv_out, json_out, manifest_out

DECISION_LABELS = {"trim": "回收", "retain": "保留"}

DATASETS = {
    "events": {"label": "事件证据", "filename": "events.csv",
               "columns": csv_out.EVENT_COLUMNS},
    "quarantine": {"label": "隔离区", "filename": "quarantine.csv",
                   "columns": csv_out.QUARANTINE_COLUMNS},
    "conflicts": {"label": "冲突表", "filename": "conflicts.csv",
                  "columns": csv_out.CONFLICT_COLUMNS},
    "snapshot": {"label": "快照", "filename": "snapshot.csv",
                 "columns": csv_out.SNAPSHOT_COLUMNS},
    "retention": {"label": "保留结果", "filename": "retention.csv",
                  "columns": csv_out.RETENTION_COLUMNS},
    "manifest": {"label": "运行清单", "filename": "manifest.json", "columns": ()},
    "snapshot_json": {"label": "快照 JSON", "filename": "snapshot.json",
                      "columns": ()},
    "retention_json": {"label": "保留结果 JSON", "filename": "retention.json",
                       "columns": ()},
}


def _batch(store, batch_id: str | None) -> dict:
    batch = store.get_batch(batch_id) if batch_id else store.latest_batch()
    if batch is None:
        raise errors.NotFound("还没有批次，先采信一次", code="E_NO_BATCH")
    return batch


def rows_for(store, batch_id: str, kind: str) -> list[list]:
    rows = store.load_events(batch_id)
    if kind == "events":
        return csv_out.event_rows(rows)
    if kind == "quarantine":
        return csv_out.quarantine_rows([r for r in rows if r["row_class"] == "quarantine"])
    if kind == "conflicts":
        return csv_out.conflict_rows([r for r in rows if r["row_class"] == "conflict"])
    if kind == "snapshot":
        return csv_out.snapshot_rows(store.load_snapshot(batch_id))
    raise errors.BadRequest(f"未知导出集 {kind!r}", code="E_BAD_DATASET")


def render_csv(store, batch_id: str, kind: str) -> tuple[bytes, str]:
    meta = DATASETS.get(kind)
    if meta is None or not meta["columns"]:
        raise errors.BadRequest(f"未知导出集 {kind!r}", code="E_BAD_DATASET")
    rows = rows_for(store, batch_id, kind)
    text = csv_out.to_csv(meta["columns"], rows)
    return csv_out.encode_csv(text), f"{batch_id}-{meta['filename']}"


def render_manifest(store, batch_id: str) -> tuple[bytes, str]:
    payload = manifest_out.build(store, batch_id)
    text = json_out.dumps(payload)
    return text.encode("utf-8"), f"{batch_id}-manifest.json"


def render_snapshot_json(store, batch_id: str) -> tuple[bytes, str]:
    batch = store.get_batch(batch_id) or {}
    summary = m5_service.summarize(store, batch_id)
    rows = m5_service.page_rows(store, batch_id)
    pending = m5_service.pending_rows(store, batch_id)
    reconcile = [_plain(r) for r in m5_service.reconcile_rows(store, batch_id)]
    payload = json_out.snapshot_document(batch=batch, summary=summary, rows=rows,
                                         pending=pending, reconcile=reconcile)
    return json_out.dumps(payload).encode("utf-8"), f"{batch_id}-snapshot.json"


# ---------- 保留结果导出：与页面同一份裁决，可按 retention_id 或取最近一次 ----------
def retention_run(store, batch_id: str, retention_id: str | None = None) -> dict:
    run = (store.find_retention(retention_id) if retention_id
           else (store.list_retentions(batch_id, limit=1) or [None])[0])
    if run is None:
        raise errors.NotFound("还没有保留运行，先跑一次保留判定",
                              code="E_RETENTION_NOT_FOUND")
    if run["batch_id"] != batch_id:
        raise errors.NotFound(f"保留运行 {run['retention_id']} 不属于批次 {batch_id}",
                              code="E_RETENTION_NOT_FOUND")
    return run


def retention_payload(store, run: dict) -> dict:
    """把落库的保留运行还原成可导出的完整结果（页面 / CSV / JSON 共用）。"""
    import json

    items = store.load_retention_items(run["retention_id"])
    archived = store.archive_seqs(run["batch_id"])
    for item in items:
        item["location"] = "archive" if int(item["seq"]) in archived else "hot"
    counters = json.loads(run.get("counters_json") or "{}")
    return {
        "retention_id": run["retention_id"],
        "run_key": run["run_key"],
        "batch_id": run["batch_id"],
        "policy": run["policy"],
        "policy_label": retention_service.boundary.POLICY_LABELS.get(
            run["policy"], run["policy"]),
        "status": run["status"],
        "published_seq": run["published_seq"],
        "published_event_time": run.get("published_event_time"),
        "slowest_seq": run.get("slowest_seq"),
        "slowest_consumer": run.get("slowest_consumer"),
        "boundary_seq": run["boundary_seq"],
        "boundary_event_time": run.get("boundary_event_time"),
        "ceiling_seq": run.get("ceiling_seq"),
        "resume": json.loads(run.get("resume_json") or "{}"),
        "blocked": json.loads(run.get("blocked_json") or "[]"),
        "counters": counters,
        "reconcile": json.loads(run.get("reconcile_json") or "[]"),
        "items": items,
    }


def render_retention_csv(store, batch_id: str,
                         retention_id: str | None = None) -> tuple[bytes, str]:
    run = retention_run(store, batch_id, retention_id)
    payload = retention_payload(store, run)
    text = csv_out.to_csv(csv_out.RETENTION_COLUMNS,
                          csv_out.retention_rows(payload["items"], DECISION_LABELS))
    return csv_out.encode_csv(text), f"{run['retention_id']}-retention.csv"


def render_retention_json(store, batch_id: str,
                          retention_id: str | None = None) -> tuple[bytes, str]:
    run = retention_run(store, batch_id, retention_id)
    payload = retention_payload(store, run)
    doc = json_out.retention_document(run=payload, items=payload["items"],
                                      decision_labels=DECISION_LABELS)
    return json_out.dumps(doc).encode("utf-8"), f"{run['retention_id']}-retention.json"


def catalog(store, batch_id: str | None = None) -> list[dict]:
    batch = _batch(store, batch_id)
    rows = store.load_events(batch["batch_id"])
    latest_retention = (store.list_retentions(batch["batch_id"], limit=1) or [None])[0]
    counts = {
        "events": len(rows),
        "quarantine": sum(1 for r in rows if r["row_class"] == "quarantine"),
        "conflicts": sum(1 for r in rows if r["row_class"] == "conflict"),
        "snapshot": len(store.load_snapshot(batch["batch_id"])),
        "retention": (len(store.load_retention_items(latest_retention["retention_id"]))
                      if latest_retention else 0),
        "manifest": 1,
        "snapshot_json": 1,
        "retention_json": 1 if latest_retention else 0,
    }
    out = []
    for key, meta in DATASETS.items():
        out.append({"kind": key, "label": meta["label"],
                    "filename": f"{batch['batch_id']}-{meta['filename']}",
                    "rows": counts.get(key, 0),
                    "columns": len(meta["columns"])})
    return out


def summaries(store, batch_id: str) -> dict:
    batch = _batch(store, batch_id)
    totals = store.load_totals(batch["batch_id"])
    return {
        "batch_id": batch["batch_id"],
        "run_id": batch["run_id"],
        "input_digest": batch["input_digest"],
        "watermark_utc": batch.get("watermark_utc"),
        "totals": {k: v for k, v in totals.items() if k != "reconcile_json"},
        "datasets": catalog(store, batch_id),
        "empty_hint": reasons.label("X_EMPTY_SELECTION")
        if not store.load_events(batch["batch_id"]) else None,
    }


def _plain(row: dict) -> dict:
    from decimal import Decimal

    return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in row.items()}
