"""JSON 导出：Decimal 与大整数安全序列化。

规则：

- Decimal → 字符串（保两位，不带科学计数法）；
- 绝对值超过 2^53 的整数 → 字符串（JS 侧不会掉精度，TRAP-09）；
- 时刻 → ISO8601 UTC 字符串；
- 键顺序固定，同一输入重复导出字节一致。
"""
from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

JS_SAFE_INT = 2 ** 53


def _convert(value):
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, dict):
        return {str(k): _convert(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_convert(v) for v in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and abs(value) > JS_SAFE_INT:
        return str(value)
    return value


def dumps(payload: dict) -> str:
    return json.dumps(_convert(payload), ensure_ascii=False, sort_keys=False,
                      indent=2, separators=(",", ": "))


def snapshot_document(*, batch: dict, summary: dict, rows: list[dict],
                      pending: list[dict], reconcile: list[dict]) -> dict:
    return {
        "batch_id": batch.get("batch_id"),
        "run_id": batch.get("run_id"),
        "as_of_utc": summary.get("as_of"),
        "watermark_utc": batch.get("watermark_utc"),
        "counters": {
            "keys_total": summary.get("keys_total"),
            "snapshot_keys": summary.get("snapshot_keys"),
            "active_keys": summary.get("active_keys"),
            "tombstone_keys": summary.get("tombstone_keys"),
            "pending_only_keys": summary.get("pending_only"),
        },
        "amounts": {
            "snapshot": str(summary.get("amount_snapshot") or "0.00"),
            "pending": str(summary.get("amount_pending") or "0.00"),
            "applied": str(summary.get("amount_applied") or "0.00"),
        },
        "reconcile": reconcile,
        "rows": [_row(row) for row in rows],
        "pending": [_row(row) for row in pending],
    }


def _row(row: dict) -> dict:
    out = {
        "table": row.get("table") or row.get("table_name"),
        "pk": str(row.get("pk")),
        "version": row.get("version"),
        "op": row.get("op"),
        "kind": row.get("kind") or ("墓碑" if row.get("op") == "delete" else "活跃"),
        "amount": None if row.get("amount") is None else f"{Decimal(str(row['amount'])):.2f}",
        "event_time_utc": row.get("event_time") or row.get("event_time_utc"),
        "row_ref": row.get("row_ref"),
        "late": bool(row.get("late")),
        "pending": bool(row.get("pending")),
    }
    return out


def retention_document(*, run: dict, items: list[dict],
                       decision_labels: dict) -> dict:
    """保留结果的独立复核文件：边界裁决 + 消费者 + 逐条裁决 + 三条守恒。

    与页面同一份数字，导出字节对同一 retention_id 稳定（键顺序固定）。
    """
    counters = run.get("counters") or {}
    by_reason = counters.get("by_reason") or {}
    return {
        "retention_id": run.get("retention_id"),
        "run_key": run.get("run_key"),
        "batch_id": run.get("batch_id"),
        "policy": run.get("policy"),
        "policy_label": run.get("policy_label"),
        "status": run.get("status"),
        "published_seq": run.get("published_seq"),
        "published_event_time": run.get("published_event_time"),
        "slowest_seq": run.get("slowest_seq"),
        "slowest_consumer": run.get("slowest_consumer"),
        "boundary_seq": run.get("boundary_seq"),
        "boundary_event_time": run.get("boundary_event_time"),
        "ceiling_seq": run.get("ceiling_seq"),
        "resume": run.get("resume") or {},
        "blocked": run.get("blocked") or [],
        "overridden": run.get("overridden") or [],
        "counters": {
            "trim": counters.get("trim", 0),
            "retain": counters.get("retain", 0),
            "total": counters.get("total", 0),
            "by_reason": by_reason,
        },
        "reconcile": run.get("reconcile") or [],
        "decision_labels": decision_labels,
        "items": [
            {
                "seq": item["seq"],
                "row_ref": item["row_ref"],
                "table": item["table_name"],
                "pk": str(item["pk"]),
                "version": item.get("version"),
                "op": item.get("op"),
                "event_time_utc": item.get("event_time_utc"),
                "row_class": item.get("row_class"),
                "decision": item["decision"],
                "decision_label": decision_labels.get(item["decision"],
                                                      item["decision"]),
                "reason_code": item.get("reason_code"),
                "location": item.get("location") or "hot",
                "detail": item.get("detail") or "",
            }
            for item in items
        ],
    }
