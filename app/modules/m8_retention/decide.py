"""逐事件裁决：哪些进归档（trim）、哪些留在热区（retain）以及为什么。

判定顺序固定，与输入遍历顺序无关，任何一条命中即留：

1. 非已应用状态（冲突 / 隔离 / 重复）→ `R_RETAINED_EVIDENCE`，证据行永远不回收；
2. 落在待定窗口（`event_time > as_of`）→ `R_RETAINED_PENDING`，还没进快照；
3. 迟到 → `R_RETAINED_LATE`，补数窗口内保留待重放；
4. 到达序号在保留边界之上 → `R_RETAINED_ABOVE_BOUNDARY`；
5. 同键在边界之上还有版本 → `R_RETAINED_KEY_OPEN`，回收旧版本会让重放折叠失配；
6. 否则 → `R_TRIMMED_CONFIRMED`，可以归档回收。

注意第 5 条：一个键只要在边界之上还有任何版本，它的全部热区版本都保留，
保证「从归档回放 + 热区剩余」仍能折叠出与水印一致的键状态。
"""
from __future__ import annotations

from ... import reasons

TRIM = "trim"
RETAIN = "retain"

# 保留原因按「越靠前越优先」排列；页面按这个顺序解释挡住清理的原因
ORDER = (
    "R_RETAINED_EVIDENCE",
    "R_RETAINED_PENDING",
    "R_RETAINED_LATE",
    "R_RETAINED_ABOVE_BOUNDARY",
    "R_RETAINED_KEY_OPEN",
)


def _keys_above(events: list[dict], boundary_seq: int) -> set[tuple[str, str]]:
    return {(row["table_name"], row["pk"]) for row in events
            if int(row.get("seq") or 0) > boundary_seq}


def decide_event(*, row: dict, boundary_seq: int, as_of: str | None,
                 open_keys: set[tuple[str, str]]) -> dict:
    seq = int(row.get("seq") or 0)
    key = (row["table_name"], row["pk"])
    event_time = row.get("event_time_utc") or ""
    if row["row_class"] != "applied":
        return _retain(seq, "R_RETAINED_EVIDENCE",
                       f"{row['row_class']} 行保留作证据")
    if as_of and event_time > as_of:
        return _retain(seq, "R_RETAINED_PENDING", f"event_time {event_time} 晚于 as_of {as_of}")
    if row.get("late"):
        return _retain(seq, "R_RETAINED_LATE", "迟到事件保留在补数窗口")
    if seq > boundary_seq:
        return _retain(seq, "R_RETAINED_ABOVE_BOUNDARY",
                       f"到达序号 {seq} 在保留边界 {boundary_seq} 之上")
    if key in open_keys:
        return _retain(seq, "R_RETAINED_KEY_OPEN", "同键在边界之上仍有版本")
    return {"decision": TRIM, "reason_code": "R_TRIMMED_CONFIRMED",
            "detail": f"已被全部消费者确认至 {boundary_seq}"}


def _retain(seq: int, code: str, detail: str) -> dict:
    return {"decision": RETAIN, "reason_code": code,
            "detail": detail or reasons.label(code)}


def judge(*, events: list[dict], boundary_seq: int, as_of: str | None) -> list[dict]:
    """给每条热区事件一个裁决行；输入不做任何修改。"""
    open_keys = _keys_above(events, boundary_seq)
    out = []
    for row in sorted(events, key=lambda r: int(r.get("seq") or 0)):
        verdict = decide_event(row=row, boundary_seq=boundary_seq, as_of=as_of,
                               open_keys=open_keys)
        out.append({
            "batch_id": row["batch_id"], "seq": int(row["seq"]),
            "row_ref": row["row_ref"], "table_name": row["table_name"],
            "pk": row["pk"], "version": row.get("version"), "op": row.get("op"),
            "event_time_utc": row.get("event_time_utc"),
            "row_class": row["row_class"], **verdict,
        })
    return out


def tally(items: list[dict]) -> dict:
    """按 decision / reason_code 聚合；reconcile 与页面共用同一份数字。"""
    out = {TRIM: {}, RETAIN: {}}
    for item in items:
        bucket = out[item["decision"]]
        bucket[item["reason_code"]] = bucket.get(item["reason_code"], 0) + 1
    trimmed = sum(out[TRIM].values())
    retained = sum(out[RETAIN].values())
    return {"trim": trimmed, "retain": retained,
            "total": trimmed + retained, "by_reason": out}


def reconcile(*, items: list[dict], hot_before: int, boundary_seq: int,
              published_seq: int, events: list[dict]) -> list[dict]:
    """三条守恒：裁决账、边界账、证据账。任一不平显式写 false。"""
    counts = tally(items)
    trim_seqs = [int(i["seq"]) for i in items if i["decision"] == TRIM]
    retain_seqs = [int(i["seq"]) for i in items if i["decision"] == RETAIN]
    evidence = [i for i in items if i["reason_code"] == "R_RETAINED_EVIDENCE"]
    over_boundary = [s for s in trim_seqs if s > boundary_seq]
    return [
        {"name": "裁决账", "kind": "count", "left": "热区事件",
         "left_value": hot_before,
         "right": "回收 + 保留", "right_value": counts["trim"] + counts["retain"],
         "ok": hot_before == counts["trim"] + counts["retain"] and
               len(set(trim_seqs) | set(retain_seqs)) == len(items)},
        {"name": "边界账", "kind": "count", "left": "回收条数",
         "left_value": counts["trim"],
         "right": "边界内回收", "right_value": len(trim_seqs) - len(over_boundary),
         "ok": not over_boundary and boundary_seq <= published_seq,
         "note": f"边界 {boundary_seq} ≤ 发布 {published_seq}"},
        {"name": "证据账", "kind": "count", "left": "非已应用事件",
         "left_value": sum(1 for e in events if e["row_class"] != "applied"),
         "right": "证据保留", "right_value": len(evidence),
         "ok": sum(1 for e in events if e["row_class"] != "applied") == len(evidence)},
    ]
