"""保留运行编排：判定 → 逐条裁决 → 归档回收 → 落库，可从游标续跑且幂等。

一次运行的所有写入都挂在同一个 `retention_id` 下：

- `run_key = (batch_id, policy, published_seq, boundary_seq, 边界消费者确认快照)`，
  同一键重试命中已完成 / 部分完成的运行，直接复用，不重复计数、不新增裁决行；
- 清理按游标推进：每处理一批就归档、从热区摘除、推进 `cursor_seq`，任何一步失败
  都停在当前游标，续跑从该游标继续，已归档的事件仍在归档表里，不会重复归档；
- 旧版本与待重放事件由 `decide` 判为 retain，不归档、不摘除；
- 历史事件的证据行（冲突 / 隔离 / 重复）永远 retain，保证记录不丢。
"""
from __future__ import annotations

import datetime as dt
import json
import uuid

from ... import config, errors, manifest as manifest_mod, reasons
from . import boundary, decide

DEFAULT_POLICY = "safe"
BATCH_SIZE = 1000


def _now_iso() -> str:
    return dt.datetime.now(config.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------- 消费者登记与确认（单调推进） ----------
def register_consumer(*, store, batch_id: str, consumer_id: str, label: str = "",
                      confirmed_seq: int = 0) -> dict:
    consumer_id = str(consumer_id or "").strip()
    if not consumer_id:
        raise errors.BadRequest("消费者标识不能为空", code="E_BAD_CONFIRMATION")
    seq = _as_seq(confirmed_seq)
    published = store.published_of(batch_id)["published_seq"]
    if seq > published:
        raise errors.BadRequest(
            f"确认位置 {seq} 超过当前发布位置 {published}",
            code="E_CONSUMER_AHEAD",
            detail={"confirmed_seq": seq, "published_seq": published})
    existing = store.get_consumer(batch_id, consumer_id)
    if existing is not None and seq < int(existing["confirmed_seq"] or 0):
        raise errors.Conflict(
            f"消费者 {consumer_id} 确认位置不能从 {existing['confirmed_seq']} 回退到 {seq}",
            code="C_CONSUMER_REGRESS")
    store.save_consumer(batch_id=batch_id, consumer_id=consumer_id,
                        label=label or (existing or {}).get("label", consumer_id),
                        confirmed_seq=seq, confirmed_at=_now_iso(),
                        status="active", note="")
    return store.get_consumer(batch_id, consumer_id) or {}


def confirm_consumer(*, store, batch_id: str, consumer_id: str,
                     confirmed_seq: int) -> dict:
    consumer = store.get_consumer(batch_id, consumer_id)
    if consumer is None:
        raise errors.NotFound(f"消费者 {consumer_id} 未登记",
                              code="E_CONSUMER_MISSING")
    seq = _as_seq(confirmed_seq)
    current = int(consumer["confirmed_seq"] or 0)
    if seq < current:
        raise errors.Conflict(
            f"消费者 {consumer_id} 确认位置不能从 {current} 回退到 {seq}",
            code="C_CONSUMER_REGRESS",
            detail={"current": current, "incoming": seq})
    published = store.published_of(batch_id)["published_seq"]
    if seq > published:
        raise errors.BadRequest(
            f"确认位置 {seq} 超过当前发布位置 {published}",
            code="E_CONSUMER_AHEAD",
            detail={"confirmed_seq": seq, "published_seq": published})
    store.save_consumer(batch_id=batch_id, consumer_id=consumer_id,
                        label=consumer.get("label", consumer_id), confirmed_seq=seq,
                        confirmed_at=_now_iso(), status="active",
                        note=consumer.get("note", ""))
    return store.get_consumer(batch_id, consumer_id) or {}


def _as_seq(value) -> int:
    if isinstance(value, bool):
        raise errors.BadRequest("确认位置必须是非负整数", code="E_BAD_CONFIRMATION")
    try:
        seq = int(value)
    except (TypeError, ValueError):
        raise errors.BadRequest("确认位置必须是非负整数", code="E_BAD_CONFIRMATION")
    if seq < 0:
        raise errors.BadRequest("确认位置必须是非负整数", code="E_BAD_CONFIRMATION")
    return seq


def consumer_rows(*, store, batch_id: str) -> list[dict]:
    """消费者视图：确认位置、落后发布多少、是否挡住清理、会不会被越过。"""
    published = store.published_of(batch_id)
    consumers = store.list_consumers(batch_id)
    out = []
    for consumer in consumers:
        confirmed = int(consumer.get("confirmed_seq") or 0)
        behind = max(0, published["published_seq"] - confirmed)
        out.append({
            "consumer_id": consumer["consumer_id"],
            "label": consumer.get("label", ""),
            "confirmed_seq": confirmed,
            "confirmed_at": consumer.get("confirmed_at"),
            "status": consumer.get("status", "active"),
            "behind": behind,
            "is_slowest": False,
            "blocks": behind > 0,
            "blocking_from_seq": confirmed + 1 if behind else None,
        })
    active = [c for c in out if c["status"] == "active"]
    if active:
        slow = min(active, key=lambda c: (c["confirmed_seq"], c["consumer_id"]))
        for c in out:
            c["is_slowest"] = c["consumer_id"] == slow["consumer_id"]
    return out


def _new_id() -> str:
    stamp = dt.datetime.now(config.UTC).strftime("%Y%m%d%H%M%S")
    return f"RET-{stamp}-{uuid.uuid4().hex[:6]}"


def run_key(*, batch_id: str, policy: str, published_seq: int, boundary_seq: int,
            consumers: list[dict]) -> str:
    """同一输入 + 同一边界 + 同一确认快照 → 同一保留键。"""
    snapshot = sorted((c["consumer_id"], int(c.get("confirmed_seq") or 0))
                      for c in consumers if (c.get("status") or "active") == "active")
    body = manifest_mod.digest_parts([
        batch_id, policy, str(published_seq), str(boundary_seq),
        json.dumps(snapshot, ensure_ascii=False),
    ])
    return f"RK-{body[:16]}"


def _as_of(store, batch_id: str) -> str | None:
    batch = store.get_batch(batch_id) or {}
    return batch.get("watermark_utc")


def plan(*, store, batch_id: str, policy: str = DEFAULT_POLICY,
         override: bool = False) -> dict:
    """只算不写：边界、挡住清理的消费者、逐条裁决与三条守恒。"""
    batch = store.get_batch(batch_id)
    if batch is None:
        raise errors.NotFound(f"批次 {batch_id} 不存在", code="E_RETENTION_NOT_FOUND")
    published = store.published_of(batch_id)
    consumers = store.list_consumers(batch_id)
    verdict = boundary.decide(
        policy=policy, published_seq=published["published_seq"],
        published_event_time=published["published_event_time"],
        consumers=consumers, override=override)
    verdict["boundary_event_time"] = boundary.boundary_event_time(
        store.load_events(batch_id), verdict["boundary_seq"])
    events = store.hot_events(batch_id)
    items = decide.judge(events=events, boundary_seq=verdict["boundary_seq"],
                         as_of=_as_of(store, batch_id))
    verdict["items"] = items
    verdict["counters"] = decide.tally(items)
    verdict["reconcile"] = decide.reconcile(
        items=items, hot_before=len(events), boundary_seq=verdict["boundary_seq"],
        published_seq=verdict["published_seq"], events=events)
    verdict["hot_before"] = len(events)
    verdict["run_key"] = run_key(
        batch_id=batch_id, policy=policy, published_seq=verdict["published_seq"],
        boundary_seq=verdict["boundary_seq"], consumers=consumers)
    verdict["consumers"] = consumers
    verdict["batch_id"] = batch_id
    return verdict


def _existing_payload(store, run: dict, *, replayed: bool) -> dict:
    items = store.load_retention_items(run["retention_id"])
    archived = store.archive_seqs(run["batch_id"])
    # 位置反映当前真实状态：已归档的事件即使裁决为 trim 也标 archive，
    # 便于页面/导出区分「回收了」与「还留在热区」。
    for item in items:
        item["location"] = "archive" if int(item["seq"]) in archived else "hot"
    counters = json.loads(run.get("counters_json") or "{}")
    archive = store.load_archive(run["batch_id"])
    resume = json.loads(run.get("resume_json") or "{}")
    return {
        "status": run["status"],
        "retention_id": run["retention_id"],
        "run_key": run["run_key"],
        "batch_id": run["batch_id"],
        "policy": run["policy"],
        "policy_label": boundary.POLICY_LABELS.get(run["policy"], run["policy"]),
        "idempotent_replay": replayed,
        "code": "R_RETENTION_REPLAY" if replayed else resume.get("boundary_code"),
        "note": reasons.label("R_RETENTION_REPLAY") if replayed else run.get("note", ""),
        "published_seq": run["published_seq"],
        "published_event_time": run.get("published_event_time"),
        "slowest_seq": run.get("slowest_seq"),
        "slowest_consumer": run.get("slowest_consumer"),
        "boundary_seq": run["boundary_seq"],
        "boundary_event_time": run.get("boundary_event_time"),
        "ceiling_seq": run.get("ceiling_seq"),
        "blocked": json.loads(run.get("blocked_json") or "[]"),
        "overridden": resume.get("overridden", []),
        "resume": resume,
        "counters": counters,
        "reconcile": json.loads(run.get("reconcile_json") or "[]"),
        "items": items,
        "archive_total": len(archive),
        "cursor_seq": run.get("cursor_seq") or 0,
        "attempts": run.get("attempts") or 1,
        "created_at": run.get("created_at"),
        "finished_at": run.get("finished_at"),
    }


def execute(*, store, batch_id: str, policy: str = DEFAULT_POLICY,
            override: bool = False, parent_id: str | None = None) -> dict:
    """跑一次保留回收。命中同键已完成 / 部分完成运行直接复用（幂等）。"""
    verdict = plan(store=store, batch_id=batch_id, policy=policy, override=override)
    existing = store.find_retention_by_key(verdict["run_key"])
    if existing is not None:
        if existing["status"] == "done":
            return _existing_payload(store, existing, replayed=True)
        if existing["status"] in ("partial", "running"):
            # 上一次清理中断（异常或进程被杀）：同键重试直接续跑，
            # 不新建裁决、不重复计数；running 行按游标正常续完。
            resumed = resume_run(store=store, retention_id=existing["retention_id"])
            resumed["idempotent_replay"] = True
            resumed["code"] = "R_RESUMED"
            return resumed

    items = verdict["items"]
    counters = verdict["counters"]
    blocked = verdict["blocked"]
    resume = {"from_seq": verdict["resume_seq"],
              "boundary_seq": verdict["boundary_seq"],
              "above_boundary": counters["by_reason"]["retain"].get(
                  "R_RETAINED_ABOVE_BOUNDARY", 0),
              "blocked_consumers": [c["consumer_id"] for c in blocked],
              "overridden": verdict["overridden"],
              "boundary_code": verdict["code"],
              "override": bool(override)}
    now = _now_iso()
    retention_id = _new_id()
    store.create_retention({
        "retention_id": retention_id, "run_key": verdict["run_key"],
        "batch_id": batch_id, "policy": policy, "status": "running",
        "published_seq": verdict["published_seq"],
        "published_event_time": verdict["published_event_time"],
        "slowest_seq": verdict["slowest_seq"],
        "slowest_consumer": verdict["slowest_consumer"],
        "boundary_seq": verdict["boundary_seq"],
        "boundary_event_time": verdict["boundary_event_time"],
        "ceiling_seq": verdict["ceiling_seq"], "blocked": blocked,
        "resume": resume, "counters": counters, "reconcile": verdict["reconcile"],
        "params": {"policy": policy, "override": bool(override)},
        "cursor_seq": 0, "attempts": 1, "created_at": now, "updated_at": now,
        "note": verdict["note"], "parent_id": parent_id,
    })
    store.save_retention_items(retention_id, items)

    # ---- 归档回收：按序号分批推进游标；失败停在当前位置，可续跑 ----
    trim_items = [item for item in items if item["decision"] == decide.TRIM]
    by_seq = {int(row["seq"]): row for row in store.load_events(batch_id)}
    cursor = 0
    archived = 0
    trimmed = 0
    try:
        for start in range(0, len(trim_items), BATCH_SIZE):
            chunk = trim_items[start:start + BATCH_SIZE]
            seqs = []
            for item in chunk:
                row = by_seq.get(int(item["seq"]))
                if row is None:
                    continue
                store.archive_event(batch_id=batch_id, retention_id=retention_id,
                                    row=row, archived_at=_now_iso())
                seqs.append(int(item["seq"]))
                archived += 1
            trimmed += store.delete_events_by_seq(batch_id, seqs)
            cursor = max((int(i["seq"]) for i in chunk), default=cursor)
            store.update_retention(retention_id, cursor_seq=cursor,
                                   updated_at=_now_iso())
    except Exception as exc:  # noqa: BLE001 - 中断也要留下可续跑的状态
        store.update_retention(
            retention_id, status="partial", cursor_seq=cursor,
            updated_at=_now_iso(),
            note=f"在游标 {cursor} 中断：{exc}")
        payload = _existing_payload(store, store.find_retention(retention_id),
                                    replayed=False)
        payload.update({"status": "partial", "code": "R_RETENTION_PARTIAL",
                        "note": reasons.label("R_RETENTION_PARTIAL"),
                        "error": str(exc), "archive_total": len(
                            store.load_archive(batch_id))})
        return payload

    finished = _now_iso()
    store.update_retention(
        retention_id, status="done", cursor_seq=cursor,
        updated_at=finished, finished_at=finished,
        counters_json=json.dumps(counters, ensure_ascii=False))
    run = store.find_retention(retention_id)
    payload = _existing_payload(store, run, replayed=False)
    payload["archived"] = archived
    payload["trimmed"] = trimmed
    return payload


def resume_run(*, store, retention_id: str) -> dict:
    """从断点续跑：同一 retention_id 继续，已归档的不再重复处理。"""
    run = store.find_retention(retention_id)
    if run is None:
        raise errors.NotFound(f"保留运行 {retention_id} 不存在",
                              code="E_RETENTION_NOT_FOUND")
    if run["status"] == "done":
        payload = _existing_payload(store, run, replayed=True)
        payload["code"] = "R_RETENTION_REPLAY"
        return payload
    # 续跑沿用原边界与裁决，只把游标之后尚未归档的 trim 项补完
    items = store.load_retention_items(retention_id)
    batch_id = run["batch_id"]
    trim_items = [i for i in items
                  if i["decision"] == decide.TRIM and int(i["seq"]) > int(run["cursor_seq"] or 0)]
    by_seq = {int(row["seq"]): row for row in store.load_events(batch_id)}
    cursor = int(run["cursor_seq"] or 0)
    archived = 0
    trimmed = 0
    store.update_retention(retention_id, status="running",
                           attempts=int(run["attempts"] or 1) + 1,
                           updated_at=_now_iso(), note="续跑")
    try:
        for item in trim_items:
            row = by_seq.get(int(item["seq"]))
            if row is None:
                continue
            store.archive_event(batch_id=batch_id, retention_id=retention_id,
                                row=row, archived_at=_now_iso())
            archived += 1
            trimmed += store.delete_events_by_seq(batch_id, [int(item["seq"])])
            cursor = max(cursor, int(item["seq"]))
            store.update_retention(retention_id, cursor_seq=cursor,
                                   updated_at=_now_iso())
    except Exception as exc:  # noqa: BLE001
        store.update_retention(retention_id, status="partial", cursor_seq=cursor,
                               updated_at=_now_iso(), note=f"续跑中断：{exc}")
        payload = _existing_payload(store, store.find_retention(retention_id),
                                    replayed=False)
        payload.update({"status": "partial", "code": "R_RETENTION_PARTIAL",
                        "error": str(exc)})
        return payload
    finished = _now_iso()
    store.update_retention(retention_id, status="done", cursor_seq=cursor,
                           updated_at=finished, finished_at=finished)
    payload = _existing_payload(store, store.find_retention(retention_id),
                                replayed=False)
    payload["archived"] = archived
    payload["trimmed"] = trimmed
    return payload


def restore(*, store, batch_id: str, retention_id: str | None = None) -> dict:
    """把归档搬回热区，用于核对或回滚；不删除归档、不改裁决记录。

    回搬之后该运行标记为 `restored`：再次按同一保留键清理会生成新结果，
    而不是复用「已经回搬过」的旧裁决。
    """
    restored = store.restore_archive(batch_id, retention_id)
    if retention_id and restored:
        store.update_retention(retention_id, status="restored",
                               updated_at=_now_iso(), note="归档回搬热区")
    return {"batch_id": batch_id, "retention_id": retention_id,
            "restored": restored, "archive_total": len(store.load_archive(batch_id))}
