"""M8 入口：消费者确认、保留边界裁决、保留结果查询、清理与续跑、归档导出。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ... import errors
from ...web import Ctx, get_ctx, render
from . import boundary, service

router = APIRouter(tags=["M8 保留与回收"])


def _batch(ctx: Ctx, batch_id: str | None = None) -> dict:
    batch = ctx.current_batch(batch_id)
    if batch is None:
        raise errors.NotFound("还没有批次，先采信一次", code="E_NO_BATCH")
    return batch


def _retention_detail(store, run: dict) -> dict:
    items = store.load_retention_items(run["retention_id"])
    archived = store.archive_seqs(run["batch_id"])
    for item in items:
        item["location"] = "archive" if int(item["seq"]) in archived else "hot"
    return {
        "retention_id": run["retention_id"],
        "run_key": run["run_key"],
        "batch_id": run["batch_id"],
        "policy": run["policy"],
        "status": run["status"],
        "published_seq": run["published_seq"],
        "published_event_time": run.get("published_event_time"),
        "slowest_seq": run.get("slowest_seq"),
        "slowest_consumer": run.get("slowest_consumer"),
        "boundary_seq": run["boundary_seq"],
        "boundary_event_time": run.get("boundary_event_time"),
        "ceiling_seq": run.get("ceiling_seq"),
        "blocked": _json_list(run.get("blocked_json")),
        "resume": _json_obj(run.get("resume_json")),
        "counters": _json_obj(run.get("counters_json")),
        "reconcile": _json_list(run.get("reconcile_json")),
        "cursor_seq": run.get("cursor_seq") or 0,
        "attempts": run.get("attempts") or 1,
        "created_at": run.get("created_at"),
        "finished_at": run.get("finished_at"),
        "note": run.get("note", ""),
        "items": items,
        "archive_total": len(store.load_archive(run["batch_id"])),
    }


def _json_list(text) -> list:
    import json
    try:
        return json.loads(text or "[]")
    except ValueError:
        return []


def _json_obj(text) -> dict:
    import json
    try:
        return json.loads(text or "{}")
    except ValueError:
        return {}


@router.get("/api/retention")
def api_retention_overview(policy: str = service.DEFAULT_POLICY,
                           ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = _batch(ctx)
    preview = service.plan(store=ctx.store, batch_id=batch["batch_id"], policy=policy,
                           override=True)
    latest = ctx.store.list_retentions(batch["batch_id"], limit=1)
    return {
        "batch_id": batch["batch_id"],
        "consumers": service.consumer_rows(store=ctx.store, batch_id=batch["batch_id"]),
        "published_seq": preview["published_seq"],
        "published_event_time": preview["published_event_time"],
        "slowest_seq": preview["slowest_seq"],
        "slowest_consumer": preview["slowest_consumer"],
        "policies": [{"key": key, "label": label}
                     for key, label in boundary.POLICY_LABELS.items()],
        "preview": {k: preview[k] for k in (
            "policy", "policy_label", "code", "boundary_seq", "boundary_event_time",
            "ceiling_seq", "resume_seq", "blocked", "overridden", "counters",
            "reconcile", "note", "run_key")},
        "latest": _retention_detail(ctx.store, latest[0]) if latest else None,
        "hot_total": preview["hot_before"],
    }


@router.get("/api/retention/plan")
def api_retention_plan(policy: str = service.DEFAULT_POLICY, override: bool = False,
                       ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = _batch(ctx)
    verdict = service.plan(store=ctx.store, batch_id=batch["batch_id"],
                           policy=policy, override=override)
    return {k: verdict[k] for k in (
        "batch_id", "policy", "policy_label", "code", "published_seq",
        "published_event_time", "slowest_seq", "slowest_consumer", "boundary_seq",
        "boundary_event_time", "ceiling_seq", "blocked", "overridden", "counters",
        "reconcile", "resume_seq", "hot_before", "run_key", "note", "items")}


@router.get("/api/retention/consumers")
def api_consumers(ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = _batch(ctx)
    return {"batch_id": batch["batch_id"],
            "consumers": service.consumer_rows(store=ctx.store,
                                               batch_id=batch["batch_id"])}


@router.post("/api/retention/consumers")
def api_register_consumer(payload: dict | None = None,
                          ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = _batch(ctx)
    body = payload or {}
    consumer = service.register_consumer(
        store=ctx.store, batch_id=batch["batch_id"],
        consumer_id=str(body.get("consumer_id") or ""),
        label=str(body.get("label") or ""),
        confirmed_seq=body.get("confirmed_seq", 0))
    return {"batch_id": batch["batch_id"], "consumer": consumer}


@router.post("/api/retention/consumers/{consumer_id}/confirm")
def api_confirm_consumer(consumer_id: str, payload: dict | None = None,
                         ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = _batch(ctx)
    body = payload or {}
    consumer = service.confirm_consumer(
        store=ctx.store, batch_id=batch["batch_id"], consumer_id=consumer_id,
        confirmed_seq=body.get("confirmed_seq"))
    return {"batch_id": batch["batch_id"], "consumer": consumer}


@router.post("/api/retention")
def api_retention_run(payload: dict | None = None,
                      ctx: Ctx = Depends(get_ctx)) -> dict:
    batch = _batch(ctx)
    body = payload or {}
    policy = str(body.get("policy") or service.DEFAULT_POLICY)
    override = bool(body.get("override"))
    result = service.execute(store=ctx.store, batch_id=batch["batch_id"],
                             policy=policy, override=override)
    return result


@router.get("/api/retention/{retention_id}")
def api_retention_detail(retention_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    run = ctx.store.find_retention(retention_id)
    if run is None:
        raise errors.NotFound(f"保留运行 {retention_id} 不存在",
                              code="E_RETENTION_NOT_FOUND")
    return _retention_detail(ctx.store, run)


@router.get("/api/retention/{retention_id}/items")
def api_retention_items(retention_id: str, decision: str | None = None,
                        reason_code: str | None = None,
                        ctx: Ctx = Depends(get_ctx)) -> dict:
    run = ctx.store.find_retention(retention_id)
    if run is None:
        raise errors.NotFound(f"保留运行 {retention_id} 不存在",
                              code="E_RETENTION_NOT_FOUND")
    items = ctx.store.load_retention_items(retention_id, decision=decision,
                                           reason_code=reason_code)
    return {"retention_id": retention_id,
            "count": len(items),
            "counters": ctx.store.count_retention_items(retention_id),
            "items": items}


@router.post("/api/retention/{retention_id}/resume")
def api_retention_resume(retention_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    return service.resume_run(store=ctx.store, retention_id=retention_id)


@router.post("/api/retention/{retention_id}/restore")
def api_retention_restore(retention_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    run = ctx.store.find_retention(retention_id)
    if run is None:
        raise errors.NotFound(f"保留运行 {retention_id} 不存在",
                              code="E_RETENTION_NOT_FOUND")
    return service.restore(store=ctx.store, batch_id=run["batch_id"],
                           retention_id=retention_id)


@router.get("/retention")
def page_retention(request: Request, policy: str = service.DEFAULT_POLICY,
                   ctx: Ctx = Depends(get_ctx)):
    batch = ctx.current_batch()
    if batch is None:
        return render(request, "retention.html", batch=None, page="retention",
                      consumers=[], preview=None, items=[], latest=None,
                      runs=[], policies=boundary.POLICY_LABELS,
                      policy=policy, page_title="保留与回收")
    if policy not in boundary.POLICIES:
        policy = service.DEFAULT_POLICY
    # 页面是查看态：越权只做提示，不抛 409；真正清理走 POST /api/retention
    preview = service.plan(store=ctx.store, batch_id=batch["batch_id"],
                           policy=policy, override=True)
    latest_rows = ctx.store.list_retentions(batch["batch_id"], limit=10)
    latest = _retention_detail(ctx.store, latest_rows[0]) if latest_rows else None
    return render(request, "retention.html", batch=batch, page="retention",
                  consumers=service.consumer_rows(store=ctx.store,
                                                  batch_id=batch["batch_id"]),
                  preview=preview, items=preview["items"],
                  latest=latest, runs=[_retention_summary(r) for r in latest_rows],
                  policies=boundary.POLICY_LABELS, policy=policy,
                  page_title="保留与回收")


def _retention_summary(run: dict) -> dict:
    counters = _json_obj(run.get("counters_json"))
    return {
        "retention_id": run["retention_id"],
        "policy": run["policy"],
        "status": run["status"],
        "boundary_seq": run["boundary_seq"],
        "published_seq": run["published_seq"],
        "trim": counters.get("trim", 0),
        "retain": counters.get("retain", 0),
        "created_at": run.get("created_at"),
        "attempts": run.get("attempts") or 1,
    }
