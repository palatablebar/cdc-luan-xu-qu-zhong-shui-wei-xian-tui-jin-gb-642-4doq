"""M8 保留与回收：边界裁决、消费者确认、逐条裁决、清理、续跑、幂等与导出。

样例批次的关键数字（docs/DEPTH.md 四账）：发布位置 = 已应用事件最大到达序号，
边界默认取最慢确认位置；证据行（冲突 / 隔离 / 重复）永远保留。
"""
from __future__ import annotations

import json

import pytest

from app import errors
from app.modules.m3_dedup import service as m3_service
from app.modules.m6_batches import service as batch_service
from app.modules.m8_retention import boundary, decide, service
from fixtures import expected as X


def _published(store, batch_id):
    return store.published_of(batch_id)["published_seq"]


def _register(store, batch_id, consumer_id, seq, label=""):
    return service.register_consumer(store=store, batch_id=batch_id,
                                     consumer_id=consumer_id, label=label,
                                     confirmed_seq=seq)


# ---------- 边界裁决 ----------
def test_safe_boundary_uses_slowest_consumer(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    _register(store, batch_id, "report", published)
    _register(store, batch_id, "offline", published - 5, "离线消费者")
    verdict = service.plan(store=store, batch_id=batch_id, policy="safe")
    assert verdict["boundary_seq"] == published - 5
    assert verdict["slowest_consumer"] == "offline"
    assert verdict["ceiling_seq"] == published
    assert verdict["resume_seq"] == published - 4


def test_no_consumers_means_safe_boundary_zero(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    verdict = service.plan(store=store, batch_id=batch_id, policy="safe")
    assert verdict["boundary_seq"] == 0
    assert verdict["counters"]["trim"] == 0
    assert verdict["counters"]["total"] == len(store.hot_events(batch_id))


def test_latest_policy_needs_override_when_consumer_behind(settings, store):
    """越过最慢确认必须显式 override，否则 409；给了 override 才收口到发布位置。"""
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    _register(store, batch_id, "offline", max(0, published - 3))
    with pytest.raises(errors.Conflict) as info:
        service.plan(store=store, batch_id=batch_id, policy="latest")
    assert info.value.status_code == 409
    assert info.value.code == "C_RETENTION_OVERRIDE_REQUIRED"
    forced = service.plan(store=store, batch_id=batch_id, policy="latest",
                          override=True)
    assert forced["boundary_seq"] == published
    assert [c["consumer_id"] for c in forced["overridden"]] == ["offline"]
    assert forced["blocked"] == forced["overridden"]


def test_latest_policy_no_override_needed_when_all_caught_up(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    _register(store, batch_id, "report", _published(store, batch_id))
    verdict = service.plan(store=store, batch_id=batch_id, policy="latest")
    assert verdict["boundary_seq"] == verdict["published_seq"]
    assert verdict["overridden"] == []


def test_unknown_policy_rejected(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    with pytest.raises(errors.BadRequest) as info:
        service.plan(store=store, batch_id=run["batch"]["batch_id"], policy="nope")
    assert info.value.code == "E_BAD_POLICY"


# ---------- 逐条裁决 ----------
def test_evidence_rows_are_always_retained(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    verdict = service.plan(store=store, batch_id=batch_id, policy="latest",
                           override=True)
    evidence = [i for i in verdict["items"] if i["row_class"] != "applied"]
    assert evidence
    assert all(i["decision"] == decide.RETAIN for i in evidence)
    assert all(i["reason_code"] == "R_RETAINED_EVIDENCE" for i in evidence)


def test_pending_and_late_are_retained(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    _register(store, batch_id, "report", published)
    verdict = service.plan(store=store, batch_id=batch_id, policy="latest")
    by_seq = {i["seq"]: i for i in verdict["items"]}
    pending = next(i for i in verdict["items"]
                   if i["row_class"] == "applied" and i["reason_code"] == "R_RETAINED_PENDING")
    assert pending["decision"] == decide.RETAIN
    late = [i for i in verdict["items"] if i["reason_code"] == "R_RETAINED_LATE"]
    assert late
    assert all(i["decision"] == decide.RETAIN for i in late)
    assert by_seq  # sanity


def test_all_consumers_caught_up_trims_applied_beyond_open_keys(settings, store):
    """所有消费者确认到发布位置：边界之上的保留消失，可回收的是闭合键的旧版本。"""
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    _register(store, batch_id, "report", published)
    verdict = service.plan(store=store, batch_id=batch_id, policy="latest")
    assert verdict["counters"]["trim"] > 0
    # 边界之上的保留只应来自证据 / 待定 / 迟到
    above = [i for i in verdict["items"]
             if i["reason_code"] == "R_RETAINED_ABOVE_BOUNDARY"]
    assert above == []
    # 被回收的都不能是证据行
    trimmed = [i for i in verdict["items"] if i["decision"] == decide.TRIM]
    assert all(i["row_class"] == "applied" for i in trimmed)


def test_key_with_version_above_boundary_is_retained(settings, store, norm_factory):
    """同键有边界之上的版本 → 旧版本保留，保证重放能折叠出同一状态。"""
    norms = [norm_factory(pk="500", version=1, event_time="2026-11-15T08:00:00Z"),
             norm_factory(pk="500", version=2, event_time="2026-11-15T08:20:00Z"),
             norm_factory(pk="501", version=1, event_time="2026-11-15T08:10:00Z"),
             norm_factory(pk="500", version=3, event_time="2026-11-15T08:40:00Z")]
    events = [{"batch_id": "B-x", "seq": seq, "row_ref": n.row_ref,
               "table_name": n.table, "pk": n.pk, "version": n.version,
               "op": n.op, "event_time_utc": n.time_iso, "row_class": "applied",
               "late": False}
              for seq, n in enumerate(norms, 1)]
    items = decide.judge(events=events, boundary_seq=3, as_of=None)
    assert items[0]["decision"] == decide.RETAIN
    assert items[0]["reason_code"] == "R_RETAINED_KEY_OPEN"
    assert items[1]["decision"] == decide.RETAIN
    assert items[1]["reason_code"] == "R_RETAINED_KEY_OPEN"
    # 另一个键整体闭合在边界内，可回收
    assert items[2]["decision"] == decide.TRIM
    assert items[2]["reason_code"] == "R_TRIMMED_CONFIRMED"
    assert items[3]["decision"] == decide.RETAIN
    assert items[3]["reason_code"] == "R_RETAINED_ABOVE_BOUNDARY"


# ---------- 清理与查询 ----------
def test_trim_archives_and_keeps_records(settings, store):
    """回收只从热区摘除；归档仍在库里，事件读取与导出看到完整历史。"""
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    _register(store, batch_id, "report", published)
    hot_before = len(store.hot_events(batch_id))
    total_before = len(store.load_events(batch_id))
    result = service.execute(store=store, batch_id=batch_id, policy="latest")
    assert result["status"] == "done"
    trim = result["counters"]["trim"]
    assert trim > 0
    assert len(store.hot_events(batch_id)) == hot_before - trim
    assert len(store.load_archive(batch_id)) == trim
    # 完整历史不丢
    assert len(store.load_events(batch_id)) == total_before
    assert result["archive_total"] == trim
    items = store.load_retention_items(result["retention_id"])
    assert len(items) == result["counters"]["total"]
    assert all(r["ok"] for r in result["reconcile"])


def test_retry_same_key_is_idempotent(settings, store):
    """同一边界 + 同一确认快照重试：命中旧结果，不新增裁决行、不重复计数。"""
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    _register(store, batch_id, "report", _published(store, batch_id))
    first = service.execute(store=store, batch_id=batch_id, policy="latest")
    items_before = len(store.load_retention_items(first["retention_id"]))
    archive_before = len(store.load_archive(batch_id))
    second = service.execute(store=store, batch_id=batch_id, policy="latest")
    assert second["idempotent_replay"] is True
    assert second["retention_id"] == first["retention_id"]
    assert second["code"] == "R_RETENTION_REPLAY"
    assert len(store.load_retention_items(first["retention_id"])) == items_before
    assert len(store.load_archive(batch_id)) == archive_before
    assert len(store.list_retentions(batch_id)) == 1


def test_retry_does_not_double_count_after_partial(settings, store):
    """中断后再重试：状态 partial 命中，不新增归档行。"""
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    _register(store, batch_id, "report", _published(store, batch_id))
    result = service.execute(store=store, batch_id=batch_id, policy="latest")
    store.update_retention(result["retention_id"], status="partial")
    archive_before = len(store.load_archive(batch_id))
    again = service.execute(store=store, batch_id=batch_id, policy="latest")
    assert again["idempotent_replay"] is True
    assert len(store.load_archive(batch_id)) == archive_before


def test_resume_continues_from_cursor(settings, store):
    """中断留下的部分保留，续跑只补游标之后的 trim，不重复归档。"""
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    _register(store, batch_id, "report", _published(store, batch_id))
    result = service.execute(store=store, batch_id=batch_id, policy="latest")
    retention_id = result["retention_id"]
    trim_items = [i for i in store.load_retention_items(retention_id)
                  if i["decision"] == decide.TRIM]
    trim_total = len(trim_items)
    # 模拟「归档了一部分、删热区之前中断」：这些行同时在归档与热区，
    # 游标停在第三条，续跑应从第四条继续且不重复归档。
    keep = trim_items[:3]
    store.restore_archive(batch_id, retention_id)  # 全部归档行暂时回到热区（归档仍在）
    # 游标之前的行在真实中断里已经摘除过热区，这里重新摘除，模拟半程状态
    store.delete_events_by_seq(batch_id, [i["seq"] for i in keep])
    store.update_retention(retention_id, status="partial", cursor_seq=keep[-1]["seq"])
    assert len(store.load_archive(batch_id)) == trim_total
    assert len(store.hot_events(batch_id)) == 42 - len(keep)
    resumed = service.resume_run(store=store, retention_id=retention_id)
    assert resumed["status"] == "done"
    assert len(store.load_archive(batch_id)) == trim_total
    assert len(store.hot_events(batch_id)) == 42 - trim_total


def test_interrupted_cleanup_is_resumable(settings, store, monkeypatch):
    """归档途中的异常：状态 partial、游标保留、热区不丢行，续跑后补齐。"""
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    _register(store, batch_id, "report", _published(store, batch_id))
    verdict = service.plan(store=store, batch_id=batch_id, policy="latest")
    trim_count = verdict["counters"]["trim"]
    assert trim_count > 2

    original = store.archive_event
    calls = {"n": 0}

    def flaky(**kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("磁盘抖动")
        return original(**kwargs)

    monkeypatch.setattr(store, "archive_event", flaky)
    result = service.execute(store=store, batch_id=batch_id, policy="latest")
    assert result["status"] == "partial"
    assert result["code"] == "R_RETENTION_PARTIAL"
    retention_id = result["retention_id"]
    # 中断没有丢事件：热区 ∪ 归档 按到达序号去重后仍是原始 42 行
    before = {r["seq"] for r in store.hot_events(batch_id)}
    before |= {r["seq"] for r in store.load_archive(batch_id)}
    assert len(before) == 42

    monkeypatch.setattr(store, "archive_event", original)
    resumed = service.execute(store=store, batch_id=batch_id, policy="latest")
    assert resumed["idempotent_replay"] is True
    assert resumed["status"] == "done"
    after = {r["seq"] for r in store.hot_events(batch_id)}
    after |= {r["seq"] for r in store.load_archive(batch_id)}
    assert len(after) == 42
    assert len(store.load_archive(batch_id)) == trim_count
    assert len(store.list_retentions(batch_id)) == 1


def test_restore_brings_archive_back(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    _register(store, batch_id, "report", _published(store, batch_id))
    result = service.execute(store=store, batch_id=batch_id, policy="latest")
    hot_after = len(store.hot_events(batch_id))
    restored = service.restore(store=store, batch_id=batch_id,
                               retention_id=result["retention_id"])
    assert restored["restored"] == result["counters"]["trim"]
    assert len(store.hot_events(batch_id)) == hot_after + result["counters"]["trim"]
    # 再次 restore 不重复插入
    again = service.restore(store=store, batch_id=batch_id,
                            retention_id=result["retention_id"])
    assert again["restored"] == 0


# ---------- 消费者确认 ----------
def test_consumer_confirmation_is_monotonic(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    _register(store, batch_id, "report", 2)
    service.confirm_consumer(store=store, batch_id=batch_id, consumer_id="report",
                             confirmed_seq=5)
    with pytest.raises(errors.Conflict) as info:
        service.confirm_consumer(store=store, batch_id=batch_id,
                                 consumer_id="report", confirmed_seq=3)
    assert info.value.code == "C_CONSUMER_REGRESS"
    consumer = store.get_consumer(batch_id, "report")
    assert consumer["confirmed_seq"] == 5


def test_consumer_cannot_confirm_beyond_published(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    published = _published(store, batch_id)
    with pytest.raises(errors.BadRequest) as info:
        _register(store, batch_id, "report", published + 1)
    assert info.value.code == "E_CONSUMER_AHEAD"


def test_bad_confirmation_value(settings, store):
    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    with pytest.raises(errors.BadRequest) as info:
        _register(store, batch_id, "report", "abc")
    assert info.value.code == "E_BAD_CONFIRMATION"


# ---------- 导出（同一结果） ----------
def test_retention_exports_match_store(settings, store):
    from app.modules.m7_export import service as export_service

    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    _register(store, batch_id, "report", _published(store, batch_id))
    result = service.execute(store=store, batch_id=batch_id, policy="latest")
    data, name = export_service.render_retention_json(store, batch_id)
    doc = json.loads(data.decode("utf-8"))
    assert name.endswith("-retention.json")
    assert doc["retention_id"] == result["retention_id"]
    assert doc["boundary_seq"] == result["boundary_seq"]
    assert doc["counters"]["trim"] == result["counters"]["trim"]
    assert len(doc["items"]) == result["counters"]["total"]
    assert {"trim", "retain"} == set(doc["decision_labels"])
    # CSV 同一份裁决
    csv_bytes, csv_name = export_service.render_retention_csv(store, batch_id)
    assert csv_name.endswith("-retention.csv")
    text = csv_bytes.decode("utf-8-sig")
    assert "R_TRIMMED_CONFIRMED" in text
    assert len(text.splitlines()) == result["counters"]["total"] + 1


def test_snapshot_rebuild_after_trim_is_stable(settings, store):
    """回收之后重算快照：读回归档，结果与回收前一致。"""
    from app.modules.m5_snapshot import service as m5_service

    run = batch_service.run(settings=settings, store=store, source="all")
    batch_id = run["batch"]["batch_id"]
    before = m5_service.summarize(store, batch_id)
    _register(store, batch_id, "report", _published(store, batch_id))
    service.execute(store=store, batch_id=batch_id, policy="latest")
    after = m5_service.summarize(store, batch_id)
    assert after["snapshot_keys"] == before["snapshot_keys"]
    assert after["amount_snapshot"] == before["amount_snapshot"]
    assert after["amount_applied"] == before["amount_applied"]
