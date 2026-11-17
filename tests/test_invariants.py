"""不变量：行账守恒、键账守恒、金额守恒、重跑幂等、迟到与准时互补。"""
from __future__ import annotations

from decimal import Decimal

from app.modules.m6_batches import service
from fixtures import expected as X


def test_row_accounting_identity(run):
    """原始行 = 已应用 + 重复 + 冲突 + 隔离（含空行之外的全部数据行）。"""
    totals = run["totals"]
    assert int(totals["raw_lines"]) == (int(totals["applied"]) + int(totals["duplicates"])
                                        + int(totals["conflicts"])
                                        + int(totals["quarantine"]))
    assert int(totals["raw_lines"]) == X.RAW_LINES


def test_key_accounting_identity(run):
    """已应用键 = 快照键 + 只有待定事件的键。"""
    totals = run["totals"]
    assert int(totals["keys_total"]) == (int(totals["snapshot_keys"])
                                         + int(totals["pending_only_keys"]))
    assert int(totals["snapshot_keys"]) == (int(totals["active_keys"])
                                            + int(totals["tombstone_keys"]))
    assert int(totals["keys_total"]) == X.KEYS_TOTAL


def test_amount_conservation_is_exact(run):
    """金额守恒：快照 + 待定 + 变化 = 已应用，Decimal 精确相等。"""
    totals = run["totals"]
    snapshot = Decimal(str(totals["amount_snapshot"]))
    pending = Decimal(str(totals["amount_pending"]))
    drift = Decimal(str(totals["amount_drift"]))
    applied = Decimal(str(totals["amount_applied"]))
    assert snapshot + pending + drift == applied
    assert applied == Decimal(X.AMOUNT_APPLIED)
    assert snapshot.quantize(Decimal("0.01")) == snapshot


def test_idempotent_rerun_keeps_rows_and_totals(settings, store):
    first = service.run(settings=settings, store=store, source="all")
    batch_id = first["batch"]["batch_id"]
    rows_before = store.count_rows(batch_id)
    replay = service.run(settings=settings, store=store, source="all")
    assert replay["idempotent_replay"] is True
    assert store.count_rows(batch_id) == rows_before
    assert replay["totals"]["applied"] == first["totals"]["applied"]
    assert replay["totals"]["amount_snapshot"] == first["totals"]["amount_snapshot"]
    assert len(store.list_batches()) == 1


def test_late_and_on_time_add_up(run):
    """迟到 + 准时 = 已应用；迟到只标记不丢弃。"""
    totals = run["totals"]
    assert int(totals["late_events"]) + int(totals["on_time_events"]) == int(totals["applied"])
    assert int(totals["late_events"]) == X.LATE_EVENTS
    assert int(totals["on_time_events"]) == X.ON_TIME_EVENTS


def test_retention_covers_every_hot_event_once(settings, store):
    """不变量：热区事件 = 回收 + 保留，且回收只发生在保留边界内。"""
    from app.modules.m8_retention import decide, service

    run = service_run(settings, store)
    batch_id = run["batch"]["batch_id"]
    published = store.published_of(batch_id)["published_seq"]
    service.register_consumer(store=store, batch_id=batch_id,
                              consumer_id="report", confirmed_seq=published)
    verdict = service.plan(store=store, batch_id=batch_id, policy="latest")
    items = verdict["items"]
    trim = [i for i in items if i["decision"] == decide.TRIM]
    retain = [i for i in items if i["decision"] == decide.RETAIN]
    assert len(trim) + len(retain) == verdict["hot_before"]
    assert all(int(i["seq"]) <= verdict["boundary_seq"] for i in trim)
    assert len({i["seq"] for i in items}) == len(items)
    assert all(row["ok"] for row in verdict["reconcile"])


def test_retention_never_loses_records(settings, store):
    """不变量：清理后 热区 ∪ 归档 仍是完整 42 行，四账与快照不变。"""
    from app.modules.m5_snapshot import service as m5_service
    from app.modules.m8_retention import service

    run = service_run(settings, store)
    batch_id = run["batch"]["batch_id"]
    totals_before = dict(run["totals"])
    summary_before = m5_service.summarize(store, batch_id)
    published = store.published_of(batch_id)["published_seq"]
    service.register_consumer(store=store, batch_id=batch_id,
                              consumer_id="report", confirmed_seq=published)
    result = service.execute(store=store, batch_id=batch_id, policy="latest")
    merged = {r["seq"] for r in store.load_events(batch_id)}
    hot = {r["seq"] for r in store.hot_events(batch_id)}
    archived = {r["seq"] for r in store.load_archive(batch_id)}
    assert merged == hot | archived
    assert len(merged) == X.RAW_LINES
    assert archived and not (hot & archived)
    assert len(archived) == result["counters"]["trim"]
    # 快照与合计不受回收影响
    summary_after = m5_service.summarize(store, batch_id)
    assert summary_after["snapshot_keys"] == summary_before["snapshot_keys"]
    assert summary_after["amount_applied"] == summary_before["amount_applied"]
    assert int(store.load_totals(batch_id)["applied"]) == int(totals_before["applied"])


def service_run(settings, store):
    from app.modules.m6_batches import service as batch_service
    return batch_service.run(settings=settings, store=store, source="all")
