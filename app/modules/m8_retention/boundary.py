"""保留边界裁决：在最慢确认位置收口，还是越过它按最新发布位置收口。

输入只有三样：发布位置（已应用事件的最大到达序号）、消费者确认位置、策略。

- `safe`：边界 = 最慢的活跃消费者确认位置；没登记消费者时为 0（一条都不回收）。
  边界之上的事件一律保留，由挡住清理的消费者负责，事后可从归档补数。
- `latest`：边界 = 最新发布位置。若仍有活跃消费者没确认到发布位置，必须显式
  `override=True` 才允许越过，否则抛 `C_RETENTION_OVERRIDE_REQUIRED`（409）；
  越过时把被越过的消费者记入 `overridden`，页面上单独可见。

边界只减不增：`boundary_seq ≤ ceiling_seq = published_seq`。回收也永远不超过边界，
这一点在 reconcile 的「边界账」里显式核对。
"""
from __future__ import annotations

from ... import errors, reasons

POLICIES = ("safe", "latest")
POLICY_LABELS = {"safe": "最慢确认（安全）", "latest": "最新发布（越过慢消费者）"}


def _active(consumers: list[dict]) -> list[dict]:
    return [c for c in consumers if (c.get("status") or "active") == "active"]


def slowest(consumers: list[dict]) -> dict | None:
    active = _active(consumers)
    if not active:
        return None
    return min(active, key=lambda c: (int(c.get("confirmed_seq") or 0), c["consumer_id"]))


def blockers(consumers: list[dict], published_seq: int) -> list[dict]:
    """没确认到发布位置的活跃消费者；按确认位置升序，第一位就是最慢的。"""
    out = []
    for consumer in _active(consumers):
        confirmed = int(consumer.get("confirmed_seq") or 0)
        if confirmed < published_seq:
            out.append({
                "consumer_id": consumer["consumer_id"],
                "label": consumer.get("label", ""),
                "confirmed_seq": confirmed,
                "published_seq": published_seq,
                "behind": published_seq - confirmed,
                "blocking_from_seq": confirmed + 1,
                "confirmed_at": consumer.get("confirmed_at"),
                # 同一消费者在 safe 下挡住清理、在 latest 下被越过，各给一个原因码
                "blocks_code": "R_RETAINED_HELD",
                "override_code": "R_OVERRIDDEN_CONSUMER",
            })
    return sorted(out, key=lambda c: (c["confirmed_seq"], c["consumer_id"]))


def decide(*, policy: str, published_seq: int, published_event_time: str | None,
           consumers: list[dict], override: bool = False) -> dict:
    """返回边界裁决；越权时抛 409，未知策略抛 400。调用前不改任何状态。"""
    if policy not in POLICIES:
        raise errors.BadRequest(
            f"未知保留策略 {policy!r}；可选 " + " / ".join(POLICIES),
            code="E_BAD_POLICY")
    published_seq = int(published_seq or 0)
    active = _active(consumers)
    slow = slowest(consumers)
    frozen = blockers(consumers, published_seq)
    slowest_seq = int(slow.get("confirmed_seq") or 0) if slow else None
    slowest_id = slow["consumer_id"] if slow else None

    if policy == "safe":
        boundary = slowest_seq if slowest_seq is not None else 0
        overridden: list[dict] = []
        code = "R_BOUNDARY_SAFE"
        requires_override = False
    else:
        boundary = published_seq
        overridden = frozen
        code = "R_BOUNDARY_LATEST"
        requires_override = bool(frozen) and not override
        if requires_override:
            raise errors.Conflict(
                "按最新发布位置收口会越过尚未确认的消费者，需要显式 override",
                code="C_RETENTION_OVERRIDE_REQUIRED",
                detail={"published_seq": published_seq,
                        "slowest_seq": slowest_seq,
                        "blocking": frozen,
                        "reason": reasons.label("C_RETENTION_OVERRIDE_REQUIRED")})

    boundary = max(0, min(boundary, published_seq))
    return {
        "policy": policy,
        "policy_label": POLICY_LABELS[policy],
        "code": code,
        "published_seq": published_seq,
        "published_event_time": published_event_time,
        "slowest_seq": slowest_seq,
        "slowest_consumer": slowest_id,
        "boundary_seq": boundary,
        "ceiling_seq": published_seq,
        "blocked": frozen,
        "overridden": overridden,
        "override": bool(override),
        "consumer_count": len(active),
        "resume_seq": boundary + 1,
        "note": (f"边界取最慢确认位置 {boundary}" if policy == "safe"
                 else f"越过 {len(overridden)} 个消费者，边界取发布位置 {boundary}"),
    }


def boundary_event_time(events: list[dict], boundary_seq: int) -> str | None:
    """边界序号对应的事件时刻；边界为 0 时没有对应事件。"""
    for row in events:
        if int(row.get("seq") or 0) == boundary_seq:
            return row.get("event_time_utc")
    return None
