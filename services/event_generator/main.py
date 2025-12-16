#!/usr/bin/env python3
"""Simple event generator that can send JSON order events to Kafka (Redpanda) or print them to stdout (dry-run)."""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

ORDER_EVENT_TYPES: list[str] = [
    "created",
    "paid",
    "packed",
    "shipped",
    "out_for_delivery",
    "delivered",
    "returned",
    "cancelled",
]

# Realistic order lifecycle patterns
NORMAL_LIFECYCLE = ["created", "paid", "packed", "shipped", "out_for_delivery", "delivered"]
CANCELLED_EARLY = ["created", "cancelled"]
CANCELLED_LATE = ["created", "paid", "packed", "cancelled"]
RETURNED_LIFECYCLE = ["created", "paid", "packed", "shipped", "delivered", "returned"]
INCOMPLETE_LIFECYCLE = ["created", "paid"]  # customer never completes


def make_event(
    order_id: str, event_type: str, base_ts: datetime, delay_seconds: int = 0
) -> dict[str, Any]:
    """Create a single order event dict with realistic metadata."""
    ts = base_ts + timedelta(seconds=delay_seconds)
    metadata: dict[str, Any] = {
        "region": random.choice(["US-EAST", "US-WEST", "EU", "ASIA"]),
        "warehouse_id": f"WH-{random.randint(1, 10):03d}",
    }

    # Add event-specific metadata
    if event_type == "created":
        metadata["total_amount"] = round(random.uniform(10.0, 500.0), 2)
        metadata["items_count"] = random.randint(1, 10)
    elif event_type == "delivered":
        metadata["delivery_time_minutes"] = delay_seconds // 60
        metadata["on_time"] = delay_seconds < 7200  # 2 hours SLA
    elif event_type == "returned":
        metadata["return_reason"] = random.choice(
            ["damaged", "wrong_item", "not_needed", "defective"]
        )

    return {
        "order_id": order_id,
        "event_type": event_type,
        "timestamp": ts.isoformat(),
        "metadata": metadata,
    }


def generate_events(
    n: int,
    duplicate_rate: float = 0.02,
    out_of_order_rate: float = 0.05,
    late_delivery_rate: float = 0.1,
) -> list[dict[str, Any]]:
    """Generate a list of order events with realistic patterns and edge cases.

    Each order follows a lifecycle pattern. Returns a flattened list with:
    - Normal orders (complete lifecycle)
    - Cancelled orders (early and late)
    - Returned orders
    - Incomplete orders (missing steps)
    - Late deliveries
    - Duplicates
    - Out-of-order events
    """
    from datetime import timezone

    base_time = datetime.now(timezone.utc).replace(tzinfo=None)
    events: list[dict[str, Any]] = []

    for i in range(n):
        oid = str(uuid4())
        ts = base_time + timedelta(minutes=i)

        # Choose lifecycle pattern
        rand = random.random()
        if rand < 0.7:  # 70% normal orders
            lifecycle = NORMAL_LIFECYCLE
            # Some orders have late deliveries
            if random.random() < late_delivery_rate:
                delay = random.randint(7200, 14400)  # 2-4 hours late
            else:
                delay = random.randint(300, 3600)  # 5-60 minutes normal
        elif rand < 0.80:  # 10% cancelled early
            lifecycle = CANCELLED_EARLY
            delay = random.randint(30, 300)
        elif rand < 0.85:  # 5% cancelled late
            lifecycle = CANCELLED_LATE
            delay = random.randint(300, 1800)
        elif rand < 0.90:  # 5% returned
            lifecycle = RETURNED_LIFECYCLE
            delay = random.randint(3600, 7200)
        else:  # 10% incomplete (missing final steps)
            lifecycle = INCOMPLETE_LIFECYCLE
            delay = random.randint(60, 600)

        # Generate events for this order
        cumulative_delay = 0
        for step in lifecycle:
            step_delay = random.randint(30, 180) if step != "delivered" else delay
            cumulative_delay += step_delay
            events.append(make_event(oid, step, ts, cumulative_delay))

    # add some duplicates
    dup_count = int(len(events) * duplicate_rate)
    for _ in range(dup_count):
        events.append(random.choice(events))

    # out-of-order: shuffle a subset
    ooo_count = int(len(events) * out_of_order_rate)
    for _ in range(ooo_count):
        i = random.randrange(len(events))
        j = random.randrange(len(events))
        events[i], events[j] = events[j], events[i]

    return events


def _send_to_kafka(events: list[dict[str, Any]], topic: str, brokers: str) -> None:
    # import kafka lazily to keep dry-run simple; kafka might not be available in all environments
    try:
        from kafka import KafkaProducer  # type: ignore
    except Exception as exc:  # pragma: no cover - environmental
        raise RuntimeError("kafka-python is required to publish events") from exc

    producer = KafkaProducer(
        bootstrap_servers=[brokers], value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    for e in events:
        producer.send(topic, e)
    producer.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true", help="print events to stdout")
    parser.add_argument("--topic", type=str, default="orders.events")
    parser.add_argument("--brokers", type=str, default="localhost:29092")
    args = parser.parse_args()

    events = generate_events(args.events)

    if args.dry_run:
        for e in events[: args.events]:
            print(json.dumps(e))
        return

    _send_to_kafka(events[: args.events], args.topic, args.brokers)


if __name__ == "__main__":
    main()
