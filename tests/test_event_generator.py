import os
import sys

# Ensure the repository root is on sys.path so tests can import local packages
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from services.event_generator.main import generate_events  # noqa: E402


def test_generate_count() -> None:
    events = generate_events(50)
    assert len(events) >= 50


def test_event_schema() -> None:
    events = generate_events(10)
    for e in events:
        assert "order_id" in e
        assert "event_type" in e
        assert "timestamp" in e
        assert isinstance(e.get("order_id"), str)
        assert isinstance(e.get("event_type"), str)
        assert isinstance(e.get("timestamp"), str)
