"""
Stream Processing Practice — Coding Exercises
================================================

Two sections:
  A) Convert batch solutions to streaming (tied to P6, P7, P9, P10)
  B) Streaming-specific coding patterns (windows, late arrivals, state management)

In these exercises, events arrive ONE AT A TIME via process_event().
You maintain state in memory and emit results when appropriate.
This simulates how Flink/Spark Streaming/Kafka Streams actually work.
"""

from datetime import datetime, timedelta
from collections import defaultdict


# ============================================================================
# SECTION A: BATCH → STREAMING CONVERSIONS
# ============================================================================

# ============================================================================
# S1. Streaming Status Change Detector (streaming version of P6)
#
# Batch P6: you had ALL records, sorted them, then scanned for changes.
# Streaming: events arrive one at a time, potentially out of order.
# Emit a change event immediately when you detect a new status.
#
# Key difference: you can't sort — you must track per-user state.
# ============================================================================

class StreamingStatusChangeDetector:
    """
    Call process_event() for each incoming event.
    Returns a change record if the status changed, None otherwise.
    Skip events with missing/null status.
    """
    def __init__(self):
        raise NotImplementedError
        pass

    def process_event(self, event):
        """
        event: {"user_id": str, "status": str or None, "timestamp": str}
        Returns: {"user_id", "status", "timestamp", "prev_status"} if change detected, else None
        """
        raise NotImplementedError
        pass


def test_s1():
    detector = StreamingStatusChangeDetector()

    events = [
        {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T10:00:00"},
        {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T11:00:00"},  # no change
        {"user_id": "u1", "status": "inactive", "timestamp": "2024-01-01T12:00:00"},  # change
        {"user_id": "u2", "status": "active", "timestamp": "2024-01-01T10:00:00"},  # first for u2
        {"user_id": "u1", "status": None, "timestamp": "2024-01-01T12:30:00"},  # null — skip
        {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T13:00:00"},  # change back
    ]

    results = []
    for e in events:
        r = detector.process_event(e)
        if r is not None:
            results.append(r)

    # First event per user counts as a change
    assert len(results) == 4, f"Expected 4 changes, got {len(results)}"
    assert results[0]["status"] == "active" and results[0]["prev_status"] is None  # u1 first
    assert results[1]["status"] == "inactive" and results[1]["prev_status"] == "active"  # u1 change
    assert results[2]["status"] == "active" and results[2]["user_id"] == "u2"  # u2 first
    assert results[3]["status"] == "active" and results[3]["prev_status"] == "inactive"  # u1 change back
    print("✅ S1 passed")


# ============================================================================
# S2. Streaming Session Builder (streaming version of P7)
#
# Batch P7: you had ALL events, grouped by user, sorted, and split on gaps.
# Streaming: events arrive one at a time.
#
# Challenge: when do you "close" a session? You can't wait forever.
# Use a timeout: if no event for a user for >30 min, close the session.
# Call flush() to close all open sessions (e.g., end of day).
# ============================================================================

class StreamingSessionBuilder:
    """
    process_event() handles each incoming event.
    Returns a completed session dict when a session closes (due to gap), else None.
    flush() closes all remaining open sessions and returns them.
    """
    def __init__(self, gap_minutes=30):
        self.gap_minutes = gap_minutes
        raise NotImplementedError
        pass

    def process_event(self, event):
        """
        event: {"user_id": str, "event": str, "timestamp": str}
        Returns: completed session dict if a previous session just closed, else None
        Session dict: {"user_id", "session_id", "start", "end", "event_count"}
        """
        raise NotImplementedError
        pass

    def flush(self):
        """Close all open sessions and return them as a list."""
        raise NotImplementedError
        pass


def test_s2():
    builder = StreamingSessionBuilder(gap_minutes=30)

    events = [
        {"user_id": "u1", "event": "click", "timestamp": "2024-01-01T10:00:00"},
        {"user_id": "u1", "event": "view", "timestamp": "2024-01-01T10:15:00"},
        {"user_id": "u2", "event": "click", "timestamp": "2024-01-01T10:20:00"},
        {"user_id": "u1", "event": "click", "timestamp": "2024-01-01T10:25:00"},
        # 40 min gap for u1 — next event should close previous session
        {"user_id": "u1", "event": "view", "timestamp": "2024-01-01T11:05:00"},
    ]

    closed_sessions = []
    for e in events:
        result = builder.process_event(e)
        if result is not None:
            closed_sessions.append(result)

    # u1's first session should have closed when the 11:05 event arrived
    assert len(closed_sessions) == 1, f"Expected 1 closed session, got {len(closed_sessions)}"
    assert closed_sessions[0]["user_id"] == "u1"
    assert closed_sessions[0]["event_count"] == 3
    assert closed_sessions[0]["start"] == "2024-01-01T10:00:00"
    assert closed_sessions[0]["end"] == "2024-01-01T10:25:00"

    # Flush remaining open sessions
    remaining = builder.flush()
    assert len(remaining) == 2  # u2's session + u1's new session

    u1_new = [s for s in remaining if s["user_id"] == "u1"]
    u2_sess = [s for s in remaining if s["user_id"] == "u2"]
    assert u1_new[0]["event_count"] == 1
    assert u1_new[0]["start"] == "2024-01-01T11:05:00"
    assert u2_sess[0]["event_count"] == 1

    print("✅ S2 passed")


# ============================================================================
# S3. Streaming Threshold Monitor (streaming version of P9)
#
# Batch P9: sorted all data, scanned for crossings.
# Streaming: values arrive one at a time per machine+metric.
# Emit an alert immediately when a threshold is crossed.
# ============================================================================

class StreamingThresholdMonitor:
    """
    process_event() handles each incoming reading.
    Returns an alert dict if threshold was just crossed upward, else None.
    """
    def __init__(self, threshold=100):
        self.threshold = threshold
        raise NotImplementedError
        pass

    def process_event(self, event):
        """
        event: {"machine_id": str, "metric": str, "value": float or None, "timestamp": str}
        Returns: {"machine_id", "metric", "crossed_at", "value", "prev_value"} if crossed, else None
        Skip events with None value (sensor error).
        """
        raise NotImplementedError
        pass


def test_s3():
    monitor = StreamingThresholdMonitor(threshold=100)

    events = [
        {"machine_id": "m1", "metric": "temp", "value": 95, "timestamp": "T1"},
        {"machine_id": "m1", "metric": "temp", "value": 102, "timestamp": "T2"},  # cross!
        {"machine_id": "m1", "metric": "temp", "value": 105, "timestamp": "T3"},  # still above, no new cross
        {"machine_id": "m1", "metric": "temp", "value": 98, "timestamp": "T4"},   # back below
        {"machine_id": "m1", "metric": "temp", "value": None, "timestamp": "T5"}, # skip
        {"machine_id": "m1", "metric": "temp", "value": 101, "timestamp": "T6"},  # cross again!
        {"machine_id": "m2", "metric": "temp", "value": 110, "timestamp": "T1"},  # first event above — NOT a cross (no prev)
        {"machine_id": "m2", "metric": "temp", "value": 90, "timestamp": "T2"},   # drop below
        {"machine_id": "m2", "metric": "temp", "value": 105, "timestamp": "T3"},  # cross!
    ]

    alerts = []
    for e in events:
        r = monitor.process_event(e)
        if r is not None:
            alerts.append(r)

    assert len(alerts) == 3, f"Expected 3 alerts, got {len(alerts)}"
    assert alerts[0]["value"] == 102 and alerts[0]["prev_value"] == 95
    assert alerts[1]["value"] == 101 and alerts[1]["prev_value"] == 98
    assert alerts[2]["value"] == 105 and alerts[2]["machine_id"] == "m2"
    print("✅ S3 passed")


# ============================================================================
# SECTION B: STREAMING-SPECIFIC PATTERNS
# ============================================================================

# ============================================================================
# S4. Tumbling Window Aggregator
#
# Count events per user in fixed 5-minute windows.
# When an event arrives that belongs to a NEW window, emit the result for
# the previous window and start a new one.
# A tumbling window: [10:00, 10:05), [10:05, 10:10), etc.
# ============================================================================

class TumblingWindowCounter:
    """
    Counts events per user in fixed-size tumbling windows.
    When a new window starts, emit the previous window's counts.
    """
    def __init__(self, window_minutes=5):
        self.window_minutes = window_minutes
        raise NotImplementedError
        pass

    def _get_window_start(self, timestamp_str):
        """Helper: return the window start for a given timestamp."""
        ts = datetime.fromisoformat(timestamp_str)
        minutes = (ts.minute // self.window_minutes) * self.window_minutes
        return ts.replace(minute=minutes, second=0, microsecond=0).isoformat()

    def process_event(self, event):
        """
        event: {"user_id": str, "timestamp": str}
        Returns: list of {"user_id", "window_start", "window_end", "count"} for any
                 windows that just closed, or empty list.
        """
        raise NotImplementedError
        pass

    def flush(self):
        """Emit all currently open windows."""
        raise NotImplementedError
        pass


def test_s4():
    counter = TumblingWindowCounter(window_minutes=5)

    events = [
        {"user_id": "u1", "timestamp": "2024-01-01T10:00:30"},  # window 10:00
        {"user_id": "u1", "timestamp": "2024-01-01T10:01:00"},  # window 10:00
        {"user_id": "u2", "timestamp": "2024-01-01T10:02:00"},  # window 10:00
        {"user_id": "u1", "timestamp": "2024-01-01T10:04:00"},  # window 10:00
        # Next event is in 10:05 window — should trigger close of 10:00 window
        {"user_id": "u1", "timestamp": "2024-01-01T10:05:30"},  # window 10:05
        {"user_id": "u1", "timestamp": "2024-01-01T10:06:00"},  # window 10:05
        # Jump to 10:15 — should close 10:05 window
        {"user_id": "u1", "timestamp": "2024-01-01T10:15:00"},  # window 10:15
    ]

    all_emitted = []
    for e in events:
        emitted = counter.process_event(e)
        if emitted:
            all_emitted.extend(emitted)

    # After event at 10:05, the 10:00 window closes
    # After event at 10:15, the 10:05 window closes
    assert len(all_emitted) >= 2, f"Expected at least 2 emitted windows, got {len(all_emitted)}"

    w1 = [w for w in all_emitted if w["window_start"] == "2024-01-01T10:00:00"]
    u1_w1 = [w for w in w1 if w["user_id"] == "u1"]
    u2_w1 = [w for w in w1 if w["user_id"] == "u2"]
    assert u1_w1[0]["count"] == 3, f"u1 in 10:00 window should have 3 events, got {u1_w1[0]['count']}"
    assert u2_w1[0]["count"] == 1

    w2 = [w for w in all_emitted if w["window_start"] == "2024-01-01T10:05:00"]
    assert w2[0]["count"] == 2  # u1 had 2 events in 10:05 window

    # Flush should emit the 10:15 window
    remaining = counter.flush()
    assert len(remaining) == 1
    assert remaining[0]["count"] == 1
    print("✅ S4 passed")


# ============================================================================
# S5. Sliding Window Rate Counter
#
# Detect if any user exceeds 5 events in any 60-second sliding window.
# Unlike tumbling windows, sliding windows overlap.
#
# Approach: for each event, count how many events that user had in the
# last 60 seconds. If >5, emit a breach.
# ============================================================================

class SlidingWindowRateCounter:
    """
    Tracks per-user event timestamps.
    On each event, checks if user exceeded max_events in the last window_seconds.
    """
    def __init__(self, max_events=5, window_seconds=60):
        self.max_events = max_events
        self.window_seconds = window_seconds
        raise NotImplementedError
        pass

    def process_event(self, event):
        """
        event: {"user_id": str, "timestamp": str}
        Returns: {"user_id", "timestamp", "event_count_in_window"} if breach, else None
        """
        raise NotImplementedError
        pass


def test_s5():
    counter = SlidingWindowRateCounter(max_events=5, window_seconds=60)

    base = datetime(2024, 1, 1, 10, 0, 0)

    # u1: 6 events in 30 seconds — breach
    events = [
        {"user_id": "u1", "timestamp": (base + timedelta(seconds=i * 5)).isoformat()}
        for i in range(6)
    ]
    # u2: 3 events spread out — no breach
    events += [
        {"user_id": "u2", "timestamp": (base + timedelta(seconds=i * 25)).isoformat()}
        for i in range(3)
    ]
    # u1: event 2 minutes later — back to 1 in window, no breach
    events.append({"user_id": "u1", "timestamp": (base + timedelta(seconds=150)).isoformat()})

    # Sort by timestamp to simulate arrival order
    events.sort(key=lambda e: e["timestamp"])

    breaches = []
    for e in events:
        r = counter.process_event(e)
        if r is not None:
            breaches.append(r)

    u1_breaches = [b for b in breaches if b["user_id"] == "u1"]
    u2_breaches = [b for b in breaches if b["user_id"] == "u2"]

    assert len(u1_breaches) >= 1, "u1 should have at least 1 breach"
    assert len(u2_breaches) == 0, "u2 should have no breaches"
    assert all(b["event_count_in_window"] > 5 for b in u1_breaches)
    print("✅ S5 passed")


# ============================================================================
# S6. Late Event Handler with Watermarks
#
# Events have event_time (when it happened) and arrive at processing_time.
# Events can arrive late (processing_time >> event_time).
#
# Rules:
# - Maintain a watermark = max event_time seen so far - allowed_lateness
# - Process events normally if event_time >= watermark
# - If event_time < watermark, the event is "too late" — emit to a side output
#
# This simulates how Flink/Spark handle late data.
# ============================================================================

class WatermarkProcessor:
    """
    Processes events with event-time semantics and late event detection.
    """
    def __init__(self, allowed_lateness_seconds=300):
        self.allowed_lateness_seconds = allowed_lateness_seconds
        raise NotImplementedError
        pass

    def process_event(self, event):
        """
        event: {"id": str, "event_time": str, "processing_time": str, "data": any}
        Returns: {"status": "processed" | "late", "event": event, "watermark": str}
        """
        raise NotImplementedError
        pass


def test_s6():
    processor = WatermarkProcessor(allowed_lateness_seconds=300)  # 5 min

    events = [
        # Normal event
        {"id": "e1", "event_time": "2024-01-01T10:00:00", "processing_time": "2024-01-01T10:00:05", "data": "click"},
        # Later event, advances watermark
        {"id": "e2", "event_time": "2024-01-01T10:10:00", "processing_time": "2024-01-01T10:10:02", "data": "view"},
        # Slightly late but within allowed lateness (event_time 10:06, watermark = 10:10 - 5min = 10:05)
        {"id": "e3", "event_time": "2024-01-01T10:06:00", "processing_time": "2024-01-01T10:10:10", "data": "click"},
        # Too late (event_time 10:01, watermark = 10:05)
        {"id": "e4", "event_time": "2024-01-01T10:01:00", "processing_time": "2024-01-01T10:10:15", "data": "click"},
        # New event advances watermark further
        {"id": "e5", "event_time": "2024-01-01T10:20:00", "processing_time": "2024-01-01T10:20:01", "data": "purchase"},
        # Now 10:06 would be too late (watermark = 10:20 - 5min = 10:15)
        {"id": "e6", "event_time": "2024-01-01T10:06:00", "processing_time": "2024-01-01T10:20:05", "data": "click"},
    ]

    results = []
    for e in events:
        r = processor.process_event(e)
        results.append(r)

    assert results[0]["status"] == "processed"  # e1: normal
    assert results[1]["status"] == "processed"  # e2: normal
    assert results[2]["status"] == "processed"  # e3: late but within allowed lateness
    assert results[3]["status"] == "late"        # e4: too late
    assert results[4]["status"] == "processed"  # e5: normal
    assert results[5]["status"] == "late"        # e6: too late now
    print("✅ S6 passed")


# ============================================================================
# S7. Streaming Deduplicator
#
# Events may arrive more than once (at-least-once delivery).
# Deduplicate based on event_id.
# But you can't store ALL event IDs forever — memory will blow up.
#
# Use a time-bounded dedup window: only dedup within the last N minutes.
# Events older than the window are assumed safe (won't see dupes).
# ============================================================================

class StreamingDeduplicator:
    """
    Deduplicates events by event_id within a time window.
    Old entries are evicted to prevent unbounded memory growth.
    """
    def __init__(self, dedup_window_seconds=600):  # 10 min window
        self.dedup_window_seconds = dedup_window_seconds
        raise NotImplementedError
        pass

    def process_event(self, event):
        """
        event: {"event_id": str, "timestamp": str, "data": any}
        Returns: {"status": "new" | "duplicate", "event_id": str}
        Also evicts expired entries from state.
        """
        raise NotImplementedError
        pass

    def state_size(self):
        """Return current number of event IDs being tracked (for testing memory management)."""
        raise NotImplementedError
        pass


def test_s7():
    dedup = StreamingDeduplicator(dedup_window_seconds=600)

    events = [
        {"event_id": "e1", "timestamp": "2024-01-01T10:00:00", "data": "click"},
        {"event_id": "e2", "timestamp": "2024-01-01T10:01:00", "data": "view"},
        {"event_id": "e1", "timestamp": "2024-01-01T10:02:00", "data": "click"},  # dup of e1
        {"event_id": "e3", "timestamp": "2024-01-01T10:05:00", "data": "purchase"},
        {"event_id": "e2", "timestamp": "2024-01-01T10:05:30", "data": "view"},  # dup of e2
        # 15 minutes later — e1 and e2 should be evicted from dedup state
        {"event_id": "e1", "timestamp": "2024-01-01T10:15:00", "data": "click"},  # NOT a dup anymore (outside window)
        {"event_id": "e4", "timestamp": "2024-01-01T10:15:30", "data": "view"},
    ]

    results = []
    for e in events:
        r = dedup.process_event(e)
        results.append(r)

    assert results[0]["status"] == "new"        # e1 first time
    assert results[1]["status"] == "new"        # e2 first time
    assert results[2]["status"] == "duplicate"  # e1 duplicate
    assert results[3]["status"] == "new"        # e3 first time
    assert results[4]["status"] == "duplicate"  # e2 duplicate
    assert results[5]["status"] == "new"        # e1 again but outside window — treated as new
    assert results[6]["status"] == "new"        # e4 first time

    # State should have been cleaned up — old entries evicted
    size = dedup.state_size()
    assert size <= 4, f"State should be bounded, got {size} entries"
    print("✅ S7 passed")


# ============================================================================
# S8. Streaming Join — Enrichment with Timeout
#
# Two streams: clicks and user_profiles.
# For each click, join with the user's profile to produce an enriched event.
# But profile events may arrive AFTER the click (eventual consistency).
#
# Rules:
# - If profile is already available, enrich immediately.
# - If profile not available, buffer the click for up to 30 seconds.
# - If profile arrives within 30s, emit enriched event.
# - If timeout expires, emit click with profile=None.
#
# This is a common pattern in stream processing (temporal join).
# ============================================================================

class StreamingEnricher:
    """
    Joins click events with user profile events.
    Profiles may arrive before or after clicks.
    """
    def __init__(self, timeout_seconds=30):
        self.timeout_seconds = timeout_seconds
        raise NotImplementedError
        pass

    def process_click(self, event):
        """
        event: {"click_id": str, "user_id": str, "timestamp": str}
        Returns: enriched event if profile available, else None (buffered).
        """
        raise NotImplementedError
        pass

    def process_profile(self, event):
        """
        event: {"user_id": str, "name": str, "segment": str, "timestamp": str}
        Returns: list of enriched events for any buffered clicks that can now be joined.
        """
        raise NotImplementedError
        pass

    def check_timeouts(self, current_time_str):
        """
        Emit any buffered clicks that have exceeded the timeout.
        Returns: list of {"click_id", "user_id", "profile": None, "status": "timeout"}
        """
        raise NotImplementedError
        pass


def test_s8():
    enricher = StreamingEnricher(timeout_seconds=30)

    # Profile arrives first for u1
    r1 = enricher.process_profile({"user_id": "u1", "name": "Alice", "segment": "premium", "timestamp": "2024-01-01T10:00:00"})
    assert r1 == []  # no buffered clicks to join

    # Click for u1 — profile available, should enrich immediately
    r2 = enricher.process_click({"click_id": "c1", "user_id": "u1", "timestamp": "2024-01-01T10:00:10"})
    assert r2 is not None
    assert r2["click_id"] == "c1"
    assert r2["profile"]["name"] == "Alice"

    # Click for u2 — no profile yet, should buffer
    r3 = enricher.process_click({"click_id": "c2", "user_id": "u2", "timestamp": "2024-01-01T10:00:15"})
    assert r3 is None  # buffered

    # Profile for u2 arrives — should emit buffered click
    r4 = enricher.process_profile({"user_id": "u2", "name": "Bob", "segment": "free", "timestamp": "2024-01-01T10:00:20"})
    assert len(r4) == 1
    assert r4[0]["click_id"] == "c2"
    assert r4[0]["profile"]["name"] == "Bob"

    # Click for u3 — no profile, will timeout
    r5 = enricher.process_click({"click_id": "c3", "user_id": "u3", "timestamp": "2024-01-01T10:01:00"})
    assert r5 is None  # buffered

    # Check timeouts at 10:01:35 — c3 has been waiting 35 seconds
    r6 = enricher.check_timeouts("2024-01-01T10:01:35")
    assert len(r6) == 1
    assert r6[0]["click_id"] == "c3"
    assert r6[0]["profile"] is None
    assert r6[0]["status"] == "timeout"

    print("✅ S8 passed")


# ============================================================================
# RUN ALL TESTS
# ============================================================================

if __name__ == "__main__":
    tests = [
        ("S1 - Streaming Status Changes", test_s1),
        ("S2 - Streaming Sessions", test_s2),
        ("S3 - Streaming Threshold", test_s3),
        ("S4 - Tumbling Windows", test_s4),
        ("S5 - Sliding Window Rate", test_s5),
        ("S6 - Watermarks & Late Events", test_s6),
        ("S7 - Streaming Dedup", test_s7),
        ("S8 - Streaming Join", test_s8),
    ]

    passed = 0
    failed = 0
    skipped = 0

    for name, test_fn in tests:
        try:
            test_fn()
            passed += 1
        except NotImplementedError:
            print(f"⏭️  {name} skipped (not implemented yet)")
            skipped += 1
        except AssertionError as e:
            print(f"❌ {name} failed: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ {name} error: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
