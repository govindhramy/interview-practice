"""
Data Engineering Interview Prep — Test Data & Test Cases
Copy-paste into CoderPad. Each problem has:
  - Sample data
  - Your function stub
  - Test runner with assertions
"""

from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================================
# P1. Group and Aggregate — total spend per user per category
# ============================================================================

P1_DATA = [
    {"user_id": "u1", "amount": 50.0, "category": "electronics"},
    {"user_id": "u1", "amount": 30.0, "category": "electronics"},
    {"user_id": "u1", "amount": 20.0, "category": "books"},
    {"user_id": "u2", "amount": 100.0, "category": "electronics"},
    {"user_id": "u2", "amount": 15.0, "category": "books"},
    {"user_id": "u3", "amount": 5.0, "category": "books"},
]

def group_and_aggregate(records):
    # TODO: return dict like {("u1", "electronics"): 80.0, ("u1", "books"): 20.0, ...}
    result = defaultdict(int)

    for r in records:
        if r["category"] is None:
            continue
        result[(r["user_id"], r["category"])] += r["amount"] or 0

    return result

# Edge cases — how would you handle these?
#
# What if amount is None or missing?
# What if a record has an empty string for category?
# What if amount is negative (refund)?
#
# Scaling: How does this change at 1 billion records in Spark?
# "Spark does a partial sum on each partition first, then shuffles only the partial results for the final aggregation —
# so network cost is proportional to the number of distinct (user, category) pairs, not the raw record count."

def test_p1():
    result = group_and_aggregate(P1_DATA)
    assert result[("u1", "electronics")] == 80.0, f"Expected 80.0, got {result[('u1', 'electronics')]}"
    assert result[("u1", "books")] == 20.0
    assert result[("u2", "electronics")] == 100.0
    assert result[("u2", "books")] == 15.0
    assert result[("u3", "books")] == 5.0
    assert len(result) == 5

    # Edge: empty input
    assert group_and_aggregate([]) == {}
    print("✅ P1 passed")


"""# ============================================================================
# P2. Sort with Tiebreakers — score desc, then earliest timestamp
# ============================================================================"""

P2_DATA = [
    {"name": "Alice", "score": 90, "timestamp": "2024-01-03T10:00:00"},
    {"name": "Bob", "score": 95, "timestamp": "2024-01-02T10:00:00"},
    {"name": "Charlie", "score": 90, "timestamp": "2024-01-01T10:00:00"},
    {"name": "Diana", "score": 95, "timestamp": "2024-01-01T10:00:00"},
    {"name": "Eve", "score": 85, "timestamp": "2024-01-01T10:00:00"},
]

def sort_with_tiebreakers(records):
    # TODO: return sorted list — score descending, timestamp ascending for ties
    return sorted(records, key=lambda r: (-r["score"],r["timestamp"]))

def test_p2():
    result = sort_with_tiebreakers(P2_DATA)
    names = [r["name"] for r in result]
    assert names == ["Diana", "Bob", "Charlie", "Alice", "Eve"], f"Got {names}"

    # Edge: single record
    assert len(sort_with_tiebreakers([P2_DATA[0]])) == 1

    # Edge: all same score
    same_score = [
        {"name": "A", "score": 50, "timestamp": "2024-01-03T10:00:00"},
        {"name": "B", "score": 50, "timestamp": "2024-01-01T10:00:00"},
    ]
    assert [r["name"] for r in sort_with_tiebreakers(same_score)] == ["B", "A"]
    print("✅ P2 passed")

# Edge cases:
#
# What if two records have the same score AND same timestamp — is the output stable?
# What if score is None?
# What if timestamps are in different formats (some with milliseconds, some without)?
#
# Scaling: This one is interesting — what happens when you need to sort 1B records in Spark? sample -> range-partition -> sort partitions


"""# ============================================================================
# P3. Deduplication — one record per email, earliest signup, prefer organic
# ============================================================================"""

P3_DATA = [
    {"email": "a@test.com", "signup_date": "2024-01-01", "source": "paid"},
    {"email": "a@test.com", "signup_date": "2024-01-01", "source": "organic"},
    {"email": "a@test.com", "signup_date": "2024-01-02", "source": "organic"},
    {"email": "b@test.com", "signup_date": "2024-01-05", "source": "paid"},
    {"email": "c@test.com", "signup_date": "2024-01-03", "source": "organic"},
    {"email": "c@test.com", "signup_date": "2024-01-01", "source": "paid"},
]

def deduplicate(records):
    # TODO: return list of one record per email
    grp = {}
    for r in records:
        if r["email"] not in grp:
            grp[r["email"]] = (r["signup_date"], r["source"])
        else:
            curr_date = grp[r["email"]][0]
            if r["signup_date"] < curr_date or (r["signup_date"] == curr_date and r["source"] == "organic"):
                grp[r["email"]] = (r["signup_date"], r["source"])

    return [{"email": email, "signup_date": signup_date, "source": source} for email,(signup_date, source) in grp.items()]

# Edge cases:

# What if email is None — do you group all null emails together?
# What if signup_date is null for some records?
# What if there's a third source like "referral" — does your tiebreaker logic still work?
# Case sensitivity — is "Organic" the same as "organic"?
#
# Scaling: instead of window fn that causes full shuffle we can do grp by which does partial agg bfr shuffle

def test_p3():
    result = {r["email"]: r for r in deduplicate(P3_DATA)}
    assert len(result) == 3
    assert result["a@test.com"]["source"] == "organic"
    assert result["a@test.com"]["signup_date"] == "2024-01-01"
    assert result["b@test.com"]["source"] == "paid"
    assert result["c@test.com"]["signup_date"] == "2024-01-01"
    assert result["c@test.com"]["source"] == "paid"  # earliest is paid, no tie

    # Edge: empty
    assert deduplicate([]) == []
    print("✅ P3 passed")


"""# ============================================================================
# P4. Merge Two Sorted Streams
# ============================================================================"""

P4_STREAM_A = [
    {"timestamp": "2024-01-01T10:00:00", "value": "a1"},
    {"timestamp": "2024-01-01T12:00:00", "value": "a2"},
    {"timestamp": "2024-01-01T14:00:00", "value": "a3"},
]

P4_STREAM_B = [
    {"timestamp": "2024-01-01T09:00:00", "value": "b1"},
    {"timestamp": "2024-01-01T12:00:00", "value": "b2"},  # collides with a2
    {"timestamp": "2024-01-01T13:00:00", "value": "b3"},
]

def merge_streams(stream_a, stream_b):
    # TODO: return merged sorted list, keep both on collision (stable order: a before b)
    i = 0
    j = 0

    res = []
    while i < len(stream_a) and j < len(stream_b):
        if stream_a[i]["timestamp"] <= stream_b[j]["timestamp"]:
            res.append(stream_a[i])
            i += 1
        else:
            res.append(stream_b[j])
            j += 1
    if i == len(stream_a):
        res.extend(stream_b[j:])
    else:
        res.extend(stream_a[i:])

    return res

# Edge cases:
#
# Both streams empty?
# One stream empty?
# What if timestamps are not sorted within a stream — does your code assume pre-sorted input?
# What if a record is missing the timestamp field?
#
# Scaling - in a distributed system, global sort is expensive and almost never needed —
# you sort within partitions for window operations, or you groupBy for aggregations.
# The merge-two-sorted-streams pattern is really a single-machine concept. for streaming,
# you'd union both streams into one Kafka topic (or consume both), and your processing framework
# handles the ordering via event-time windows and watermarks.

def test_p4():
    result = merge_streams(P4_STREAM_A, P4_STREAM_B)
    values = [r["value"] for r in result]
    assert values == ["b1", "a1", "a2", "b2", "b3", "a3"], f"Got {values}"

    # Edge: one empty stream
    assert merge_streams(P4_STREAM_A, []) == P4_STREAM_A
    assert merge_streams([], P4_STREAM_B) == P4_STREAM_B

    # Edge: both empty
    assert merge_streams([], []) == []
    print("✅ P4 passed")


"""# ============================================================================
# P5. Window Lookback — add prev_action per user
# ============================================================================"""

P5_DATA = [
    {"user_id": "u1", "action": "login", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u2", "action": "view", "timestamp": "2024-01-01T10:01:00"},
    {"user_id": "u1", "action": "click", "timestamp": "2024-01-01T10:02:00"},
    {"user_id": "u1", "action": "purchase", "timestamp": "2024-01-01T10:05:00"},
    {"user_id": "u2", "action": "click", "timestamp": "2024-01-01T10:06:00"},
]

def add_prev_action(records):
    # TODO: return list of records with added "prev_action" field (None if first for that user)
    # Input is already sorted by timestamp
    pass

def test_p5():
    result = add_prev_action(P5_DATA)
    assert result[0]["prev_action"] is None  # u1 first event
    assert result[1]["prev_action"] is None  # u2 first event
    assert result[2]["prev_action"] == "login"  # u1's second
    assert result[3]["prev_action"] == "click"  # u1's third
    assert result[4]["prev_action"] == "view"  # u2's second

    # Edge: single record
    single = [{"user_id": "u1", "action": "x", "timestamp": "2024-01-01T10:00:00"}]
    assert add_prev_action(single)[0]["prev_action"] is None
    print("✅ P5 passed")


# ============================================================================
# P6. Detect Status Changes
# ============================================================================

P6_DATA = [
    {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T11:00:00"},  # no change
    {"user_id": "u1", "status": "inactive", "timestamp": "2024-01-01T12:00:00"},  # change
    {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T13:00:00"},  # change
    {"user_id": "u2", "status": "active", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u2", "status": "active", "timestamp": "2024-01-01T11:00:00"},  # no change
]

P6_DATA_UNSORTED = [
    {"user_id": "u1", "status": "inactive", "timestamp": "2024-01-01T12:00:00"},
    {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T11:00:00"},
]

P6_DATA_MALFORMED = [
    {"user_id": "u1", "status": "active", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u1", "timestamp": "2024-01-01T11:00:00"},  # missing status
    {"user_id": "u1", "status": None, "timestamp": "2024-01-01T12:00:00"},  # null status
    {"user_id": "u1", "status": "inactive", "timestamp": "2024-01-01T13:00:00"},
]

def detect_status_changes(records):
    # TODO: return dict {user_id: [list of records where status changed]}
    # Include each user's first record as a "change"
    # Handle: unsorted input, missing/null status
    pass

def test_p6():
    result = detect_status_changes(P6_DATA)
    assert len(result["u1"]) == 3  # first(active), inactive, active
    assert len(result["u2"]) == 1  # only first(active)
    assert result["u1"][0]["status"] == "active"
    assert result["u1"][1]["status"] == "inactive"
    assert result["u1"][2]["status"] == "active"

    # Unsorted input should still work
    result2 = detect_status_changes(P6_DATA_UNSORTED)
    assert len(result2["u1"]) == 2  # active, then inactive

    # Malformed data — your choice how to handle, but shouldn't crash
    try:
        result3 = detect_status_changes(P6_DATA_MALFORMED)
        print("  (P6 malformed data handled without crash)")
    except Exception as e:
        print(f"  ⚠️  P6 crashed on malformed data: {e}")

    print("✅ P6 passed")


# ============================================================================
# P7. Session Builder — 30 min gap = new session
# ============================================================================

P7_DATA = [
    {"user_id": "u1", "event": "click", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u1", "event": "view", "timestamp": "2024-01-01T10:15:00"},
    {"user_id": "u1", "event": "click", "timestamp": "2024-01-01T10:25:00"},
    # 35 min gap — new session
    {"user_id": "u1", "event": "view", "timestamp": "2024-01-01T11:00:00"},
    {"user_id": "u2", "event": "click", "timestamp": "2024-01-01T10:00:00"},
    # single event session for u2
]

def build_sessions(records, gap_minutes=30):
    # TODO: return list of {"user_id", "session_id", "start", "end", "event_count"}
    pass

def test_p7():
    result = build_sessions(P7_DATA)
    u1_sessions = [s for s in result if s["user_id"] == "u1"]
    u2_sessions = [s for s in result if s["user_id"] == "u2"]

    assert len(u1_sessions) == 2, f"Expected 2 sessions for u1, got {len(u1_sessions)}"
    assert u1_sessions[0]["event_count"] == 3
    assert u1_sessions[0]["start"] == "2024-01-01T10:00:00"
    assert u1_sessions[0]["end"] == "2024-01-01T10:25:00"
    assert u1_sessions[1]["event_count"] == 1
    assert len(u2_sessions) == 1
    assert u2_sessions[0]["event_count"] == 1
    assert u2_sessions[0]["start"] == u2_sessions[0]["end"]  # single event

    # Edge: empty
    assert build_sessions([]) == []
    print("✅ P7 passed")


# ============================================================================
# P8. First and Last Per Group
# ============================================================================

P8_DATA = [
    {"device_id": "d1", "reading": 22.5, "timestamp": "2024-01-01T10:00:00"},
    {"device_id": "d1", "reading": 23.1, "timestamp": "2024-01-01T12:00:00"},
    {"device_id": "d1", "reading": None, "timestamp": "2024-01-01T11:00:00"},  # null reading, middle time
    {"device_id": "d2", "reading": 18.0, "timestamp": "2024-01-01T09:00:00"},
    {"device_id": "d1", "reading": 24.0, "timestamp": "2024-01-01T14:00:00"},
]

def first_last_per_device(records):
    # TODO: return dict {device_id: {"first_reading": ..., "last_reading": ..., "count": ...}}
    # "first" and "last" are by timestamp, even if data is unsorted
    # count includes all records (even null readings)
    pass

def test_p8():
    result = first_last_per_device(P8_DATA)
    assert result["d1"]["first_reading"] == 22.5
    assert result["d1"]["last_reading"] == 24.0
    assert result["d1"]["count"] == 4
    assert result["d2"]["first_reading"] == 18.0
    assert result["d2"]["last_reading"] == 18.0  # only one record
    assert result["d2"]["count"] == 1

    # Edge: single device single reading
    single = [{"device_id": "d9", "reading": 1.0, "timestamp": "2024-01-01T00:00:00"}]
    r = first_last_per_device(single)
    assert r["d9"]["first_reading"] == r["d9"]["last_reading"] == 1.0
    print("✅ P8 passed")


# ============================================================================
# P9. Running State Tracker — threshold crossings
# ============================================================================

P9_DATA = [
    {"machine_id": "m1", "metric": "temp", "value": 95, "timestamp": "2024-01-01T10:00:00"},
    {"machine_id": "m1", "metric": "temp", "value": 102, "timestamp": "2024-01-01T10:05:00"},  # crossed 100
    {"machine_id": "m1", "metric": "temp", "value": 98, "timestamp": "2024-01-01T10:10:00"},  # back below
    {"machine_id": "m1", "metric": "temp", "value": 105, "timestamp": "2024-01-01T10:15:00"},  # crossed again
    {"machine_id": "m1", "metric": "pressure", "value": 110, "timestamp": "2024-01-01T10:00:00"},  # already above
    {"machine_id": "m1", "metric": "pressure", "value": 90, "timestamp": "2024-01-01T10:05:00"},
    {"machine_id": "m1", "metric": "pressure", "value": 101, "timestamp": "2024-01-01T10:10:00"},  # crossed
]

P9_DATA_EDGE = [
    {"machine_id": "m2", "metric": "temp", "value": 100, "timestamp": "2024-01-01T10:00:00"},  # exactly at threshold
    {"machine_id": "m2", "metric": "temp", "value": 101, "timestamp": "2024-01-01T10:05:00"},  # above
    {"machine_id": "m2", "metric": "temp", "value": None, "timestamp": "2024-01-01T10:10:00"},  # missing value
    {"machine_id": "m2", "metric": "temp", "value": 99, "timestamp": "2024-01-01T10:15:00"},
]

def detect_threshold_crossings(records, threshold=100):
    # TODO: return list of {"machine_id", "metric", "crossed_at", "value"}
    # A crossing = value goes from <=threshold to >threshold
    # Handle out-of-order data (sort by timestamp first), missing values
    pass

def test_p9():
    result = detect_threshold_crossings(P9_DATA, threshold=100)
    temp_crossings = [r for r in result if r["metric"] == "temp"]
    pressure_crossings = [r for r in result if r["metric"] == "pressure"]

    assert len(temp_crossings) == 2, f"Expected 2 temp crossings, got {len(temp_crossings)}"
    assert temp_crossings[0]["value"] == 102
    assert temp_crossings[1]["value"] == 105
    assert len(pressure_crossings) == 1  # only the 90->101 crossing (first record already above doesn't count)

    # Edge: exact threshold, missing values
    result2 = detect_threshold_crossings(P9_DATA_EDGE, threshold=100)
    # 100 -> 101 is a crossing (at to above)? Depends on your definition.
    # Accept either 0 or 1 crossings here — just make sure it doesn't crash.
    try:
        assert isinstance(result2, list)
        print("  (P9 edge cases handled)")
    except Exception as e:
        print(f"  ⚠️  P9 edge case issue: {e}")

    print("✅ P9 passed")


# ============================================================================
# P10. Time Gap Detector — sensor heartbeat gaps > 5 min
# ============================================================================

P10_DATA = [
    {"sensor_id": "s1", "timestamp": "2024-01-01T10:00:00"},
    {"sensor_id": "s1", "timestamp": "2024-01-01T10:03:00"},
    {"sensor_id": "s1", "timestamp": "2024-01-01T10:12:00"},  # 9 min gap from 10:03
    {"sensor_id": "s1", "timestamp": "2024-01-01T10:14:00"},
    {"sensor_id": "s2", "timestamp": "2024-01-01T10:00:00"},
    {"sensor_id": "s2", "timestamp": "2024-01-01T10:01:00"},
    # s3 has only one record
    {"sensor_id": "s3", "timestamp": "2024-01-01T10:00:00"},
]

P10_DATA_UNSORTED = [
    {"sensor_id": "s4", "timestamp": "2024-01-01T10:20:00"},
    {"sensor_id": "s4", "timestamp": "2024-01-01T10:00:00"},
    {"sensor_id": "s4", "timestamp": "2024-01-01T10:02:00"},
    # gap between 10:02 and 10:20 = 18 min
]

def detect_gaps(records, max_gap_seconds=300):
    # TODO: return list of {"sensor_id", "gap_start", "gap_end", "gap_duration_sec"}
    pass

def test_p10():
    result = detect_gaps(P10_DATA)
    s1_gaps = [g for g in result if g["sensor_id"] == "s1"]
    s2_gaps = [g for g in result if g["sensor_id"] == "s2"]
    s3_gaps = [g for g in result if g["sensor_id"] == "s3"]

    assert len(s1_gaps) == 1, f"Expected 1 gap for s1, got {len(s1_gaps)}"
    assert s1_gaps[0]["gap_start"] == "2024-01-01T10:03:00"
    assert s1_gaps[0]["gap_end"] == "2024-01-01T10:12:00"
    assert s1_gaps[0]["gap_duration_sec"] == 540
    assert len(s2_gaps) == 0
    assert len(s3_gaps) == 0  # single record, no gap

    # Unsorted
    result2 = detect_gaps(P10_DATA_UNSORTED)
    assert len(result2) == 1
    assert result2[0]["gap_duration_sec"] == 1080  # 18 min
    print("✅ P10 passed")


# ============================================================================
# P11. Event Funnel
# ============================================================================

FUNNEL_STEPS = ["page_view", "add_to_cart", "checkout", "purchase"]

P11_DATA = [
    # u1: completes full funnel
    {"user_id": "u1", "event_type": "page_view", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u1", "event_type": "add_to_cart", "timestamp": "2024-01-01T10:05:00"},
    {"user_id": "u1", "event_type": "checkout", "timestamp": "2024-01-01T10:10:00"},
    {"user_id": "u1", "event_type": "purchase", "timestamp": "2024-01-01T10:15:00"},
    # u2: only gets to add_to_cart
    {"user_id": "u2", "event_type": "page_view", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u2", "event_type": "add_to_cart", "timestamp": "2024-01-01T10:05:00"},
    # u3: skips add_to_cart (goes page_view -> checkout) — checkout shouldn't count
    {"user_id": "u3", "event_type": "page_view", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u3", "event_type": "checkout", "timestamp": "2024-01-01T10:05:00"},
    # u4: out of order events — purchase before checkout timestamp
    {"user_id": "u4", "event_type": "page_view", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u4", "event_type": "add_to_cart", "timestamp": "2024-01-01T10:05:00"},
    {"user_id": "u4", "event_type": "purchase", "timestamp": "2024-01-01T10:06:00"},
    {"user_id": "u4", "event_type": "checkout", "timestamp": "2024-01-01T10:10:00"},
    # u5: repeated steps
    {"user_id": "u5", "event_type": "page_view", "timestamp": "2024-01-01T10:00:00"},
    {"user_id": "u5", "event_type": "page_view", "timestamp": "2024-01-01T10:01:00"},
    {"user_id": "u5", "event_type": "add_to_cart", "timestamp": "2024-01-01T10:05:00"},
]

def compute_funnel(records, steps=FUNNEL_STEPS):
    # TODO: return dict {user_id: {"furthest_step": str, "completed_funnel": bool}}
    # A step only counts if it happened AFTER the previous step in the funnel
    pass

def test_p11():
    result = compute_funnel(P11_DATA)
    assert result["u1"]["furthest_step"] == "purchase"
    assert result["u1"]["completed_funnel"] == True
    assert result["u2"]["furthest_step"] == "add_to_cart"
    assert result["u2"]["completed_funnel"] == False
    assert result["u3"]["furthest_step"] == "page_view"  # checkout doesn't count (skipped add_to_cart)
    assert result["u3"]["completed_funnel"] == False
    assert result["u4"]["furthest_step"] == "add_to_cart"  # purchase came before checkout
    assert result["u4"]["completed_funnel"] == False
    assert result["u5"]["furthest_step"] == "add_to_cart"  # repeated page_views are fine
    print("✅ P11 passed")


# ============================================================================
# P12. SLA Breach Detector
# ============================================================================

P12_DATA = [
    # ticket1: resolved in 2 hours — no breach
    {"ticket_id": "t1", "status": "opened", "timestamp": "2024-01-01T10:00:00"},
    {"ticket_id": "t1", "status": "assigned", "timestamp": "2024-01-01T10:30:00"},
    {"ticket_id": "t1", "status": "resolved", "timestamp": "2024-01-01T12:00:00"},
    # ticket2: resolved in 5 hours — breach
    {"ticket_id": "t2", "status": "opened", "timestamp": "2024-01-01T08:00:00"},
    {"ticket_id": "t2", "status": "resolved", "timestamp": "2024-01-01T13:00:00"},
    # ticket3: never resolved
    {"ticket_id": "t3", "status": "opened", "timestamp": "2024-01-01T09:00:00"},
    {"ticket_id": "t3", "status": "assigned", "timestamp": "2024-01-01T09:30:00"},
    # ticket4: duplicate statuses
    {"ticket_id": "t4", "status": "opened", "timestamp": "2024-01-01T10:00:00"},
    {"ticket_id": "t4", "status": "opened", "timestamp": "2024-01-01T10:01:00"},  # dup
    {"ticket_id": "t4", "status": "resolved", "timestamp": "2024-01-01T11:00:00"},
    # ticket5: missing opened event
    {"ticket_id": "t5", "status": "assigned", "timestamp": "2024-01-01T10:00:00"},
    {"ticket_id": "t5", "status": "resolved", "timestamp": "2024-01-01T11:00:00"},
]

def detect_sla_breaches(records, sla_hours=4):
    # TODO: return list of {"ticket_id", "open_time", "resolve_time", "duration_hrs", "breached": bool}
    # Handle: unresolved tickets, missing opened event, duplicate statuses
    pass

def test_p12():
    result = {r["ticket_id"]: r for r in detect_sla_breaches(P12_DATA)}

    assert result["t1"]["breached"] == False
    assert result["t1"]["duration_hrs"] == 2.0
    assert result["t2"]["breached"] == True
    assert result["t2"]["duration_hrs"] == 5.0

    # t3: unresolved — either exclude or mark specially, both acceptable
    # Just make sure it doesn't crash
    assert "t4" in result
    assert result["t4"]["breached"] == False

    # t5: no opened event — should handle gracefully
    # Either exclude or handle — just no crash
    print("✅ P12 passed")


# ============================================================================
# P13. Daily Active Users with Retention
# ============================================================================

P13_DATA = [
    {"user_id": "u1", "event": "click", "date": "2024-01-01"},
    {"user_id": "u2", "event": "click", "date": "2024-01-01"},
    {"user_id": "u3", "event": "click", "date": "2024-01-01"},
    {"user_id": "u1", "event": "click", "date": "2024-01-01"},  # dup same day
    {"user_id": "u1", "event": "view", "date": "2024-01-02"},
    {"user_id": "u2", "event": "click", "date": "2024-01-02"},
    {"user_id": "u4", "event": "click", "date": "2024-01-02"},
    {"user_id": "u4", "event": "view", "date": "2024-01-03"},
]

def compute_dau_retention(records):
    # TODO: return list of {"date", "dau", "d1_retention_pct"}
    # d1_retention_pct = % of today's users who are also active tomorrow
    # Last day's retention can be None (no next day data)
    pass

def test_p13():
    result = {r["date"]: r for r in compute_dau_retention(P13_DATA)}

    assert result["2024-01-01"]["dau"] == 3  # u1, u2, u3
    assert result["2024-01-02"]["dau"] == 3  # u1, u2, u4

    # Day 1 retention: u1 and u2 came back on day 2, u3 didn't → 2/3 ≈ 66.67%
    assert abs(result["2024-01-01"]["d1_retention_pct"] - 66.67) < 1.0

    # Day 2 retention: u4 came back on day 3, u1 and u2 didn't → 1/3 ≈ 33.33%
    assert abs(result["2024-01-02"]["d1_retention_pct"] - 33.33) < 1.0

    # Day 3: last day, no next-day data
    assert result["2024-01-03"]["d1_retention_pct"] is None
    assert result["2024-01-03"]["dau"] == 1
    print("✅ P13 passed")


# ============================================================================
# P14. Change Data Capture Replay
# ============================================================================

P14_DATA = [
    {"table": "users", "pk": "1", "op": "insert", "data": {"name": "Alice", "email": "a@test.com"}, "timestamp": "2024-01-01T10:00:00"},
    {"table": "users", "pk": "2", "op": "insert", "data": {"name": "Bob", "email": "b@test.com"}, "timestamp": "2024-01-01T10:01:00"},
    {"table": "users", "pk": "1", "op": "update", "data": {"name": "Alice", "email": "alice@new.com"}, "timestamp": "2024-01-01T10:05:00"},
    {"table": "users", "pk": "2", "op": "delete", "data": None, "timestamp": "2024-01-01T10:10:00"},
    {"table": "orders", "pk": "100", "op": "insert", "data": {"user_id": "1", "total": 50}, "timestamp": "2024-01-01T10:02:00"},
]

P14_DATA_EDGE = [
    # update before insert (late arrival)
    {"table": "users", "pk": "3", "op": "update", "data": {"name": "Charlie", "email": "c@test.com"}, "timestamp": "2024-01-01T10:05:00"},
    {"table": "users", "pk": "3", "op": "insert", "data": {"name": "Charles", "email": "c@old.com"}, "timestamp": "2024-01-01T10:00:00"},
    # double delete
    {"table": "users", "pk": "4", "op": "insert", "data": {"name": "Diana"}, "timestamp": "2024-01-01T10:00:00"},
    {"table": "users", "pk": "4", "op": "delete", "data": None, "timestamp": "2024-01-01T10:01:00"},
    {"table": "users", "pk": "4", "op": "delete", "data": None, "timestamp": "2024-01-01T10:02:00"},
]

def replay_cdc(records):
    # TODO: return {table_name: {pk: current_row_data}}
    # deleted rows should not appear in output
    # sort by timestamp before replaying
    pass

def test_p14():
    result = replay_cdc(P14_DATA)
    assert result["users"]["1"] == {"name": "Alice", "email": "alice@new.com"}
    assert "2" not in result["users"]  # deleted
    assert result["orders"]["100"] == {"user_id": "1", "total": 50}

    # Edge cases
    result2 = replay_cdc(P14_DATA_EDGE)
    # pk 3: insert at 10:00, update at 10:05 → final state is update
    assert result2["users"]["3"] == {"name": "Charlie", "email": "c@test.com"}
    # pk 4: inserted then deleted twice — should not exist
    assert "4" not in result2["users"]
    print("✅ P14 passed")


# ============================================================================
# P15. Slowly Changing Dimension Type 2
# ============================================================================

P15_DATA = [
    {"employee_id": "e1", "department": "Engineering", "effective_date": "2023-01-01"},
    {"employee_id": "e1", "department": "Product", "effective_date": "2023-06-01"},
    {"employee_id": "e1", "department": "Engineering", "effective_date": "2024-01-01"},
    {"employee_id": "e2", "department": "Sales", "effective_date": "2023-03-01"},
]

P15_DATA_EDGE = [
    # Out of order
    {"employee_id": "e3", "department": "HR", "effective_date": "2024-01-01"},
    {"employee_id": "e3", "department": "Finance", "effective_date": "2023-01-01"},
    # Same-day change
    {"employee_id": "e4", "department": "A", "effective_date": "2023-06-01"},
    {"employee_id": "e4", "department": "B", "effective_date": "2023-06-01"},
]

def build_scd2(records):
    # TODO: return list of {"employee_id", "department", "start_date", "end_date", "is_current"}
    # end_date = None and is_current = True for latest record
    # Sort by effective_date before processing
    pass

def test_p15():
    result = build_scd2(P15_DATA)
    e1 = sorted([r for r in result if r["employee_id"] == "e1"], key=lambda x: x["start_date"])
    e2 = [r for r in result if r["employee_id"] == "e2"]

    assert len(e1) == 3
    assert e1[0]["department"] == "Engineering"
    assert e1[0]["end_date"] == "2023-06-01"
    assert e1[0]["is_current"] == False
    assert e1[1]["department"] == "Product"
    assert e1[1]["end_date"] == "2024-01-01"
    assert e1[2]["department"] == "Engineering"
    assert e1[2]["end_date"] is None
    assert e1[2]["is_current"] == True

    assert len(e2) == 1
    assert e2[0]["is_current"] == True
    assert e2[0]["end_date"] is None

    # Edge: out of order should produce correct results
    result2 = build_scd2(P15_DATA_EDGE)
    e3 = sorted([r for r in result2 if r["employee_id"] == "e3"], key=lambda x: x["start_date"])
    assert e3[0]["department"] == "Finance"
    assert e3[1]["department"] == "HR"
    assert e3[1]["is_current"] == True
    print("✅ P15 passed")


# ============================================================================
# P16. Rate Limiter Check — sliding window
# ============================================================================

# Generate burst data: 101 requests in 50 seconds for one key
P16_DATA = []
base_time = datetime(2024, 1, 1, 10, 0, 0)
for i in range(101):
    P16_DATA.append({
        "api_key": "key1",
        "endpoint": "/api/data",
        "timestamp": (base_time + timedelta(seconds=i * 0.5)).isoformat(),
    })
# Add non-breaching key
for i in range(50):
    P16_DATA.append({
        "api_key": "key2",
        "endpoint": "/api/data",
        "timestamp": (base_time + timedelta(seconds=i * 2)).isoformat(),
    })
# Add a second endpoint for key1 (no breach)
for i in range(10):
    P16_DATA.append({
        "api_key": "key1",
        "endpoint": "/api/health",
        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
    })

def detect_rate_breaches(records, max_requests=100, window_seconds=60):
    # TODO: return list of {"api_key", "endpoint", "window_start", "request_count"} for breaches
    pass

def test_p16():
    result = detect_rate_breaches(P16_DATA)
    key1_data = [r for r in result if r["api_key"] == "key1" and r["endpoint"] == "/api/data"]
    key2_data = [r for r in result if r["api_key"] == "key2"]
    key1_health = [r for r in result if r["api_key"] == "key1" and r["endpoint"] == "/api/health"]

    assert len(key1_data) >= 1, "key1 /api/data should have at least 1 breach"
    assert len(key2_data) == 0, "key2 should have no breaches"
    assert len(key1_health) == 0, "key1 /api/health should have no breaches"
    assert all(r["request_count"] > 100 for r in key1_data)
    print("✅ P16 passed")


# ============================================================================
# P17. Log-Level Anomaly Spotter
# ============================================================================

P17_DATA = []
# Service A: normal hours with ~10% error rate, one spike hour
base = datetime(2024, 1, 1, 0, 0, 0)
for hour in range(24):
    for i in range(90):
        P17_DATA.append({
            "service": "svc-a",
            "log_level": "INFO",
            "timestamp": (base + timedelta(hours=hour, minutes=i % 60)).isoformat(),
        })
    error_count = 10 if hour != 14 else 50  # hour 14 has spike
    for i in range(error_count):
        P17_DATA.append({
            "service": "svc-a",
            "log_level": "ERROR",
            "timestamp": (base + timedelta(hours=hour, minutes=30 + i % 30)).isoformat(),
        })

# Service B: zero errors
for hour in range(24):
    for i in range(100):
        P17_DATA.append({
            "service": "svc-b",
            "log_level": "INFO",
            "timestamp": (base + timedelta(hours=hour, minutes=i % 60)).isoformat(),
        })

def detect_log_anomalies(records):
    # TODO: return list of {"service", "hour", "error_rate", "avg_error_rate", "is_anomaly": bool}
    # anomaly = hourly error rate > 2x service's overall average error rate
    pass

def test_p17():
    result = detect_log_anomalies(P17_DATA)
    svc_a_anomalies = [r for r in result if r["service"] == "svc-a" and r["is_anomaly"]]
    svc_b_anomalies = [r for r in result if r["service"] == "svc-b" and r["is_anomaly"]]

    # Hour 14 should be flagged for svc-a
    anomaly_hours = [r["hour"] for r in svc_a_anomalies]
    assert 14 in anomaly_hours, f"Hour 14 should be anomaly, got {anomaly_hours}"

    # svc-b has zero errors — should have no anomalies
    assert len(svc_b_anomalies) == 0

    print("✅ P17 passed")


# ============================================================================
# RUN ALL TESTS
# ============================================================================

if __name__ == "__main__":
    tests = [
        ("P1", test_p1), ("P2", test_p2), ("P3", test_p3), ("P4", test_p4),
        ("P5", test_p5), ("P6", test_p6), ("P7", test_p7), ("P8", test_p8),
        ("P9", test_p9), ("P10", test_p10), ("P11", test_p11), ("P12", test_p12),
        ("P13", test_p13), ("P14", test_p14), ("P15", test_p15), ("P16", test_p16),
        ("P17", test_p17),
    ]

    passed = 0
    failed = 0
    skipped = 0

    for name, test_fn in tests:
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"❌ {name} failed: {e}")
            failed += 1
        except TypeError as e:
            if "NoneType" in str(e):
                print(f"⏭️  {name} skipped (not implemented yet)")
                skipped += 1
            else:
                print(f"❌ {name} error: {e}")
                failed += 1
        except Exception as e:
            print(f"❌ {name} error: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
