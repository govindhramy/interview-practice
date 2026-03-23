-- ============================================================================
-- P1. Group and Aggregate — total spend per user per category
-- Expected:
--   u1 | electronics | 80.0
--   u1 | books       | 20.0
--   u2 | electronics | 100.0
--   u2 | books       | 15.0
--   u3 | books       | 5.0
-- ============================================================================

DROP TABLE IF EXISTS p1_purchases;
CREATE TABLE p1_purchases (
    user_id TEXT,
    amount NUMERIC,
    category TEXT
);
INSERT INTO p1_purchases VALUES
    ('u1', 50.0, 'electronics'),
    ('u1', 30.0, 'electronics'),
    ('u1', 20.0, 'books'),
    ('u2', 100.0, 'electronics'),
    ('u2', 15.0, 'books'),
    ('u3', 5.0, 'books');

-- TODO: write your query
SELECT user_id, category, sum(amount)
FROM p1_purchases 
group by 1,2
;


-- ============================================================================
-- P2. Sort with Tiebreakers — score desc, then earliest timestamp
-- Expected order: Diana, Bob, Charlie, Alice, Eve
-- ============================================================================

DROP TABLE IF EXISTS p2_scores;
CREATE TABLE p2_scores (
    name TEXT,
    score INT,
    ts TIMESTAMP
);
INSERT INTO p2_scores VALUES
    ('Alice',   90, '2024-01-03 10:00:00'),
    ('Bob',     95, '2024-01-02 10:00:00'),
    ('Charlie', 90, '2024-01-01 10:00:00'),
    ('Diana',   95, '2024-01-01 10:00:00'),
    ('Eve',     85, '2024-01-01 10:00:00');

-- TODO: write your query
SELECT *
FROM p2_scores 
order by score desc, ts asc
;


-- ============================================================================
-- P3. Deduplication — one record per email, earliest signup, prefer organic on tie
-- Expected:
--   a@test.com | 2024-01-01 | organic
--   b@test.com | 2024-01-05 | paid
--   c@test.com | 2024-01-01 | paid
-- ============================================================================

DROP TABLE IF EXISTS p3_signups;
CREATE TABLE p3_signups (
    email TEXT,
    signup_date DATE,
    source TEXT
);
INSERT INTO p3_signups VALUES
    ('a@test.com', '2024-01-01', 'paid'),
    ('a@test.com', '2024-01-01', 'organic'),
    ('a@test.com', '2024-01-02', 'organic'),
    ('b@test.com', '2024-01-05', 'paid'),
    ('c@test.com', '2024-01-03', 'organic'),
    ('c@test.com', '2024-01-01', 'paid');

-- TODO: write your query (hint: ROW_NUMBER with careful ORDER BY)
with ranked as (
  SELECT 
    *,
    RANK() over (PARTITION BY email order by signup_date, case when source = 'organic' then 0 else 1 end) as rnk
  FROM p3_signups
)
SELECT email, signup_date, source
FROM ranked
WHERE rnk = 1
;


-- ============================================================================
-- P4. Merge Two Sorted Streams
-- Expected order by timestamp (a before b on tie):
--   b1, a1, a2, b2, b3, a3
-- ============================================================================

DROP TABLE IF EXISTS p4_stream_a;
DROP TABLE IF EXISTS p4_stream_b;
CREATE TABLE p4_stream_a (ts TIMESTAMP, value TEXT);
CREATE TABLE p4_stream_b (ts TIMESTAMP, value TEXT);
INSERT INTO p4_stream_a VALUES
    ('2024-01-01 10:00:00', 'a1'),
    ('2024-01-01 12:00:00', 'a2'),
    ('2024-01-01 14:00:00', 'a3');
INSERT INTO p4_stream_b VALUES
    ('2024-01-01 09:00:00', 'b1'),
    ('2024-01-01 12:00:00', 'b2'),
    ('2024-01-01 13:00:00', 'b3');

-- TODO: write your query (hint: UNION ALL + ORDER BY with stream source for tiebreak)
select ts, "value"
from (
  SELECT *, 'a' as stream
  from p4_stream_a
  union ALL
  SELECT *, 'b' as stream
  from p4_stream_b
) merged
order by ts, stream
;

-- ============================================================================
-- P5. Window Lookback — add prev_action per user
-- Expected:
--   u1 | login    | NULL
--   u2 | view     | NULL
--   u1 | click    | login
--   u1 | purchase | click
--   u2 | click    | view
-- ============================================================================

DROP TABLE IF EXISTS p5_actions;
CREATE TABLE p5_actions (
    user_id TEXT,
    action TEXT,
    ts TIMESTAMP
);
INSERT INTO p5_actions VALUES
    ('u1', 'login',    '2024-01-01 10:00:00'),
    ('u2', 'view',     '2024-01-01 10:01:00'),
    ('u1', 'click',    '2024-01-01 10:02:00'),
    ('u1', 'purchase', '2024-01-01 10:05:00'),
    ('u2', 'click',    '2024-01-01 10:06:00');

-- TODO: write your query (hint: LAG window function)
SELECT 
  user_id,
  "action",
  lag("action") over (partition by user_id order by ts) as prev_action

FROM p5_actions
;


-- ============================================================================
-- P6. Detect Status Changes — only rows where status differs from previous
-- Expected for u1: active(10:00), inactive(12:00), active(13:00)
-- Expected for u2: active(10:00) only
-- ============================================================================

DROP TABLE IF EXISTS p6_statuses;
CREATE TABLE p6_statuses (
    user_id TEXT,
    status TEXT,
    ts TIMESTAMP
);
INSERT INTO p6_statuses VALUES
    ('u1', 'active',   '2024-01-01 10:00:00'),
    ('u1', 'active',   '2024-01-01 11:00:00'),
    ('u1', 'inactive', '2024-01-01 12:00:00'),
    ('u1', 'active',   '2024-01-01 13:00:00'),
    ('u2', 'active',   '2024-01-01 10:00:00'),
    ('u2', 'active',   '2024-01-01 11:00:00');

-- Edge case data: unsorted, malformed
INSERT INTO p6_statuses VALUES
    ('u3', 'inactive', '2024-01-01 12:00:00'),
    ('u3', 'active',   '2024-01-01 10:00:00'),
    ('u3', 'active',   '2024-01-01 11:00:00');
-- u3 expected: active(10:00), inactive(12:00)

-- TODO: write your query (hint: LAG + filter where status != prev_status OR prev is NULL)
with prev as (
  SELECT
    *,
    lag(status) over (partition by user_id order by ts) as prev_status
  FROM p6_statuses
)
select user_id, status, ts from prev where status <> prev_status or prev_status is null
;


-- ============================================================================
-- P7. Session Builder — new session if >30 min gap
-- Expected:
--   u1 | session 1 | 10:00 - 10:25 | 3 events
--   u1 | session 2 | 11:00 - 11:00 | 1 event
--   u2 | session 1 | 10:00 - 10:00 | 1 event
-- ============================================================================

DROP TABLE IF EXISTS p7_events;
CREATE TABLE p7_events (
    user_id TEXT,
    event TEXT,
    ts TIMESTAMP
);
INSERT INTO p7_events VALUES
    ('u1', 'click', '2024-01-01 10:00:00'),
    ('u1', 'view',  '2024-01-01 10:15:00'),
    ('u1', 'click', '2024-01-01 10:25:00'),
    ('u1', 'view',  '2024-01-01 11:00:00'),
    ('u2', 'click', '2024-01-01 10:00:00');

-- TODO: write your query
-- (hint: LAG to get prev timestamp, flag new session if gap > 30 min,
--  SUM the flags as running count to assign session_id, then GROUP BY)
with flag as (
  SELECT 
    *,
    case when extract(minute from (ts - lag(ts) over (partition by user_id order by ts))) > 30 then 1 else 0 end as ses_flag
  FROM p7_events
), ses as (
  select 
    user_id, 
    "event", 
    ts,
    sum(ses_flag) over (partition by user_id order by ts) + 1 as ses_id
  from Flag
)
select 
  user_id,
  ses_id,
  max(ts) - min(ts) as ses_time,
  count(*) as event_cnt
from ses
group by 1,2
order by 1,2
;


-- ============================================================================
-- P8. First and Last Per Group
-- Expected:
--   d1 | first_reading=22.5 | last_reading=24.0 | count=4
--   d2 | first_reading=18.0 | last_reading=18.0 | count=1
-- ============================================================================

DROP TABLE IF EXISTS p8_readings;
CREATE TABLE p8_readings (
    device_id TEXT,
    reading NUMERIC,
    ts TIMESTAMP
);
INSERT INTO p8_readings VALUES
    ('d1', 22.5, '2024-01-01 10:00:00'),
    ('d1', 23.1, '2024-01-01 12:00:00'),
    ('d1', NULL, '2024-01-01 11:00:00'),
    ('d2', 18.0, '2024-01-01 09:00:00'),
    ('d1', 24.0, '2024-01-01 14:00:00');

-- TODO: write your query
-- (hint: FIRST_VALUE / LAST_VALUE window functions, or subquery with MIN/MAX ts)
with windowed as (
  SELECT 
    device_id,
    FIRST_VALUE(reading) over (partition by device_id order by ts) as first_reading,
    LAST_VALUE(reading) over (partition by device_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_reading
  FROM p8_readings
)
select 
  device_id,
  first_reading,
  last_reading,
  count(1) as cnt
from windowed
group by 1,2,3
;


-- ============================================================================
-- P9. Running State Tracker — threshold crossings
-- A crossing = value goes from <=100 to >100
-- Expected crossings:
--   m1 | temp     | 10:05 | value=102 (was 95)
--   m1 | temp     | 10:15 | value=105 (was 98)
--   m1 | pressure | 10:10 | value=101 (was 90)
-- ============================================================================

DROP TABLE IF EXISTS p9_metrics;
CREATE TABLE p9_metrics (
    machine_id TEXT,
    metric TEXT,
    value NUMERIC,
    ts TIMESTAMP
);
INSERT INTO p9_metrics VALUES
    ('m1', 'temp',     95,  '2024-01-01 10:00:00'),
    ('m1', 'temp',     102, '2024-01-01 10:05:00'),
    ('m1', 'temp',     98,  '2024-01-01 10:10:00'),
    ('m1', 'temp',     105, '2024-01-01 10:15:00'),
    ('m1', 'pressure', 110, '2024-01-01 10:00:00'),
    ('m1', 'pressure', 90,  '2024-01-01 10:05:00'),
    ('m1', 'pressure', 101, '2024-01-01 10:10:00');

-- TODO: write your query (hint: LAG to get prev_value, filter where prev <= 100 AND current > 100)
with crossings as (
  SELECT 
    *,
    case when value > 100 and lag("value") over (partition by metric order by ts) <= 100 then true else false end as crossing 
  FROM p9_metrics 
)
select machine_id, metric , value, ts
from crossings
where crossing = true
;


-- ============================================================================
-- P10. Time Gap Detector — sensor gaps > 5 minutes
-- Expected:
--   s1 | gap_start=10:03 | gap_end=10:12 | gap_sec=540
-- ============================================================================

DROP TABLE IF EXISTS p10_heartbeats;
CREATE TABLE p10_heartbeats (
    sensor_id TEXT,
    ts TIMESTAMP
);
INSERT INTO p10_heartbeats VALUES
    ('s1', '2024-01-01 10:00:00'),
    ('s1', '2024-01-01 10:03:00'),
    ('s1', '2024-01-01 10:12:00'),
    ('s1', '2024-01-01 10:14:00'),
    ('s2', '2024-01-01 10:00:00'),
    ('s2', '2024-01-01 10:01:00'),
    ('s3', '2024-01-01 10:00:00');

-- TODO: write your query (hint: LAG + EXTRACT(EPOCH FROM ...) for seconds diff)
with lag as (
  SELECT 
    *,
    lag(ts) over (partition by sensor_id order by ts) as prev_ts
  FROM p10_heartbeats
)
select 
  sensor_id,
  prev_ts as gap_start,
  ts as gap_end
from lag 
where ts - prev_ts > interval '5' MINUTE
;


-- ============================================================================
-- P11. Event Funnel
-- Steps: page_view → add_to_cart → checkout → purchase
-- A step counts only if it happened AFTER the previous step
-- Expected:
--   u1 | purchase     | completed=true
--   u2 | add_to_cart  | completed=false
--   u3 | page_view    | completed=false  (skipped add_to_cart)
--   u4 | add_to_cart  | completed=false  (purchase before checkout)
--   u5 | add_to_cart  | completed=false
-- ============================================================================

DROP TABLE IF EXISTS p11_funnel;
CREATE TABLE p11_funnel (
    user_id TEXT,
    event_type TEXT,
    ts TIMESTAMP
);
INSERT INTO p11_funnel VALUES
    ('u1', 'page_view',    '2024-01-01 10:00:00'),
    ('u1', 'add_to_cart',  '2024-01-01 10:05:00'),
    ('u1', 'checkout',     '2024-01-01 10:10:00'),
    ('u1', 'purchase',     '2024-01-01 10:15:00'),
    ('u2', 'page_view',    '2024-01-01 10:00:00'),
    ('u2', 'add_to_cart',  '2024-01-01 10:05:00'),
    ('u3', 'page_view',    '2024-01-01 10:00:00'),
    ('u3', 'checkout',     '2024-01-01 10:05:00'),
    ('u4', 'page_view',    '2024-01-01 10:00:00'),
    ('u4', 'add_to_cart',  '2024-01-01 10:05:00'),
    ('u4', 'purchase',     '2024-01-01 10:06:00'),
    ('u4', 'checkout',     '2024-01-01 10:10:00'),
    ('u5', 'page_view',    '2024-01-01 10:00:00'),
    ('u5', 'page_view',    '2024-01-01 10:01:00'),
    ('u5', 'add_to_cart',  '2024-01-01 10:05:00');

-- TODO: write your query
-- (hint: get MIN timestamp per user per step, then self-join or use
--  conditional aggregation to check each step happened after previous)
with prev as (
  SELECT
    *,
    lag(event_type) over (partition by user_id order by ts) as prev_event
  FROM p11_funnel
), validity as (
  SELECT
    *,
    sum( 
      case
        when 
          (event_type = 'page_view' and (prev_event is null or prev_event = 'page_view'))
          or (event_type = 'add_to_cart' and prev_event in ('page_view','add_to_cart'))
          or (event_type = 'checkout' and prev_event = 'add_to_cart')
          or (event_type = 'purchase' and prev_event = 'checkout')
        then 0 
        else 1
      end 
    ) over (partition by user_id order by ts) as grp
    from prev
), final as (
  select 
    user_id, 
    LAST_VALUE(event_type) over (partition by user_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_valid_event
  from validity
  where grp = 0
  )
select 
  user_id,
  last_valid_event,
  case 
    when last_valid_event = 'purchase' then 'completed=true'
    else 'completed=false'
  end
from final
group by 1,2
;


-- ============================================================================
-- P12. SLA Breach Detector — opened→resolved > 4 hours = breach
-- Expected:
--   t1 | 2hrs  | breached=false
--   t2 | 5hrs  | breached=true
--   t3 | unresolved
--   t4 | 1hr   | breached=false
--   t5 | no opened event
-- ============================================================================

DROP TABLE IF EXISTS p12_tickets;
CREATE TABLE p12_tickets (
    ticket_id TEXT,
    status TEXT,
    ts TIMESTAMP
);
INSERT INTO p12_tickets VALUES
    ('t1', 'opened',   '2024-01-01 10:00:00'),
    ('t1', 'assigned', '2024-01-01 10:30:00'),
    ('t1', 'resolved', '2024-01-01 12:00:00'),
    ('t2', 'opened',   '2024-01-01 08:00:00'),
    ('t2', 'resolved', '2024-01-01 13:00:00'),
    ('t3', 'opened',   '2024-01-01 09:00:00'),
    ('t3', 'assigned', '2024-01-01 09:30:00'),
    ('t4', 'opened',   '2024-01-01 10:00:00'),
    ('t4', 'opened',   '2024-01-01 10:01:00'),
    ('t4', 'resolved', '2024-01-01 11:00:00'),
    ('t5', 'assigned', '2024-01-01 10:00:00'),
    ('t5', 'resolved', '2024-01-01 11:00:00');

-- TODO: write your query
-- (hint: pivot MIN ts per ticket per status, compute duration, flag breach)
with pivoted as (
  SELECT 
    ticket_id,
    min(case when status = 'opened' then ts end)as opn_ts,
    min(case when status = 'assigned' then ts end) as asgn_ts,
    min(case when status = 'resolved' then ts end) as res_ts
  FROM p12_tickets
  group by 1
)
select 
  ticket_id,
  case 
    when opn_ts is not null and res_ts is not null then res_ts - opn_ts 
  end as res_time,
  case when opn_ts is not null and res_ts is not null and res_ts - opn_ts >= interval '5' hour then true else false 
  end as breached
from pivoted
;


-- ============================================================================
-- P13. Daily Active Users with Day-1 Retention
-- Expected:
--   2024-01-01 | dau=3 | d1_retention=66.67%
--   2024-01-02 | dau=3 | d1_retention=33.33%
--   2024-01-03 | dau=1 | d1_retention=NULL
-- ============================================================================

DROP TABLE IF EXISTS p13_events;
CREATE TABLE p13_events (
    user_id TEXT,
    event TEXT,
    event_date DATE
);
INSERT INTO p13_events VALUES
    ('u1', 'click', '2024-01-01'),
    ('u2', 'click', '2024-01-01'),
    ('u3', 'click', '2024-01-01'),
    ('u1', 'click', '2024-01-01'),
    ('u1', 'view',  '2024-01-02'),
    ('u2', 'click', '2024-01-02'),
    ('u4', 'click', '2024-01-02'),
    ('u4', 'view',  '2024-01-03');

-- TODO: write your query
-- (hint: get distinct users per day, self-join day to day+1,
--  count retained / count total per day)
with dist as (
  SELECT 
    distinct user_id, event_date
  FROM p13_events
)
SELECT 
  td.event_date,
  count(td.user_id) as dau,
  case 
    when max(tm.event_date) is null then null 
    else 100.0 * count(tm.user_id) / count(td.user_id)
  end as retention
FROM dist td
  LEFT JOIN dist tm
    ON td.event_date = tm.event_date - interval '1' DAY
    AND td.user_id = tm.user_id
group by 1
order by 1
;


-- ============================================================================
-- P14. Change Data Capture Replay
-- Replay inserts/updates/deletes to get final table state
-- Expected final state:
--   users: pk=1 → Alice, alice@new.com (updated)
--          pk=2 → deleted
--   orders: pk=100 → user_id=1, total=50
-- ============================================================================

DROP TABLE IF EXISTS p14_cdc;
CREATE TABLE p14_cdc (
    tbl TEXT,
    pk TEXT,
    op TEXT,
    data_name TEXT,
    data_email TEXT,
    data_user_id TEXT,
    data_total INT,
    ts TIMESTAMP
);
INSERT INTO p14_cdc VALUES
    ('users',  '1',   'insert', 'Alice',   'a@test.com',  NULL, NULL, '2024-01-01 10:00:00'),
    ('users',  '2',   'insert', 'Bob',     'b@test.com',  NULL, NULL, '2024-01-01 10:01:00'),
    ('users',  '1',   'update', 'Alice',   'alice@new.com', NULL, NULL, '2024-01-01 10:05:00'),
    ('users',  '2',   'delete', NULL,      NULL,          NULL, NULL, '2024-01-01 10:10:00'),
    ('orders', '100', 'insert', NULL,      NULL,          '1',  50,   '2024-01-01 10:02:00');

-- Edge cases
INSERT INTO p14_cdc VALUES
    ('users', '3', 'update', 'Charlie', 'c@test.com', NULL, NULL, '2024-01-01 10:05:00'),
    ('users', '3', 'insert', 'Charles', 'c@old.com',  NULL, NULL, '2024-01-01 10:00:00'),
    ('users', '4', 'insert', 'Diana',   NULL,         NULL, NULL, '2024-01-01 10:00:00'),
    ('users', '4', 'delete', NULL,      NULL,         NULL, NULL, '2024-01-01 10:01:00'),
    ('users', '4', 'delete', NULL,      NULL,         NULL, NULL, '2024-01-01 10:02:00');

-- TODO: write your query
-- (hint: get latest record per tbl+pk using ROW_NUMBER ORDER BY ts DESC,
--  then filter out deletes)
SELECT
  *,
  
FROM p14_cdc
;


-- ============================================================================
-- P15. Slowly Changing Dimension Type 2
-- Build history with start_date, end_date, is_current
-- Expected for e1:
--   Engineering | 2023-01-01 | 2023-06-01 | false
--   Product     | 2023-06-01 | 2024-01-01 | false
--   Engineering | 2024-01-01 | NULL       | true
-- ============================================================================

DROP TABLE IF EXISTS p15_departments;
CREATE TABLE p15_departments (
    employee_id TEXT,
    department TEXT,
    effective_date DATE
);
INSERT INTO p15_departments VALUES
    ('e1', 'Engineering', '2023-01-01'),
    ('e1', 'Product',     '2023-06-01'),
    ('e1', 'Engineering', '2024-01-01'),
    ('e2', 'Sales',       '2023-03-01'),
    ('e3', 'HR',          '2024-01-01'),
    ('e3', 'Finance',     '2023-01-01');

-- TODO: write your query
-- (hint: LEAD window function to get next effective_date as end_date)
-- SELECT ... FROM p15_departments ...;


-- ============================================================================
-- P16. Rate Limiter Check — >100 requests in any 60-second window
-- Expected: key1 + /api/data has breach(es), key2 and key1+/api/health do not
-- ============================================================================

DROP TABLE IF EXISTS p16_requests;
CREATE TABLE p16_requests (
    api_key TEXT,
    endpoint TEXT,
    ts TIMESTAMP
);

-- key1 /api/data: 101 requests in 50 seconds (breach)
INSERT INTO p16_requests
SELECT 'key1', '/api/data',
       '2024-01-01 10:00:00'::timestamp + (i * 0.5) * interval '1 second'
FROM generate_series(0, 100) AS i;

-- key2 /api/data: 50 requests spread over 100 seconds (no breach)
INSERT INTO p16_requests
SELECT 'key2', '/api/data',
       '2024-01-01 10:00:00'::timestamp + (i * 2) * interval '1 second'
FROM generate_series(0, 49) AS i;

-- key1 /api/health: 10 requests (no breach)
INSERT INTO p16_requests
SELECT 'key1', '/api/health',
       '2024-01-01 10:00:00'::timestamp + i * interval '1 second'
FROM generate_series(0, 9) AS i;

-- TODO: write your query
-- (hint: self-join or window with range frame to count events within 60s of each event)
-- For PostgreSQL, you can use:
--   COUNT(*) OVER (PARTITION BY api_key, endpoint
--                  ORDER BY ts
--                  RANGE BETWEEN INTERVAL '60 seconds' PRECEDING AND CURRENT ROW)
-- SELECT ... FROM p16_requests ...;


-- ============================================================================
-- P17. Log-Level Anomaly Spotter
-- Flag hours where error rate > 2x the service's overall average error rate
-- Expected: svc-a hour 14 is anomaly, svc-b has no anomalies
-- ============================================================================

DROP TABLE IF EXISTS p17_logs;
CREATE TABLE p17_logs (
    service TEXT,
    log_level TEXT,
    ts TIMESTAMP
);

-- svc-a: 90 INFO + 10 ERROR per hour, except hour 14 has 50 ERRORs
INSERT INTO p17_logs
SELECT 'svc-a', 'INFO',
       '2024-01-01'::timestamp + h * interval '1 hour' + (i % 60) * interval '1 minute'
FROM generate_series(0, 23) AS h, generate_series(1, 90) AS i;

INSERT INTO p17_logs
SELECT 'svc-a', 'ERROR',
       '2024-01-01'::timestamp + h * interval '1 hour' + (30 + i % 30) * interval '1 minute'
FROM generate_series(0, 23) AS h, generate_series(1, 10) AS i
WHERE h != 14;

INSERT INTO p17_logs
SELECT 'svc-a', 'ERROR',
       '2024-01-01 14:00:00'::timestamp + (30 + i % 30) * interval '1 minute'
FROM generate_series(1, 50) AS i;

-- svc-b: 100 INFO per hour, zero errors
INSERT INTO p17_logs
SELECT 'svc-b', 'INFO',
       '2024-01-01'::timestamp + h * interval '1 hour' + (i % 60) * interval '1 minute'
FROM generate_series(0, 23) AS h, generate_series(1, 100) AS i;

-- TODO: write your query
-- (hint: compute hourly counts per service, compute error_rate per hour,
--  compute avg error_rate per service, flag where hourly > 2 * avg)
-- SELECT ... FROM p17_logs ...;


-- ============================================================================
-- SQL PATTERNS CHEAT SHEET (for reference while practicing)
-- ============================================================================
--
-- WINDOW FUNCTIONS:
--   LAG(col, 1) OVER (PARTITION BY x ORDER BY ts)     -- previous row's value
--   LEAD(col, 1) OVER (PARTITION BY x ORDER BY ts)    -- next row's value
--   ROW_NUMBER() OVER (PARTITION BY x ORDER BY ts)     -- rank within group
--   SUM(flag) OVER (PARTITION BY x ORDER BY ts)        -- running sum (session IDs)
--   FIRST_VALUE(col) OVER (PARTITION BY x ORDER BY ts) -- first in group
--   LAST_VALUE(col) OVER (PARTITION BY x ORDER BY ts
--     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
--
-- RANGE FRAME (P16 rate limiting):
--   COUNT(*) OVER (PARTITION BY x ORDER BY ts
--     RANGE BETWEEN INTERVAL '60 seconds' PRECEDING AND CURRENT ROW)
--
-- TIME HELPERS:
--   EXTRACT(EPOCH FROM (ts2 - ts1))              -- seconds between timestamps
--   EXTRACT(HOUR FROM ts)                         -- hour of day (0-23)
--   DATE_TRUNC('hour', ts)                        -- truncate to hour
--   ts + INTERVAL '30 minutes'                    -- timestamp arithmetic
--
-- CONDITIONAL AGGREGATION:
--   SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)  -- count by condition
--   MIN(CASE WHEN event = 'X' THEN ts END)              -- first timestamp of type
--
-- USEFUL PATTERNS:
--   WITH cte AS (...) SELECT ... FROM cte        -- CTEs for readability
--   FILTER (WHERE condition)                      -- PostgreSQL aggregate filter
--   COALESCE(x, default)                          -- NULL handling
