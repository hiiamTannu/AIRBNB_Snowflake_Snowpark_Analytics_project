# Snowpark Airbnb Analytics

A hands-on Snowpark Python project built to learn the Snowpark DataFrame API
from scratch, using a mock Airbnb dataset (bookings, listings, hosts) inside
Snowflake Notebooks.

## Dataset

Three staging tables:

| Table | Rows | Key columns |
|---|---|---|
| `STG_AIRBNB_BOOKINGS` | 5,000 | `BOOKING_ID`, `LISTING_ID`, `BOOKING_DATE`, `BOOKING_AMOUNT`, `CLEANING_FEE`, `SERVICE_FEE`, `BOOKING_STATUS` |
| `STG_AIRBNB_LISTINGS` | 500 | `LISTING_ID`, `HOST_ID`, `PROPERTY_TYPE`, `ROOM_TYPE`, `CITY`, `PRICE_PER_NIGHT` |
| `STG_AIRBNB_HOSTS` | 200 | `HOST_ID`, `HOST_NAME`, `IS_SUPERHOST`, `RESPONSE_RATE` |

Relationships: `HOSTS` → `LISTINGS` (via `HOST_ID`) → `BOOKINGS` (via `LISTING_ID`).

## Structure

```
exercises/
  level1_dataframe_basics.py   # filtering, grouping, aggregation, avg comparisons
  level2_joins.py               # single & chained joins, revenue by city/host/room type
  level3_window_functions.py    # ranking within partitions, running totals
  level4_udfs.py                # Python UDFs: price bucketing, high-value flagging
  level5_gold_layer.py          # host-level KPI table, written back to Snowflake
```

## What each level covers

**Level 1 — DataFrame basics**
Revenue by booking status, top listings by booking count, monthly booking
trends, listings priced above average (three different ways: `.collect()`
scalar, empty-partition window function, and cross-join broadcast).

**Level 2 — Joins**
Chained joins across all three tables, revenue by host/city/room type,
superhost vs. non-superhost average revenue *per listing* (not per booking —
a key distinction for fair comparison).

**Level 3 — Window functions**
`rank()` / `row_number()` within partitions (by city, by host), and a
cumulative running total of monthly revenue using `sum().over()`.

**Level 4 — UDFs**
Two Python UDFs pushed into Snowflake: a price-tier bucketer
(Budget/Mid/Luxury) and a high-value booking flagger, both threshold-checked
against the actual data distribution before picking cutoffs.

**Level 5 — Gold layer**
`AIRBNB_HOST_PERFORMANCE` — one row per host with total revenue, distinct
listing count (`count_distinct`, not `count`), and average revenue per
listing — written with `.write.mode("overwrite").save_as_table()`.

## Key Snowpark concepts practiced

- Lazy evaluation — nothing runs until an action (`.show()`, `.collect()`, `.write`)
- `groupBy` granularity and functional dependency (grouping by a unique key
  alongside another column silently breaks aggregation)
- `Window.partitionBy()` — empty (global) vs. non-empty (per-group) partitions
- Three patterns for "compare a row to an aggregate": `.collect()` scalar,
  window function, cross-join broadcast
- `count()` vs `count_distinct()`
- Why `.orderBy()` on an intermediate DataFrame doesn't guarantee final
  display order — re-sort the final DataFrame before `.show()`
- UDF creation with type hints / `return_type`, temporary vs. permanent

## Requirements

```
snowflake-snowpark-python
```

Designed to run inside a **Snowflake Notebook**, where the Snowpark `session`
object is auto-injected. If running elsewhere, create it manually:

```python
from snowflake.snowpark.context import get_active_session
session = get_active_session()
```
