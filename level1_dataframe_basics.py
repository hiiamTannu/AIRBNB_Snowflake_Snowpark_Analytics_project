"""
Level 1 — DataFrame basics: filtering, grouping, aggregation.
"""

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as sum_, avg, count, year, month
from snowflake.snowpark.window import Window

session = get_active_session()

bookings_df = session.table("STG_AIRBNB_BOOKINGS")
hosts_df = session.table("STG_AIRBNB_HOSTS")
listings_df = session.table("STG_AIRBNB_LISTINGS")


# --- Exercise 1: total revenue per booking_status ---------------------------
revenue_by_status_df = (
    bookings_df.groupBy(col("BOOKING_STATUS"))
    .agg(
        sum_(
            col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
        ).alias("TOTAL_REVENUE")
    )
    .orderBy(col("TOTAL_REVENUE").desc())
)
revenue_by_status_df.show()


# --- Exercise 2: top 10 listings by number of bookings -----------------------
top_listings_df = (
    bookings_df.groupBy(col("LISTING_ID"))
    .agg(count(col("BOOKING_ID")).alias("TOTAL_BOOKINGS"))
    .orderBy(col("TOTAL_BOOKINGS").desc())
    .limit(10)
)
top_listings_df.show()


# --- Exercise 3: bookings by month (year + month, chronological) -------------
monthly_booking_df = (
    bookings_df.groupBy(
        year(col("BOOKING_DATE")).alias("BOOKING_YEAR"),
        month(col("BOOKING_DATE")).alias("BOOKING_MONTH"),
    )
    .agg(count(col("BOOKING_ID")).alias("TOTAL_BOOKINGS"))
    .orderBy(col("BOOKING_YEAR"), col("BOOKING_MONTH"))
)
monthly_booking_df.show(24)


# --- Exercise 4: listings priced above average --------------------------------
# Approach A — window function with an empty partition (single query, most efficient)
window_spec = Window.partitionBy()
above_avg_listings_window_df = (
    listings_df.withColumn("AVG_PRICE", avg(col("PRICE_PER_NIGHT")).over(window_spec))
    .filter(col("PRICE_PER_NIGHT") > col("AVG_PRICE"))
    .select(col("LISTING_ID"))
    .orderBy(col("LISTING_ID").asc())
)
above_avg_listings_window_df.show()

# Approach B — cross join against a one-row aggregate (broadcast the average)
avg_price_df = listings_df.agg(avg(col("PRICE_PER_NIGHT")).alias("AVG_PRICE"))
above_avg_listings_join_df = (
    listings_df.cross_join(avg_price_df)
    .filter(col("PRICE_PER_NIGHT") > col("AVG_PRICE"))
    .select(col("LISTING_ID"))
    .orderBy(col("LISTING_ID"))
)
above_avg_listings_join_df.show()

# Approach C — .collect() a scalar, then filter with a plain Python value
# (best when you need the number itself back in Python, e.g. for logging/params)
avg_price_value = listings_df.agg(avg(col("PRICE_PER_NIGHT"))).collect()[0][0]
above_avg_listings_collect_df = listings_df.filter(
    col("PRICE_PER_NIGHT") > avg_price_value
)
above_avg_listings_collect_df.show()
