"""
Level 5 — Gold layer: host-level KPI table, written back to Snowflake.
"""

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as sum_, count_distinct, round as round_

session = get_active_session()

bookings_df = session.table("STG_AIRBNB_BOOKINGS")
hosts_df = session.table("STG_AIRBNB_HOSTS")
listings_df = session.table("STG_AIRBNB_LISTINGS")


# --- Exercise 14: AIRBNB_HOST_PERFORMANCE gold table ----------------------------
full_df = bookings_df.join(
    listings_df,
    bookings_df["LISTING_ID"] == listings_df["LISTING_ID"],
    how="inner",
).join(
    hosts_df,
    listings_df["HOST_ID"] == hosts_df["HOST_ID"],
    how="inner",
)

# Group by HOST_ID only — NOT LISTING_ID + HOST_ID. This must be one row per
# host, rolling up ALL of that host's listings/bookings together.
host_level_kpi_df = (
    full_df.groupBy(
        hosts_df["HOST_ID"].alias("HOST_ID"),
        col("HOST_FIRST_NAME"),
        col("HOST_LAST_NAME"),
    )
    .agg(
        sum_(
            col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
        ).alias("TOTAL_REVENUE"),
        # count_distinct, not count — a host's listing can appear multiple
        # times across multiple bookings, so plain count() would inflate
        # the listing count.
        count_distinct(listings_df["LISTING_ID"]).alias("LISTING_COUNT"),
    )
    .withColumn(
        "AVG_REVENUE_PER_LISTING",
        round_(col("TOTAL_REVENUE") / col("LISTING_COUNT"), 2),
    )
)

host_level_kpi_df.show()

host_level_kpi_df.write.mode("overwrite").save_as_table("AIRBNB_HOST_PERFORMANCE")
