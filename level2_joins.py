"""
Level 2 — Joins: single & chained joins across bookings, listings, hosts.
"""

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as sum_, avg, round as round_

session = get_active_session()

bookings_df = session.table("STG_AIRBNB_BOOKINGS")
hosts_df = session.table("STG_AIRBNB_HOSTS")
listings_df = session.table("STG_AIRBNB_LISTINGS")


# --- Exercise 5: total revenue per host (3-way join) -------------------------
bookings_listings_hosts_df = bookings_df.join(
    listings_df,
    bookings_df["LISTING_ID"] == listings_df["LISTING_ID"],
    how="inner",
).join(
    hosts_df,
    listings_df["HOST_ID"] == hosts_df["HOST_ID"],
    how="inner",
)

revenue_per_host_df = (
    bookings_listings_hosts_df.groupBy(
        hosts_df["HOST_ID"].alias("HOST_ID"),
        col("HOST_FIRST_NAME"),
        col("HOST_LAST_NAME"),
    )
    .agg(
        sum_(
            col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
        ).alias("TOTAL_REVENUE")
    )
    .orderBy(col("TOTAL_REVENUE").desc())
)
revenue_per_host_df.show()


# --- Exercise 6: which CITY generates the most revenue ------------------------
most_revenued_city_df = (
    bookings_df.join(
        listings_df,
        bookings_df["LISTING_ID"] == listings_df["LISTING_ID"],
        how="inner",
    )
    .group_by(col("CITY"))
    .agg(
        sum_(
            col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
        ).alias("TOTAL_REVENUE")
    )
    .orderBy(col("TOTAL_REVENUE").desc())
    .limit(1)
)
most_revenued_city_df.show()


# --- Exercise 7: average nights booked per room type ---------------------------
avg_nights_by_room_type_df = (
    bookings_df.join(
        listings_df,
        bookings_df["LISTING_ID"] == listings_df["LISTING_ID"],
        how="inner",
    )
    .groupBy(col("ROOM_TYPE"))
    .agg(round_(avg(col("NIGHTS_BOOKED")), 2).alias("AVG_NIGHTS_BOOKED"))
    .orderBy(col("AVG_NIGHTS_BOOKED").desc())
)
avg_nights_by_room_type_df.show()


# --- Exercise 8: superhost vs non-superhost avg revenue PER LISTING ------------
# (per listing, not per booking — avoids unfairly weighting hosts with more listings)
full_df = bookings_df.join(
    listings_df,
    bookings_df["LISTING_ID"] == listings_df["LISTING_ID"],
    how="inner",
).join(
    hosts_df,
    listings_df["HOST_ID"] == hosts_df["HOST_ID"],
    how="inner",
)

listing_revenue_df = full_df.groupBy(
    listings_df["LISTING_ID"], col("IS_SUPERHOST")
).agg(
    sum_(
        col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
    ).alias("TOTAL_LISTING_REVENUE")
)

superhost_comparison_df = (
    listing_revenue_df.groupBy(col("IS_SUPERHOST"))
    .agg(
        round_(avg(col("TOTAL_LISTING_REVENUE")), 2).alias(
            "AVG_REVENUE_PER_LISTING"
        )
    )
    .orderBy(col("AVG_REVENUE_PER_LISTING"))
)
superhost_comparison_df.show()
