"""
Level 3 — Window functions: ranking within partitions, running totals.
"""

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as sum_, rank, row_number, year, month
from snowflake.snowpark.window import Window

session = get_active_session()

bookings_df = session.table("STG_AIRBNB_BOOKINGS")
hosts_df = session.table("STG_AIRBNB_HOSTS")
listings_df = session.table("STG_AIRBNB_LISTINGS")


# --- Exercise 9: rank listings within each city by total revenue -------------
full_df = bookings_df.join(
    listings_df,
    bookings_df["LISTING_ID"] == listings_df["LISTING_ID"],
    how="inner",
).join(
    hosts_df,
    listings_df["HOST_ID"] == hosts_df["HOST_ID"],
    how="inner",
)

listing_revenue_per_city_df = full_df.groupBy(
    listings_df["LISTING_ID"].alias("LISTING_ID"), col("CITY")
).agg(
    sum_(
        col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
    ).alias("TOTAL_REVENUE")
)

city_window_spec = Window.partitionBy(col("CITY")).orderBy(col("TOTAL_REVENUE").desc())

ranked_by_city_df = listing_revenue_per_city_df.withColumn(
    "LISTING_RANK_PER_CITY", rank().over(city_window_spec)
)

# sanity check: top 3 per city, several cities side by side
ranked_by_city_df.filter(col("LISTING_RANK_PER_CITY") <= 3).orderBy(
    col("CITY"), col("LISTING_RANK_PER_CITY")
).show(30)


# --- Exercise 10: each host's single highest-earning listing ------------------
joined_df = bookings_df.join(
    listings_df,
    bookings_df["LISTING_ID"] == listings_df["LISTING_ID"],
    how="inner",
)

listing_earning_df = joined_df.groupBy(
    listings_df["LISTING_ID"], col("HOST_ID")
).agg(
    sum_(
        col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
    ).alias("TOTAL_EARNING")
)

host_window_spec = Window.partitionBy(col("HOST_ID")).orderBy(col("TOTAL_EARNING").desc())

top_listing_per_host_df = listing_earning_df.withColumn(
    "HOST_RANKING", row_number().over(host_window_spec)
).filter(col("HOST_RANKING") == 1)
top_listing_per_host_df.show()


# --- Exercise 11: running total of revenue per month ---------------------------
filtered_df = (
    bookings_df.withColumn(
        "REVENUE", col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
    )
    .withColumn("BOOKING_YEAR", year(col("BOOKING_DATE")))
    .withColumn("BOOKING_MONTH", month(col("BOOKING_DATE")))
)

monthly_revenue_df = filtered_df.groupBy(
    col("BOOKING_YEAR"), col("BOOKING_MONTH")
).agg(sum_(col("REVENUE")).alias("MONTHLY_REVENUE"))

running_total_window_spec = Window.orderBy(col("BOOKING_YEAR"), col("BOOKING_MONTH"))

running_total_monthly_revenue_df = (
    monthly_revenue_df.withColumn(
        "RUNNING_TOTAL_MONTHLY_REVENUE",
        sum_(col("MONTHLY_REVENUE")).over(running_total_window_spec),
    )
    # re-sort the FINAL dataframe — orderBy on an intermediate df doesn't
    # guarantee display order after further transformations
    .orderBy(col("BOOKING_YEAR"), col("BOOKING_MONTH"))
)
running_total_monthly_revenue_df.show(24)
