"""
Level 4 — UDFs: Python logic pushed into Snowflake.
"""

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, udf, min as min_, max as max_, avg, round as round_, count
from snowflake.snowpark.types import StringType

session = get_active_session()

bookings_df = session.table("STG_AIRBNB_BOOKINGS")
listings_df = session.table("STG_AIRBNB_LISTINGS")


# --- Exercise 12: bucket listings into Budget / Mid / Luxury -------------------
# Checked actual distribution first: min=50, max=300, avg=172 (before picking
# thresholds) — thresholds below chosen to actually spread across all 3 buckets.
listings_df.select(
    min_(col("PRICE_PER_NIGHT")).alias("MIN_PRICE"),
    max_(col("PRICE_PER_NIGHT")).alias("MAX_PRICE"),
    round_(avg(col("PRICE_PER_NIGHT")), 0).alias("AVG_PRICE"),
).show()


@udf(name="price_bucket", is_permanent=False, replace=True, return_type=StringType())
def price_bucket(price: int):
    if price < 100:
        return "Budget"
    elif price < 200:
        return "Mid"
    else:
        return "Luxury"


listings_with_bucket_df = listings_df.withColumn(
    "PRICE_BUCKET", price_bucket(col("PRICE_PER_NIGHT"))
)
listings_with_bucket_df.show()

# verify the split is meaningful (all 3 buckets populated, not one dominating)
listings_with_bucket_df.groupBy(col("PRICE_BUCKET")).agg(
    count(col("LISTING_ID")).alias("TOTAL_COUNT")
).orderBy(col("TOTAL_COUNT").desc()).show()


# --- Exercise 13: flag high-value bookings --------------------------------------
# Checked actual revenue distribution first: avg ~= 1378
bookings_df.select(
    min_(col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")).alias(
        "MIN_REV"
    ),
    max_(col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")).alias(
        "MAX_REV"
    ),
    round_(
        avg(col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")), 0
    ).alias("AVG_REV"),
).show()


@udf(name="booking_standard", is_permanent=False, replace=True, return_type=StringType())
def booking_standard(revenue: int):
    if revenue < 1300:
        return "Standard"
    return "High Value"


revenue_calculated_df = bookings_df.withColumn(
    "REVENUE", col("BOOKING_AMOUNT") + col("CLEANING_FEE") + col("SERVICE_FEE")
).withColumn("BOOKING_STANDARD", booking_standard(col("REVENUE")))
revenue_calculated_df.show()

revenue_calculated_df.groupBy(col("BOOKING_STANDARD")).count().show()
