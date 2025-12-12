"""
Pure transformation functions for taxi data pipeline.
These functions contain no DLT decorators, no side effects, and are safe to reuse and test.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, round, sum, avg, count, current_timestamp, max


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Transforms bronze taxi data to silver layer by:
    - Filtering out invalid records
    - Adding calculated fields (fare_per_mile)
    - Standardizing data types
    - Adding updated_at timestamp for incremental tracking
    
    Args:
        df: Bronze layer DataFrame
        
    Returns:
        DataFrame: Cleaned and enriched silver layer data
    """
    return (
        df.filter(col("fare_amount") > 0)
        .filter(col("trip_distance") > 0)
        .withColumn(
            "fare_per_mile",
            when(col("trip_distance") > 0, round(col("fare_amount") / col("trip_distance"), 2))
            .otherwise(0)
        )
        .withColumn("updated_at", current_timestamp())
    )


def transform_silver_to_gold(df: DataFrame) -> DataFrame:
    """
    Transforms silver taxi data to gold layer by aggregating metrics by pickup zone.
    
    Creates business-level aggregations:
    - Trip counts per zone
    - Revenue metrics per zone
    - Average fare statistics
    - Last updated timestamp
    
    Args:
        df: Silver layer DataFrame
        
    Returns:
        DataFrame: Aggregated metrics by pickup zone
    """
    return (
        df.groupBy(col("pickup_zip"))
        .agg(
            count("*").alias("total_trips"),
            round(sum("fare_amount"), 2).alias("total_revenue"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            max(col("updated_at")).alias("last_updated")
        )
        .orderBy(col("total_revenue").desc())
    )

