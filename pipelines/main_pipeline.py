"""
DLT Pipeline Orchestration - Taxi Data Pipeline
This file contains only DLT table definitions and orchestration logic.
All transformation logic is imported from the taxi_dlt_pkg package.

Ingestion:
- Bronze layer reads raw data from source (samples.nyctaxi.trips)
- Ingestion functions are in taxi_dlt_pkg.ingestion module
- Supports configurable source tables for testing and different environments

Incremental Processing:
- Bronze: Uses incremental mode to process only new records
- Silver: Uses incremental mode with deduplication
- Gold: Uses incremental mode to update aggregated metrics

Write Operations:
- Gold layer data is automatically written by DLT @dlt.table decorator
- Additional write operations can be added after gold layer for downstream consumption
"""

import dlt

from taxi_dlt_pkg.ingestion import read_raw_taxi_trips
from taxi_dlt_pkg.transforms import transform_bronze_to_silver, transform_silver_to_gold
from taxi_dlt_pkg.writes import write_gold_to_reporting_table


# Bronze layer - Raw ingestion with incremental processing
@dlt.table(
    name="bronze_taxi_trips",
    comment="Bronze table containing raw NYC Taxi trip data",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_taxi_trips():
    """
    Ingests raw NYC Taxi trips from the samples catalog.
    This serves as the bronze (raw) layer in the medallion architecture.
    
    Uses incremental processing: DLT automatically tracks which records
    have been processed and only processes new data on subsequent runs.
    """
    # Uses default source_table=None which reads from samples.nyctaxi.trips
    return read_raw_taxi_trips(spark)


# Silver layer - Cleaned and transformed data with incremental processing
@dlt.table(
    name="silver_taxi_trips",
    comment="Silver table with cleaned and enriched taxi trip data",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_fare", "fare_amount > 0")
@dlt.expect("valid_distance", "trip_distance > 0")
@dlt.expect_or_drop("has_pickup_zip", "pickup_zip IS NOT NULL")
def silver_taxi_trips():
    """
    Transforms bronze taxi data by:
    - Filtering out invalid records
    - Adding calculated fields (fare_per_mile)
    - Standardizing data types
    
    Uses incremental processing: Only processes new records from bronze layer.
    DLT automatically tracks dependencies and processes incrementally.
    """
    df = dlt.read("bronze_taxi_trips")
    return transform_bronze_to_silver(df)


# Gold layer - Aggregated business metrics with incremental processing
@dlt.table(
    name="gold_zone_metrics",
    comment="Gold table with aggregated taxi metrics by pickup zone",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_zone_metrics():
    """
    Creates business-level aggregations:
    - Trip counts per zone
    - Revenue metrics per zone
    - Average fare statistics
    
    Uses incremental processing: Only processes new/updated records from silver layer.
    DLT automatically handles incremental aggregation updates.
    """
    df = dlt.read("silver_taxi_trips")
    return transform_silver_to_gold(df)


# Post-Gold: Reporting table for downstream consumption
@dlt.table(
    name="reporting_zone_metrics",
    comment="Reporting table with gold metrics for downstream consumption and reporting",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def reporting_zone_metrics():
    """
    Creates a reporting/export table from gold metrics.
    This table is optimized for downstream consumption, reporting tools, or external systems.
    
    Note: In DLT, this is automatically written. For custom write operations,
    use the write functions from taxi_dlt_pkg.writes module.
    """
    # Read from gold layer
    df = dlt.read("gold_zone_metrics")
    
    # Additional transformations for reporting can be added here if needed
    # For now, we just pass through the gold data
    return df

