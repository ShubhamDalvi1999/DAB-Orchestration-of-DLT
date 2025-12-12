"""
DDL SQL statements for taxi data pipeline.
Contains schema definitions and table creation statements.
"""

# Bronze layer DDL
BRONZE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS bronze_taxi_trips (
    trip_id STRING,
    vendor_id INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    payment_type INT,
    fare_amount DOUBLE,
    surcharge DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    total_amount DOUBLE,
    pickup_zip STRING,
    dropoff_zip STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
"""

# Silver layer DDL
SILVER_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS silver_taxi_trips (
    trip_id STRING,
    vendor_id INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    payment_type INT,
    fare_amount DOUBLE,
    surcharge DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    total_amount DOUBLE,
    pickup_zip STRING,
    dropoff_zip STRING,
    ingestion_timestamp TIMESTAMP,
    fare_per_mile DOUBLE,
    updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
"""

# Gold layer DDL
GOLD_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS gold_zone_metrics (
    pickup_zip STRING,
    total_trips BIGINT,
    total_revenue DOUBLE,
    avg_fare DOUBLE,
    avg_fare_per_mile DOUBLE,
    avg_trip_distance DOUBLE,
    last_updated TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
"""


def execute_ddl(spark, ddl_statement: str) -> None:
    """
    Executes a DDL statement using Spark SQL.
    
    Args:
        spark: SparkSession instance
        ddl_statement: DDL SQL statement to execute
    """
    spark.sql(ddl_statement)


def create_all_tables(spark) -> None:
    """
    Creates all tables for the taxi data pipeline.
    
    Args:
        spark: SparkSession instance
    """
    execute_ddl(spark, BRONZE_TABLE_DDL)
    execute_ddl(spark, SILVER_TABLE_DDL)
    execute_ddl(spark, GOLD_TABLE_DDL)

