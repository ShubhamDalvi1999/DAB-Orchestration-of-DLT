"""
Unit tests for taxi_dlt_pkg ingestion functions with local test data.
These tests work without requiring Databricks-specific tables.
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from taxi_dlt_pkg.ingestion import read_raw_taxi_trips


@pytest.fixture
def test_taxi_schema():
    """Schema matching samples.nyctaxi.trips structure."""
    return StructType([
        StructField("trip_id", StringType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("rate_code_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("surcharge", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("pickup_zip", StringType(), True),
        StructField("dropoff_zip", StringType(), True),
    ])


@pytest.fixture
def test_taxi_data(spark, test_taxi_schema):
    """Sample taxi trip data for local testing."""
    data = [
        ("trip_1", 1, "2024-01-01 10:00:00", "2024-01-01 10:30:00", 2, 5.5, -73.985, 40.758, 1, "N", -73.980, 40.750, 1, 15.50, 0.0, 2.00, 0.0, 17.50, "10001", "10002"),
        ("trip_2", 1, "2024-01-01 11:00:00", "2024-01-01 11:20:00", 1, 3.2, -73.990, 40.760, 1, "N", -73.985, 40.758, 1, 12.00, 0.0, 1.50, 0.0, 13.50, "10002", "10003"),
    ]
    return spark.createDataFrame(data, schema=test_taxi_schema)


def test_read_raw_taxi_trips_with_local_data(spark, test_taxi_data):
    """Test ingestion function with local test data."""
    # Register the test data as a temporary view
    test_taxi_data.createOrReplaceTempView("test_taxi_trips")
    
    # Test the ingestion function with the test table
    result = read_raw_taxi_trips(spark, source_table="test_taxi_trips")
    
    # Verify ingestion_timestamp is added
    assert "ingestion_timestamp" in result.columns
    
    # Verify all original columns are present
    original_cols = set(test_taxi_data.columns)
    result_cols = set(result.columns)
    assert original_cols.issubset(result_cols)
    
    # Verify row count matches
    assert result.count() == 2
    
    # Verify ingestion_timestamp is not null
    result_rows = result.select("ingestion_timestamp").collect()
    assert all(row["ingestion_timestamp"] is not None for row in result_rows)


def test_read_raw_taxi_trips_with_source_table_parameter(spark, test_taxi_data):
    """Test that source_table parameter works correctly."""
    # Register test data with a specific name
    test_taxi_data.createOrReplaceTempView("custom_source_table")
    
    # Use the custom table name
    result = read_raw_taxi_trips(spark, source_table="custom_source_table")
    
    assert result.count() == 2
    assert "ingestion_timestamp" in result.columns

