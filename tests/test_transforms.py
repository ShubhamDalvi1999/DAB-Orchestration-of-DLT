"""
Unit tests for taxi_dlt_pkg transformation functions.
These tests verify the pure transformation logic without DLT dependencies.
All tests work with local Spark and don't require Databricks.
"""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.functions import col

from taxi_dlt_pkg.transforms import transform_bronze_to_silver, transform_silver_to_gold


pytestmark = pytest.mark.local


@pytest.fixture
def bronze_schema():
    """Schema for bronze layer test data."""
    return StructType([
        StructField("trip_id", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_zip", StringType(), True),
        StructField("dropoff_zip", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])


@pytest.fixture
def bronze_test_data(spark, bronze_schema):
    """Sample bronze layer data for testing."""
    data = [
        ("trip_1", 10.5, 2.5, "10001", "10002", "2024-01-01 10:00:00"),
        ("trip_2", 15.0, 3.0, "10002", "10003", "2024-01-01 11:00:00"),
        ("trip_3", 0.0, 1.0, "10003", "10004", "2024-01-01 12:00:00"),  # Invalid fare
        ("trip_4", 20.0, 0.0, "10004", "10005", "2024-01-01 13:00:00"),  # Invalid distance
        ("trip_5", 25.0, 5.0, "10005", "10006", "2024-01-01 14:00:00"),
    ]
    return spark.createDataFrame(data, schema=bronze_schema)


def test_transform_bronze_to_silver_filters_invalid_records(spark, bronze_test_data):
    """Test that invalid records (zero fare or distance) are filtered out."""
    result = transform_bronze_to_silver(bronze_test_data)
    
    # Should only have 3 valid records (trip_1, trip_2, trip_5)
    assert result.count() == 3
    
    # Verify invalid records are filtered
    result_ids = [row["trip_id"] for row in result.select("trip_id").collect()]
    assert "trip_1" in result_ids
    assert "trip_2" in result_ids
    assert "trip_5" in result_ids
    assert "trip_3" not in result_ids  # Invalid fare
    assert "trip_4" not in result_ids  # Invalid distance


def test_transform_bronze_to_silver_adds_fare_per_mile(spark, bronze_test_data):
    """Test that fare_per_mile is calculated correctly."""
    result = transform_bronze_to_silver(bronze_test_data)
    
    # Check fare_per_mile calculation for trip_1: 10.5 / 2.5 = 4.2
    trip_1 = result.filter(col("trip_id") == "trip_1").first()
    assert trip_1 is not None
    assert abs(trip_1["fare_per_mile"] - 4.2) < 0.01
    
    # Check fare_per_mile calculation for trip_2: 15.0 / 3.0 = 5.0
    trip_2 = result.filter(col("trip_id") == "trip_2").first()
    assert trip_2 is not None
    assert abs(trip_2["fare_per_mile"] - 5.0) < 0.01


def test_transform_bronze_to_silver_adds_updated_at(spark, bronze_test_data):
    """Test that updated_at timestamp is added."""
    result = transform_bronze_to_silver(bronze_test_data)
    
    # Verify updated_at column exists and is not null
    assert "updated_at" in result.columns
    result_rows = result.select("updated_at").collect()
    assert all(row["updated_at"] is not None for row in result_rows)


def test_transform_silver_to_gold_aggregates_by_zone(spark, bronze_schema):
    """Test that silver data is correctly aggregated by pickup zone."""
    # Create silver-like data (already transformed)
    silver_data = [
        ("trip_1", 10.5, 2.5, "10001", "10002", "2024-01-01 10:00:00", 4.2, "2024-01-01 10:00:00"),
        ("trip_2", 15.0, 3.0, "10001", "10003", "2024-01-01 11:00:00", 5.0, "2024-01-01 11:00:00"),
        ("trip_3", 20.0, 4.0, "10002", "10004", "2024-01-01 12:00:00", 5.0, "2024-01-01 12:00:00"),
    ]
    
    silver_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_zip", StringType(), True),
        StructField("dropoff_zip", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("fare_per_mile", DoubleType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    
    silver_df = spark.createDataFrame(silver_data, schema=silver_schema)
    result = transform_silver_to_gold(silver_df)
    
    # Should have 2 zones (10001 and 10002)
    assert result.count() == 2
    
    # Check zone 10001 aggregation
    zone_10001 = result.filter(col("pickup_zip") == "10001").first()
    assert zone_10001 is not None
    assert zone_10001["total_trips"] == 2
    assert abs(zone_10001["total_revenue"] - 25.5) < 0.01  # 10.5 + 15.0
    assert abs(zone_10001["avg_fare"] - 12.75) < 0.01  # (10.5 + 15.0) / 2
    
    # Check zone 10002 aggregation
    zone_10002 = result.filter(col("pickup_zip") == "10002").first()
    assert zone_10002 is not None
    assert zone_10002["total_trips"] == 1
    assert abs(zone_10002["total_revenue"] - 20.0) < 0.01


def test_transform_silver_to_gold_orders_by_revenue(spark, bronze_schema):
    """Test that results are ordered by total_revenue descending."""
    silver_data = [
        ("trip_1", 10.0, 2.0, "10001", "10002", "2024-01-01 10:00:00", 5.0, "2024-01-01 10:00:00"),
        ("trip_2", 30.0, 3.0, "10002", "10003", "2024-01-01 11:00:00", 10.0, "2024-01-01 11:00:00"),
        ("trip_3", 20.0, 4.0, "10003", "10004", "2024-01-01 12:00:00", 5.0, "2024-01-01 12:00:00"),
    ]
    
    silver_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_zip", StringType(), True),
        StructField("dropoff_zip", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("fare_per_mile", DoubleType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    
    silver_df = spark.createDataFrame(silver_data, schema=silver_schema)
    result = transform_silver_to_gold(silver_df)
    
    # Verify ordering: 10002 (30.0) > 10003 (20.0) > 10001 (10.0)
    result_rows = result.collect()
    assert result_rows[0]["pickup_zip"] == "10002"
    assert result_rows[0]["total_revenue"] == 30.0
    assert result_rows[1]["pickup_zip"] == "10003"
    assert result_rows[1]["total_revenue"] == 20.0
    assert result_rows[2]["pickup_zip"] == "10001"
    assert result_rows[2]["total_revenue"] == 10.0


def test_transform_silver_to_gold_includes_last_updated(spark, bronze_schema):
    """Test that last_updated timestamp is included in gold layer."""
    silver_data = [
        ("trip_1", 10.0, 2.0, "10001", "10002", "2024-01-01 10:00:00", 5.0, "2024-01-01 10:00:00"),
        ("trip_2", 15.0, 3.0, "10001", "10003", "2024-01-01 11:00:00", 5.0, "2024-01-01 11:00:00"),
    ]
    
    silver_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_zip", StringType(), True),
        StructField("dropoff_zip", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("fare_per_mile", DoubleType(), True),
        StructField("updated_at", TimestampType(), True),
    ])
    
    silver_df = spark.createDataFrame(silver_data, schema=silver_schema)
    result = transform_silver_to_gold(silver_df)
    
    # Verify last_updated column exists
    assert "last_updated" in result.columns
    
    # Check that last_updated is the maximum updated_at for the zone
    zone_10001 = result.filter(col("pickup_zip") == "10001").first()
    assert zone_10001["last_updated"] is not None

