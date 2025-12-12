"""
Unit tests for taxi_dlt_pkg write functions.
These tests verify write operations work correctly with local Spark.
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
from taxi_dlt_pkg.writes import (
    write_gold_table,
    write_gold_to_reporting_table,
    write_gold_to_export_table
)


pytestmark = pytest.mark.local


@pytest.fixture
def gold_metrics_schema():
    """Schema for gold layer metrics."""
    return StructType([
        StructField("pickup_zip", StringType(), True),
        StructField("total_trips", LongType(), True),
        StructField("total_revenue", DoubleType(), True),
        StructField("avg_fare", DoubleType(), True),
        StructField("avg_fare_per_mile", DoubleType(), True),
        StructField("avg_trip_distance", DoubleType(), True),
        StructField("last_updated", TimestampType(), True),
    ])


@pytest.fixture
def gold_metrics_data(spark, gold_metrics_schema):
    """Sample gold layer metrics data for testing."""
    data = [
        ("10001", 150, 2500.50, 16.67, 5.0, 3.5, "2024-01-01 10:00:00"),
        ("10002", 200, 3500.75, 17.50, 5.5, 4.0, "2024-01-01 11:00:00"),
    ]
    return spark.createDataFrame(data, schema=gold_metrics_schema)


def test_write_gold_table_overwrite_mode(spark, gold_metrics_data):
    """Test writing gold table in overwrite mode."""
    table_name = "test_gold_table"
    
    try:
        # Write in overwrite mode
        write_gold_table(
            df=gold_metrics_data,
            table_name=table_name,
            mode="overwrite",
            spark=spark
        )
        
        # Verify table was created and has data
        result = spark.read.table(table_name)
        assert result.count() == 2
        
        # Verify data integrity
        result_rows = result.collect()
        assert len(result_rows) == 2
        assert result_rows[0]["pickup_zip"] in ["10001", "10002"]
        
    finally:
        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass


def test_write_gold_table_merge_mode(spark, gold_metrics_data):
    """Test writing gold table in merge mode."""
    table_name = "test_gold_merge_table"
    
    try:
        # First write to create table
        write_gold_table(
            df=gold_metrics_data,
            table_name=table_name,
            mode="overwrite",
            spark=spark
        )
        
        # Create updated data with one new record and one updated
        updated_data = spark.createDataFrame([
            ("10001", 160, 2600.50, 16.25, 5.1, 3.6, "2024-01-01 12:00:00"),  # Updated
            ("10003", 100, 1500.00, 15.00, 4.5, 3.0, "2024-01-01 12:00:00"),  # New
        ], schema=gold_metrics_data.schema)
        
        # Merge the updated data
        write_gold_table(
            df=updated_data,
            table_name=table_name,
            mode="merge",
            merge_keys=["pickup_zip"],
            spark=spark
        )
        
        # Verify merge worked
        result = spark.read.table(table_name)
        assert result.count() == 3  # Original 2, updated 1, new 1
        
        # Verify updated record
        updated_row = result.filter("pickup_zip = '10001'").first()
        assert updated_row is not None
        assert updated_row["total_trips"] == 160
        
        # Verify new record
        new_row = result.filter("pickup_zip = '10003'").first()
        assert new_row is not None
        assert new_row["total_trips"] == 100
        
    finally:
        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass


def test_write_gold_to_reporting_table(spark, gold_metrics_data):
    """Test writing gold data to reporting table."""
    reporting_table = "test_reporting_table"
    
    try:
        write_gold_to_reporting_table(
            df=gold_metrics_data,
            reporting_table_name=reporting_table,
            spark=spark,
            mode="overwrite"
        )
        
        # Verify table was created
        result = spark.read.table(reporting_table)
        assert result.count() == 2
        
    finally:
        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {reporting_table}")
        except Exception:
            pass


def test_write_gold_to_export_table(spark, gold_metrics_data):
    """Test writing gold data to export table."""
    export_table = "test_export_table"
    
    try:
        write_gold_to_export_table(
            df=gold_metrics_data,
            export_table_name=export_table,
            spark=spark
        )
        
        # Verify table was created
        result = spark.read.table(export_table)
        assert result.count() == 2
        
    finally:
        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {export_table}")
        except Exception:
            pass


def test_write_gold_to_export_table_with_partition(spark, gold_metrics_data):
    """Test writing gold data to export table with partitioning."""
    export_table = "test_export_partitioned"
    
    try:
        write_gold_to_export_table(
            df=gold_metrics_data,
            export_table_name=export_table,
            spark=spark,
            partition_by=["pickup_zip"]
        )
        
        # Verify table was created
        result = spark.read.table(export_table)
        assert result.count() == 2
        
    finally:
        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {export_table}")
        except Exception:
            pass

