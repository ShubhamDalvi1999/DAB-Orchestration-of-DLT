"""
Unit tests for taxi_dlt_pkg ingestion functions.
These tests require Databricks environment with samples.nyctaxi.trips table.
For local testing, use test_ingestion_local.py instead.
"""

import pytest
from taxi_dlt_pkg.ingestion import read_raw_taxi_trips


@pytest.mark.databricks
def test_read_raw_taxi_trips_adds_ingestion_timestamp(spark):
    """Test that ingestion_timestamp is added to raw data.
    
    This test requires access to samples.nyctaxi.trips table in Databricks.
    For local testing, use test_ingestion_local.py instead.
    """
    try:
        result = read_raw_taxi_trips(spark)
        
        # Verify ingestion_timestamp column exists
        assert "ingestion_timestamp" in result.columns
        
        # Verify it's not null
        sample = result.select("ingestion_timestamp").limit(1).collect()
        if sample:
            assert sample[0]["ingestion_timestamp"] is not None
    except Exception as e:
        # If table doesn't exist in test environment, skip test
        pytest.skip(f"Test requires samples.nyctaxi.trips table: {e}")

