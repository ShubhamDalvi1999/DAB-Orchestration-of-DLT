"""
Ingestion functions for taxi data pipeline.
Contains read operations for raw data sources.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from typing import Optional


def read_raw_taxi_trips(spark, source_table: Optional[str] = None) -> DataFrame:
    """
    Reads raw NYC Taxi trips from a source table.
    
    Args:
        spark: SparkSession instance
        source_table: Name of the source table to read from.
                     Defaults to "samples.nyctaxi.trips" (Databricks samples catalog).
                     For local testing, provide a test table name or path.
        
    Returns:
        DataFrame: Raw taxi trips data with ingestion timestamp
    """
    table_name = source_table or "samples.nyctaxi.trips"
    
    return (
        spark.read.table(table_name)
        .withColumn("ingestion_timestamp", current_timestamp())
    )

