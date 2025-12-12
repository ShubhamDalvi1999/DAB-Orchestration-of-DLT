"""
Incremental processing utilities for taxi data pipeline.
Contains functions for merge and append operations with deduplication.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as spark_max, current_timestamp, lit
from delta.tables import DeltaTable


def get_latest_timestamp(df: DataFrame, timestamp_col: str = "ingestion_timestamp") -> DataFrame:
    """
    Gets the latest timestamp from a DataFrame for incremental processing.
    
    Args:
        df: DataFrame to check
        timestamp_col: Name of the timestamp column
        
    Returns:
        DataFrame: Single row with max timestamp
    """
    return df.select(spark_max(col(timestamp_col)).alias("latest_timestamp"))


def filter_incremental(
    df: DataFrame, 
    target_table_path: str, 
    timestamp_col: str = "ingestion_timestamp",
    spark=None
) -> DataFrame:
    """
    Filters DataFrame to only include records newer than the latest in target table.
    For initial load, returns all records if target table doesn't exist.
    
    Args:
        df: Source DataFrame
        target_table_path: Path or table name of target Delta table
        timestamp_col: Name of the timestamp column to use for filtering
        spark: SparkSession instance (required for table operations)
        
    Returns:
        DataFrame: Filtered DataFrame with only new records
    """
    if spark is None:
        raise ValueError("SparkSession is required for incremental processing")
    
    try:
        # Try to read the target table to get latest timestamp
        target_df = spark.read.table(target_table_path)
        latest_timestamp = target_df.select(spark_max(col(timestamp_col))).collect()[0][0]
        
        if latest_timestamp is not None:
            # Filter to only new records
            return df.filter(col(timestamp_col) > latest_timestamp)
        else:
            # Table exists but is empty, return all records
            return df
    except Exception:
        # Table doesn't exist yet, return all records for initial load
        return df


def merge_incremental(
    target_table: DeltaTable,
    source_df: DataFrame,
    merge_keys: list,
    update_columns: list = None,
    insert_columns: list = None
) -> None:
    """
    Performs an incremental merge operation on a Delta table.
    
    Args:
        target_table: DeltaTable instance for the target table
        source_df: Source DataFrame with new/updated records
        merge_keys: List of column names to use for matching records
        update_columns: List of columns to update when match found (defaults to all non-key columns)
        insert_columns: List of columns to insert when no match (defaults to all columns)
    """
    if update_columns is None:
        # Get all columns except merge keys
        all_cols = source_df.columns
        update_columns = [col for col in all_cols if col not in merge_keys]
    
    if insert_columns is None:
        insert_columns = source_df.columns
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
    
    # Build update set
    update_set = {col: f"source.{col}" for col in update_columns}
    
    # Build insert set
    insert_set = {col: f"source.{col}" for col in insert_columns}
    
    # Perform merge
    (
        target_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )


def append_with_deduplication(
    df: DataFrame,
    target_table_path: str,
    deduplication_keys: list,
    spark=None
) -> None:
    """
    Appends DataFrame to target table with deduplication.
    Removes duplicates based on deduplication keys before appending.
    
    Args:
        df: Source DataFrame
        target_table_path: Path or table name of target Delta table
        deduplication_keys: List of column names to use for deduplication
        spark: SparkSession instance
    """
    if spark is None:
        raise ValueError("SparkSession is required for append operations")
    
    # Remove duplicates within the new data
    df_deduped = df.dropDuplicates(deduplication_keys)
    
    # Append to target table
    df_deduped.write.format("delta").mode("append").saveAsTable(target_table_path)

