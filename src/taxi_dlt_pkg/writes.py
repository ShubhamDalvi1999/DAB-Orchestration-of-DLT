"""
Write operations for taxi data pipeline.
Contains functions to write DataFrames to Delta tables.
These functions are pure and can be used outside of DLT pipelines.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Optional
from delta.tables import DeltaTable


def write_bronze_table(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    merge_keys: Optional[list] = None,
    spark: Optional[SparkSession] = None
) -> None:
    """
    Writes DataFrame to bronze table.
    
    Args:
        df: DataFrame to write
        table_name: Name of the target bronze table
        mode: Write mode - "append", "overwrite", or "merge" (default: "append")
        merge_keys: List of columns to use for merge operation (required if mode="merge")
        spark: SparkSession instance (required for merge operations)
    """
    if mode == "merge":
        if merge_keys is None:
            raise ValueError("merge_keys must be provided for merge mode")
        if spark is None:
            raise ValueError("SparkSession is required for merge operations")
        
        # Perform merge operation
        target_table = DeltaTable.forName(spark, table_name)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        (target_table.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenNotMatchedInsertAll()
         .execute())
    else:
        # Standard write operation
        (df.write
         .format("delta")
         .mode(mode)
         .option("mergeSchema", "true")
         .saveAsTable(table_name))


def write_silver_table(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    merge_keys: Optional[list] = None,
    spark: Optional[SparkSession] = None
) -> None:
    """
    Writes DataFrame to silver table with deduplication support.
    
    Args:
        df: DataFrame to write
        table_name: Name of the target silver table
        mode: Write mode - "append", "overwrite", or "merge" (default: "append")
        merge_keys: List of columns to use for merge operation (required if mode="merge")
        spark: SparkSession instance (required for merge operations)
    """
    if mode == "merge":
        if merge_keys is None:
            raise ValueError("merge_keys must be provided for merge mode")
        if spark is None:
            raise ValueError("SparkSession is required for merge operations")
        
        # Perform merge operation (update existing, insert new)
        target_table = DeltaTable.forName(spark, table_name)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Get all columns except merge keys for update
        update_columns = [col for col in df.columns if col not in merge_keys]
        update_set = {col: f"source.{col}" for col in update_columns}
        
        (target_table.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenMatchedUpdate(set=update_set)
         .whenNotMatchedInsertAll()
         .execute())
    else:
        # Standard write operation with deduplication
        # Remove duplicates before writing
        if merge_keys:
            df_deduped = df.dropDuplicates(merge_keys)
        else:
            df_deduped = df
        
        (df_deduped.write
         .format("delta")
         .mode(mode)
         .option("mergeSchema", "true")
         .saveAsTable(table_name))


def write_gold_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    merge_keys: Optional[list] = None,
    spark: Optional[SparkSession] = None
) -> None:
    """
    Writes aggregated DataFrame to gold table.
    Gold tables are typically overwritten or merged since they contain aggregations.
    
    Args:
        df: DataFrame to write (aggregated data)
        table_name: Name of the target gold table
        mode: Write mode - "append", "overwrite", or "merge" (default: "overwrite")
        merge_keys: List of columns to use for merge operation (required if mode="merge")
        spark: SparkSession instance (required for merge operations)
    """
    if mode == "merge":
        if merge_keys is None:
            raise ValueError("merge_keys must be provided for merge mode")
        if spark is None:
            raise ValueError("SparkSession is required for merge operations")
        
        # Perform merge operation (update existing aggregations, insert new)
        target_table = DeltaTable.forName(spark, table_name)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Get all columns except merge keys for update
        update_columns = [col for col in df.columns if col not in merge_keys]
        update_set = {col: f"source.{col}" for col in update_columns}
        
        (target_table.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenMatchedUpdate(set=update_set)
         .whenNotMatchedInsertAll()
         .execute())
    else:
        # Standard write operation
        (df.write
         .format("delta")
         .mode(mode)
         .option("mergeSchema", "true")
         .saveAsTable(table_name))


def write_table(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    merge_keys: Optional[list] = None,
    spark: Optional[SparkSession] = None
) -> None:
    """
    Generic write function for any Delta table.
    
    Args:
        df: DataFrame to write
        table_name: Name of the target table
        mode: Write mode - "append", "overwrite", or "merge" (default: "append")
        merge_keys: List of columns to use for merge operation (required if mode="merge")
        spark: SparkSession instance (required for merge operations)
    """
    if mode == "merge":
        if merge_keys is None:
            raise ValueError("merge_keys must be provided for merge mode")
        if spark is None:
            raise ValueError("SparkSession is required for merge operations")
        
        target_table = DeltaTable.forName(spark, table_name)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        update_columns = [col for col in df.columns if col not in merge_keys]
        update_set = {col: f"source.{col}" for col in update_columns}
        
        (target_table.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenMatchedUpdate(set=update_set)
         .whenNotMatchedInsertAll()
         .execute())
    else:
        (df.write
         .format("delta")
         .mode(mode)
         .option("mergeSchema", "true")
         .saveAsTable(table_name))


def write_gold_to_reporting_table(
    df: DataFrame,
    reporting_table_name: str,
    spark: SparkSession,
    mode: str = "overwrite"
) -> None:
    """
    Writes gold layer data to a reporting table.
    This is typically used for downstream consumption or reporting purposes.
    
    Args:
        df: Gold layer DataFrame (aggregated metrics)
        reporting_table_name: Name of the reporting/export table
        spark: SparkSession instance
        mode: Write mode - "append", "overwrite", or "merge" (default: "overwrite")
    """
    write_gold_table(
        df=df,
        table_name=reporting_table_name,
        mode=mode,
        merge_keys=["pickup_zip"],  # Use pickup_zip as merge key for gold metrics
        spark=spark
    )


def write_gold_to_export_table(
    df: DataFrame,
    export_table_name: str,
    spark: SparkSession,
    partition_by: Optional[list] = None
) -> None:
    """
    Writes gold layer data to an export table, optionally partitioned.
    Useful for exporting data to external systems or for analytics.
    
    Args:
        df: Gold layer DataFrame
        export_table_name: Name of the export table
        spark: SparkSession instance
        partition_by: List of columns to partition by (optional)
    """
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
    )
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.saveAsTable(export_table_name)
