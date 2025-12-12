"""
SQL transformation functions for taxi data pipeline.
Contains SQL queries that can be executed as transformations.
"""

from pyspark.sql import DataFrame, SparkSession


def transform_bronze_to_silver_sql(spark: SparkSession, table_name: str = "bronze_taxi_trips") -> str:
    """
    Returns SQL query to transform bronze taxi data to silver layer.
    
    Args:
        spark: SparkSession instance
        table_name: Name of the bronze table to read from
        
    Returns:
        str: SQL query string for silver transformation
    """
    return f"""
    SELECT 
        *,
        CASE 
            WHEN trip_distance > 0 THEN ROUND(fare_amount / trip_distance, 2)
            ELSE 0 
        END AS fare_per_mile
    FROM {table_name}
    WHERE fare_amount > 0
      AND trip_distance > 0
    """


def transform_silver_to_gold_sql(spark: SparkSession, table_name: str = "silver_taxi_trips") -> str:
    """
    Returns SQL query to transform silver taxi data to gold layer.
    
    Args:
        spark: SparkSession instance
        table_name: Name of the silver table to read from
        
    Returns:
        str: SQL query string for gold transformation
    """
    return f"""
    SELECT 
        pickup_zip,
        COUNT(*) AS total_trips,
        ROUND(SUM(fare_amount), 2) AS total_revenue,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(AVG(fare_per_mile), 2) AS avg_fare_per_mile,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance
    FROM {table_name}
    GROUP BY pickup_zip
    ORDER BY total_revenue DESC
    """


def execute_sql_transform(spark: SparkSession, sql_query: str) -> DataFrame:
    """
    Executes a SQL query and returns the result as a DataFrame.
    
    Args:
        spark: SparkSession instance
        sql_query: SQL query string to execute
        
    Returns:
        DataFrame: Result of the SQL query
    """
    return spark.sql(sql_query)

