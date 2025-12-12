"""
Taxi DLT Package - Pure transformation functions for NYC Taxi data pipeline.
These functions contain no DLT decorators and are reusable, testable transformations.

Modules:
- ingestion: Read operations for raw data sources
- transforms: Pure DataFrame transformation functions (bronze→silver, silver→gold)
- writes: Write operations for writing DataFrames to Delta tables
- sql_transforms: SQL-based transformation queries (alternative to DataFrame transforms)
- ddl: DDL SQL statements and table creation utilities
- incremental: Incremental processing utilities (optional, for advanced use cases)
"""

__version__ = "0.1.0"

from taxi_dlt_pkg import ingestion, transforms, writes, ddl, sql_transforms

__all__ = ["ingestion", "transforms", "writes", "ddl", "sql_transforms"]

