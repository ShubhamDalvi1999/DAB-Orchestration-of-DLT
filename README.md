# Taxi DLT Bundle - Separated Orchestration and Transformations

This Databricks Asset Bundle demonstrates a clean separation between DLT orchestration and pure transformation functions, following Databricks' recommended best practices.

## Folder Structure

```
taxi_dlt_bundle/
├── databricks.yml              # Bundle configuration
├── pyproject.toml              # Package build configuration
├── pipelines/
│   └── main_pipeline.py        # DLT orchestration only (no transforms)
├── src/
│   └── taxi_dlt_pkg/           # Pure transformation package
│       ├── __init__.py
│       ├── ingestion.py       # Read operations
│       └── transforms.py      # Pure transform functions (no DLT decorators)
└── resources/
    └── taxi_data_pipeline.pipeline.yml  # Pipeline resource definition
```

## Architecture

### Separation of Concerns

1. **Orchestration** (`pipelines/main_pipeline.py`):
   - Contains only DLT table definitions (`@dlt.table`)
   - Defines data dependencies and expectations
   - Imports and calls transformation functions

2. **Transformations** (`src/taxi_dlt_pkg/`):
   - Pure Python functions that operate on DataFrames
   - No DLT decorators, no side effects
   - Reusable, testable, and packageable
   - Safe to use in multiple contexts

### Pipeline Flow

```
Bronze → Silver → Gold → Reporting
```

- **Bronze**: Raw ingestion from `samples.nyctaxi.trips`
- **Silver**: Cleaned data with calculated fields (fare_per_mile)
- **Gold**: Aggregated business metrics by pickup zone
- **Reporting**: Downstream reporting table for consumption (post-gold write operation)

## Building and Deploying

### 1. Build the Package

```bash
uv build --wheel
```

This creates `dist/taxi_dlt_pkg-0.1.0-py3-none-any.whl`

### 2. Deploy the Bundle

```bash
databricks bundle deploy
```

### 3. Run the Pipeline

```bash
databricks bundle run taxi_data_pipeline
```

## Key Features

### Incremental Processing
- **Automatic Incremental Processing**: DLT automatically tracks which records have been processed
- **Bronze Layer**: Processes only new records from source
- **Silver Layer**: Incrementally processes new records from bronze with deduplication
- **Gold Layer**: Incrementally updates aggregated metrics based on new silver data
- **Auto-Optimization**: Tables are configured with auto-optimize for better performance

### DDL SQL Support
- **Schema Definitions**: DDL statements for all tables in `src/taxi_dlt_pkg/ddl.py`
- **Table Creation**: Utility functions to execute DDL statements
- **Version Control**: DDL changes are tracked in source control

### Unit Testing
- **Comprehensive Tests**: Unit tests for all transformation functions
- **Pure Function Testing**: Tests run without DLT dependencies
- **Test Coverage**: Tests for data validation, transformations, and aggregations

## Key Benefits

1. **Testability**: Pure transform functions can be unit tested without DLT
2. **Reusability**: Transform functions can be used in notebooks, jobs, or other pipelines
3. **Maintainability**: Clear separation makes code easier to understand and modify
4. **Packaging**: Transformations are packaged as a wheel, following Databricks best practices
5. **Incremental Processing**: Efficient processing of only new/changed data
6. **DDL Management**: Version-controlled schema definitions

## Package Structure Details

### `src/taxi_dlt_pkg/ingestion.py`
Contains read operations for raw data sources. Functions return DataFrames without any DLT decorators.

### `src/taxi_dlt_pkg/transforms.py`
Contains pure transformation functions:
- `transform_bronze_to_silver()`: Cleans and enriches bronze data
- `transform_silver_to_gold()`: Aggregates metrics by zone

### `src/taxi_dlt_pkg/writes.py`
Contains write operations for DataFrames:
- `write_bronze_table()`: Write operations for bronze layer
- `write_silver_table()`: Write operations for silver layer with deduplication
- `write_gold_table()`: Write operations for gold layer aggregations
- `write_gold_to_reporting_table()`: Write gold data to reporting table
- `write_gold_to_export_table()`: Write gold data to export table (optionally partitioned)
- `write_table()`: Generic write function for any Delta table

### `src/taxi_dlt_pkg/ddl.py`
Contains DDL SQL statements and utilities:
- `BRONZE_TABLE_DDL`: Schema definition for bronze table
- `SILVER_TABLE_DDL`: Schema definition for silver table
- `GOLD_TABLE_DDL`: Schema definition for gold table
- `execute_ddl()`: Function to execute DDL statements
- `create_all_tables()`: Function to create all pipeline tables

### `pipelines/main_pipeline.py`
DLT orchestration file that:
- Defines table names and metadata
- Sets up data quality expectations
- Configures incremental processing with table properties
- Imports and calls transformation functions
- Manages data dependencies between layers

### `tests/`
Unit tests for the package:
- `test_transforms.py`: Tests for transformation functions
- `test_ingestion.py`: Tests for ingestion functions
- `test_ddl.py`: Tests for DDL execution

## Running Tests

### Local Testing (No Databricks Required)

The codebase supports local testing without Databricks Connect:

```bash
# Set environment variable to use local Spark
export USE_LOCAL_SPARK=1  # Linux/Mac
# or
$env:USE_LOCAL_SPARK=1    # Windows PowerShell

# Run all local tests
uv run pytest -m local

# Run all tests (will use local Spark if Databricks Connect unavailable)
uv run pytest
```

### Databricks Testing

```bash
# Run tests that require Databricks
uv run pytest -m databricks

# Run all tests (including Databricks-specific)
uv run pytest
```

See `README_TESTING.md` for detailed testing documentation.

## Using DDL SQL

You can execute DDL statements programmatically:

```python
from taxi_dlt_pkg.ddl import create_all_tables, execute_ddl, BRONZE_TABLE_DDL

# Create all tables
create_all_tables(spark)

# Or execute individual DDL
execute_ddl(spark, BRONZE_TABLE_DDL)
```

## Incremental Processing Details

DLT automatically handles incremental processing:
- **Bronze**: Tracks ingestion_timestamp to process only new records
- **Silver**: Processes only new records from bronze (DLT tracks dependencies)
- **Gold**: Updates aggregations incrementally based on new silver data

Table properties enable auto-optimization:
- `pipelines.autoOptimize.managed`: DLT manages optimization
- `delta.autoOptimize.optimizeWrite`: Optimizes writes automatically
- `delta.autoOptimize.autoCompact`: Automatically compacts small files

## Notes

- The wheel filename in `dist/` depends on your build tooling and versioning
- Pipeline libraries reference the wheel built from `./src`
- All transformation logic is isolated in the package, making it easy to test and reuse
- DLT incremental processing is automatic - no manual checkpointing needed
- DDL statements can be executed before pipeline runs to ensure schema consistency
