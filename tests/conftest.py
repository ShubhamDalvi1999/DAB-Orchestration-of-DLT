"""This file configures pytest.

This file is in the root since it can be used for tests in any place in this
project, including tests under resources/.
"""

import os, sys, pathlib
from contextlib import contextmanager


try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    import pytest
    import json
    import csv
    import os
except ImportError:
    raise ImportError(
        "Test dependencies not found.\n\nRun tests using 'uv run pytest'. See http://docs.astral.sh/uv to learn more about uv."
    )


def _create_local_spark() -> SparkSession:
    """Create a local SparkSession for testing without Databricks Connect."""
    from pyspark.sql import SparkSession as PySparkSession
    
    builder = (
        PySparkSession.builder
        .appName("taxi_dlt_pkg_tests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "file:./spark-warehouse")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    
    # Try to add Delta Lake extensions if available
    try:
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    except Exception:
        # Delta Lake not available, continue without it
        print("⚠️  Delta Lake extensions not available, continuing without Delta support", file=sys.stderr)
    
    return builder.getOrCreate()


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.
    
    Tries to use Databricks Connect if available, otherwise falls back to local Spark.
    Set environment variable USE_LOCAL_SPARK=1 to force local Spark.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    # Check if we should force local Spark
    if os.environ.get("USE_LOCAL_SPARK", "0") == "1":
        return _create_local_spark()
    
    # Try Databricks Connect first
    try:
        return DatabricksSession.builder.getOrCreate()
    except Exception:
        # Fall back to local Spark if Databricks Connect is not available
        print("⚠️  Databricks Connect not available, using local Spark for testing", file=sys.stderr)
        return _create_local_spark()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    with _allow_stderr_output(config):
        _enable_fallback_compute()

        # Initialize Spark session eagerly, so it is available even when
        # SparkSession.builder.getOrCreate() is used. For DB Connect 15+,
        # we validate version compatibility with the remote cluster.
        if hasattr(DatabricksSession.builder, "validateSession"):
            DatabricksSession.builder.validateSession().getOrCreate()
        else:
            DatabricksSession.builder.getOrCreate()
