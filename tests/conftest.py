import sys
import os
import pytest

# Run the tests from the root directory
sys.path.append(os.getcwd())

# Returning a Spark Session
@pytest.fixture()
def spark():
    # When using databricks connect
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        # When using pyspark
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except:
            raise ImportError("Neither Databricks Session or Spark Session are available")
    return spark