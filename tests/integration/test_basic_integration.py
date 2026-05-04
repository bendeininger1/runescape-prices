import pytest
import pyspark.sql.connect.session

# Basic test to make sure the Spark session is running
@pytest.mark.integration_test
def test_get_spark(spark):
  assert isinstance(spark, pyspark.sql.connect.session.SparkSession)