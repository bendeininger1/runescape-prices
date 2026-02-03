from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType




# Create a Delta table sink for writing data to 'runescape.01_bronze.latest_prices_raw'
dp.create_sink(
  name = "delta_sink",
  format = "delta",
  options = { "tableName": "runescape.01_bronze.latest_prices_raw" }
)

# create path and schema for table
path = "/Volumes/runescape/00_landing/data_sources/latest_prices_transformed/"

schema = StructType([
StructField("id", IntegerType(), True),
StructField("price", IntegerType(), True),
StructField("time", LongType(), True),
StructField("highorlow", StringType(), True)
])

# TODO decide if this table is even needed
@dp.table(
  comment="Raw latest price data for runescape GE"
)
def latest_prices_raw_stream():
    """
    Reads the latest prices parquet files as a streaming source.
    """
    return (
        spark.readStream
            .schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(path)
            .select("*")
    )

# write data to sink with append flow, writing data to 'runescape.01_bronze.latest_prices_raw'
@dp.append_flow(name = "delta_sink_flow", target="delta_sink")
def delta_sink_flow():
  return(
  spark.readStream.table("latest_prices_raw_stream")
)

  


'''
with tempfile.TemporaryDirectory(prefix="writeStream") as d:
    # Create a table with Rate source.
    query = df.writeStream.toTable(
        "my_table", checkpointLocation=d)
    time.sleep(3)
    query.stop()
'''