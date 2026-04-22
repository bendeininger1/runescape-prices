import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .getOrCreate()

#catalog = spark.conf.get("catalog")
catalog = "runescape_dev"

# path of json files
path = f"/Volumes/{catalog}/00_landing/data_sources/latest_prices/"

df_raw = (spark.readStream
            #.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(path)
            .select("*"))

df = df_raw.select('data.*')
df2 = df.withColumn(
    "tab",
    sf.array(
        *[
            sf.struct(sf.lit(x).alias("id"), sf.col(x).alias("Count")).alias(x)
            for x in df.columns
        ]
    ),
).selectExpr("inline(tab)")

df3 = df2.select('id', sf.inline(sf.array('count')))

df3.display()
