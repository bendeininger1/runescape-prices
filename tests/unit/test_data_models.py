# test_data_models.py
import pytest
from utils.data_models import make_df_1m_price, make_df_1h_price, make_df_1h_price_last_enriched
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DoubleType

@pytest.mark.unit_test
def test_make_df_1h_price(spark):
    
    # Create a example json data
    data = {
        "data": {
            "2": {
                "avgHighPrice": 306,
                "highPriceVolume": 737968,
                "avgLowPrice": 302,
                "lowPriceVolume": 181867
            },
        },
        "timestamp": 1775836800
    }
    # Create data frame using data_models util
    result_df = make_df_1h_price(spark, data)

    expected_data = [(2, 306, 737968, 302, 181867, 1775836800)]
    
    schema = "id: int, avg1HourHigh: int, avg1HourHighVolume: int, avg1HourLow: int, avg1HourLowVolume: int, time: bigint"

    expected_df = spark.createDataFrame(expected_data, schema)

    assertDataFrameEqual(result_df, expected_df)

@pytest.mark.unit_test
def test_make_df_1h_price_last_enriched(spark):

    # Create example data input
    # TODO consider how slowly changing dimension implementation for item_mapping impacts the data below
    # Possibly drop these columns in assertion?
    result_data = [(2, 292, 858295, 286, 397570, 1776117600, "Steel cannonball", 3, 11000, "true"),
                   (2, 310, 1189874, 303, 490467, 1775498400, "Steel cannonball", 3, 11000, "true"),
                   (2, 297, 950064, 292, 337742, 1775163600, "Steel cannonball", 3, 11000, "true"),
                   (2, 296, 641262, 293, 222330, 1775167200, "Steel cannonball", 3, 11000, "true"),
                   (39, 3, 26860, 2, 50, 1776117600, "Bronze arrowtips", 1, 10000, "true"),
                   (39, 4, 13323, 4, 17240, 1775498400, "Bronze arrowtips", 1, 10000, "true"),]

    schema_1h_prices_enriched = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("avg1HourHigh", IntegerType(), True),
            StructField("avg1HourHighVolume", IntegerType(), False),
            StructField("avg1HourLow", IntegerType(), True),
            StructField("avg1HourLowVolume", IntegerType(), False),
            StructField("time", LongType(), False),
            StructField("name", StringType(), False),
            StructField("highalch", IntegerType(), True),
            StructField("limit", IntegerType(), True),
            StructField("members", StringType(), False)
        ]
    )

    # expected_data is the most recent
    expected_data = [(2, 292, 858295, 286, 397570, 1776117600, "Steel cannonball", 3, 11000, "true"),
                     (39, 3, 26860, 2, 50, 1776117600, "Bronze arrowtips", 1, 10000, "true")]

    schema_1h_prices_last_enriched = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("avg1HourHigh", IntegerType(), True),
            StructField("avg1HourHighVolume", IntegerType(), False),
            StructField("avg1HourLow", IntegerType(), True),
            StructField("avg1HourLowVolume", IntegerType(), False),
            StructField("time", LongType(), False),
            StructField("name", StringType(), False),
            StructField("highalch", IntegerType(), True),
            StructField("limit", IntegerType(), True),
            StructField("members", StringType(), False)
        ]
    )

    df_1h_prices_enriched = spark.createDataFrame(result_data, schema_1h_prices_enriched)

    # Create data frame using data_models util
    result_df = make_df_1h_price_last_enriched(spark, df_1h_prices_enriched)

    expected_df = spark.createDataFrame(expected_data, schema_1h_prices_last_enriched)

    assertDataFrameEqual(result_df, expected_df)

@pytest.mark.unit_test
def test_make_df_1m_price(spark):
    
    # Create a example json data
    json_data = '''{
	"data": {
            "2": {
                "high": 290,
                "highTime": 1776281201,
                "low": 282,
                "lowTime": 1776281112
            },
            "6": {
                "high": 187838,
                "highTime": 1776280798,
                "low": 186735,
                "lowTime": 1776281094
            },
            "8": {
                "high": 195680,
                "highTime": 1776280843,
                "low": 193345,
                "lowTime": 1776281097
            }...
        }
    }'''
    # Create data frame
    df_raw = spark.read.json("tests/unit/data/1m_prices_test.json", multiLine =True)

    # Create data frame using data_models util
    result_df = make_df_1m_price(spark, df_raw)

    expected_data = [(2, 290, 1776281201, "high"),
                     (2, 282, 1776281112, "low"),
                     (6, 187838, 1776280798, "high"),
                     (6, 186735, 1776281094, "low"),
                     (8, 195680, 1776280843, "high"),
                     (8, 193345, 1776281097, "low"),
                     (2660, 50000, 1618883126, "high")]
    
    schema = "id: int, price: int, time: bigint, highorlow: string"

    expected_df = spark.createDataFrame(expected_data, schema)

    assertDataFrameEqual(result_df, expected_df, checkRowOrder=False)
    