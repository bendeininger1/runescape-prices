# test_data_models.py

from utils.data_models import make_df_1m_price, make_df_1h_price
from pyspark.testing.utils import assertDataFrameEqual

def test_make_df_1m_price(spark):
    
    # Create a example json data
    data = {
        "data": {
			"2": {
				"high": 304,
				"highTime": 1775841793,
				"low": 300,
				"lowTime": 1775841765
			},
		}
	}
    # Create data frame using data_models util
    result_df = make_df_1m_price(spark, data)

    expected_data = [(2, 304, 1775841793, "high"),
                     (2, 300, 1775841765, "low")]
    
    schema = "id: int, price: int, time: bigint, highorlow: string"

    expected_df = spark.createDataFrame(expected_data, schema)

    assertDataFrameEqual(result_df, expected_df)

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