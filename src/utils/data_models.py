#
from pyspark.sql.functions import lit, col

def make_df_1m_price(spark, data):
    """
    Make a price Dataframe from the raw ingested data.

    This is minute  price data
    Get the latest high and low prices for the items that we have data
    for, and the Unix timestamp when that transaction took place. 
    Map from itemId (see here for a reference) to an object of {high, highTime, low, lowTime}
    https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices#Latest_price_(all_items)

    Parameters
    ----------
    data: json
        raw json data containing prices for given items
        --1m price data example
        {
            "data": {
                "2": {
                    "high": 304,
                    "highTime": 1775841793,
                    "low": 300,
                    "lowTime": 1775841765
                },
                ...
            }
        }      

    Returns
    -------
    df : pyspark.sql.Dataframe
        latest
            id: int
            price: int
            time: bigint
            highorlow: string
            
    """

    # take the data from the raw json data
    item_data = data.get('data', {})

    # create df
    df = spark.createDataFrame(
        [
            (
                int(item_id),
                item.get("high", 0),
                item.get("highTime", 0),
                item.get("low", 0),
                item.get("lowTime", 0),
            )
            for item_id, item in item_data.items()
        ],
        schema = "id: int, high: int, highTime: bigint, low: int, lowTime: bigint",
    )

    # create and then union low and high price data for latest data into one df
    # create df with High price data
    df_high = df.select(
        "id",
        col("high").alias("price"),
        col("highTime").alias("time"))\
        .withColumn("highorlow", lit("high"))

    # create df with Low price data
    df_low = df.select(
        "id",
        col("low").alias("price"),
        col("lowTime").alias("time"))\
        .withColumn("highorlow", lit("low"))
    
    # union both dataframes
    output_df = df_high.union(df_low)

    return output_df

def make_df_1h_price(spark, data):
    """
    Make a price Dataframe from the raw ingested data.

    Gives hourly average of item high and low prices, and the number (volume) traded. 
    https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices#1-hour_prices

    Parameters
    ----------
    data: json
        raw json data containing prices for given items
        --1hour data example
        {
            "data": {
                "2": {
                    "avgHighPrice": 306,
                    "highPriceVolume": 737968,
                    "avgLowPrice": 302,
                    "lowPriceVolume": 181867
                },
                ...
            },
            "timestamp": 1775836800
        }

    Returns
    -------
    df : pyspark.sql.Dataframe
        1hour
            id: int
            avg1HourHigh: int
            avg1HourHighVolume: int
            avg1HourLow: int
            avg1HourLowVolume: int
            time: bigint
            
    """

    # take the data from the raw json data
    item_data = data.get('data', {})

    # create df
    df = spark.createDataFrame(
        [
            (
                int(item_id),
                item.get("avgHighPrice", 0),
                item.get("highPriceVolume", 0),
                item.get("avgLowPrice", 0),
                item.get("lowPriceVolume", 0),
            )
            for item_id, item in item_data.items()
        ],
        schema = "id: int, avg1HourHigh: int, avg1HourHighVolume: int, avg1HourLow: int, avg1HourLowVolume: int"
    )
    # add timestamp column
    output_df = df.withColumn("time", lit(data.get('timestamp')).cast("bigint"))

    return output_df