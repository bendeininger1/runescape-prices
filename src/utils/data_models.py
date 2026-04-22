import pyspark.sql.functions as sf

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
        sf.col("high").alias("price"),
        sf.col("highTime").alias("time"))\
        .withColumn("highorlow", sf.lit("high"))

    # create df with Low price data
    df_low = df.select(
        "id",
        sf.col("low").alias("price"),
        sf.col("lowTime").alias("time"))\
        .withColumn("highorlow", sf.lit("low"))
    
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
        {    # create and then union low and high price data for latest data into one df
    # create df with High price data
    df_high = df.select(
        "id",
        sf.col("high").alias("price"),
        sf.col("highTime").alias("time"))\
        .withColumn("highorlow", sf.lit("high"))

    # create df with Low price data
    df_low = df.select(
        "id",
        sf.col("low").alias("price"),
        sf.col("lowTime").alias("time"))\
        .withColumn("highorlow", sf.lit("low"))
    
    # union both dataframes
    output_df = df_high.union(df_low)

    return output_df
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
    output_df = df.withColumn("time", sf.lit(data.get('timestamp')).cast("bigint"))

    return output_df

def make_df_1h_price_last_enriched(spark, df_1h_prices_enriched):
    """
    Make a price Dataframe from enriched hourly price data from the silver layer with only a single record (latest)

    Parameters
    ----------
    df : pyspark.sql.Dataframe
        enriched hourly price data from the silver layer
        df_1h_prices_enriched
            id: int
            avg1HourHigh: int
            avg1HourHighVolume: int
            avg1HourLow: int
            avg1HourLowVolume: int
            time: bigint
            name: string
            highalch: int
            limit: int
            members: string
         


    Returns
    -------
    df : pyspark.sql.Dataframe
        df_1h_prices_last_enriched
            id: int
            avg1HourHigh: int
            avg1HourHighVolume: int
            avg1HourLow: int
            avg1HourLowVolume: int
            time: bigint
            name: string
            highalch: int
            limit: int
            members: string
            
    """

    # Create Aggregate Dataframe grouping by id to get the max time value for each id
    # this represents the "last" hourly price record for each given item
    df_latest_time = df_1h_prices_enriched.groupBy("id").max("time").withColumnRenamed("max(time)", "time")

    # Join the df_latest_time with the enriched hourly price data
    df_1h_prices_last_enriched = df_1h_prices_enriched.join(df_latest_time, ["id", "time"])

    # ReOrder columns
    df_1h_prices_last_enriched = df_1h_prices_last_enriched.select(
        'id',
        'avg1HourHigh',
        'avg1HourHighVolume',
        'avg1HourLow',
        'avg1HourLowVolume',
        'time',
        'name',
        'highalch',
        'limit',
        'members'
    )

    return df_1h_prices_last_enriched

def make_df_1m_price(spark, df_raw):
    """
    Make a price Dataframe from the raw ingested data frame.

    This is minute price data
    Get the latest high and low prices for the items that we have data
    for, and the Unix timestamp when that transaction took place. 
    Map from itemId (see here for a reference) to an object of {high, highTime, low, lowTime}
    https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices#Latest_price_(all_items)

    Parameters
    ----------
    df_raw
    +--------------------+
    |                data|
    +--------------------+
    |{{290, 1776281201...|
    +--------------------+

    underlying json dat is in this structure
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
        1m
            id: int
            price: int
            time: bigint
            highorlow: string 
            
    df example data
    df
    +--------------------+--------------------+--------------------+
    |                   2|                   6|                   8|
    +--------------------+--------------------+--------------------+
    |{290, 1776281201,...|{187838, 17762807...|{195680, 17762808...|
    +--------------------+--------------------+--------------------+

    df2
    +---+--------------------+
    | id|          price_data|
    +---+--------------------+
    |  2|{290, 1776281201,...|
    |  6|{187838, 17762807...|
    |  8|{195680, 17762808...|
    +---+--------------------+

    df3
    +---+------+----------+------+----------+
    | id|  high|  highTime|   low|   lowTime|
    +---+------+----------+------+----------+
    |  2|   290|1776281201|   282|1776281112|
    |  6|187838|1776280798|186735|1776281094|
    |  8|195680|1776280843|193345|1776281097|
    +---+------+----------+------+----------+
    """

    # Convert data into 1 column per ID with associated data nested
    df = df_raw.select('data.*')

    # Convert df into df with 1 record per id
    df2 = df.withColumn(
        "tab",
        sf.array(*[
            sf.struct(
                sf.lit(x).cast("int").alias("id"),
                sf.col(x).alias("price_data")
                ).alias(x)
            for x in df.columns]
        ),
    ).selectExpr("inline(tab)")

    # Convert df so columns are split
    df3 = df2.select('id', sf.inline(sf.array('price_data')))

    # Cast data types
    df3 = df3.select('id'
                     ,sf.col("high").cast('int')
                     ,sf.col("highTime").cast('bigint')
                     ,sf.col("low").cast('int')
                     ,sf.col("lowTime").cast('bigint'))

    # create and then union low and high price data for latest data into one df
    # create df with High price data
    df_high = df3.select(
        "id",
        sf.col("high").alias("price"),
        sf.col("highTime").alias("time"))\
        .withColumn("highorlow", sf.lit("high"))

    # create df with Low price data
    df_low = df3.select(
        "id",
        sf.col("low").alias("price"),
        sf.col("lowTime").alias("time"))\
        .withColumn("highorlow", sf.lit("low"))
    
    # union both dataframes
    output_df = df_high.union(df_low)

    # Drop any null values
    output_df = output_df.na.drop()

    return output_df