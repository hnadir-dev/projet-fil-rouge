import findspark
findspark.init()


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, FloatType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, concat, lit, col, to_date, from_unixtime, to_utc_timestamp, to_timestamp, when

import logging

kafka_group_id = 'group-id'

kafka_config = {
    'group.id': kafka_group_id,
    "bootstrap.servers": "localhost:9092", 
}


def create_spark_configuration():
    spark_config = None
    try:
        spark_config = (SparkSession.builder
            .appName("ElasticsearchSparkIntegration")
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-20_2.12:7.17.14,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")
            .getOrCreate())
        
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_config



def connect_to_kafka(spark_config,topic):
    spark_df = None
    try:
        spark_df = spark_config.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .load()
        
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_order_from_kafka(spark_df):
    schema = StructType([
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("startDate", StringType(), True),
        StructField("new", StringType(), True),
        StructField("active", StringType(), True),
        StructField("activeUpdateDate", StringType(), True),
        StructField("regularPrice", StringType(), True),
        StructField("salePrice", StringType(), True),
        StructField("priceUpdateDate", StringType(), True),
        StructField("digital", StringType(), True),
        StructField("preowned", StringType(), True),
        StructField("url", StringType(), True),
        StructField("productTemplate", StringType(), True),
        StructField("customerReviewCount", StringType(), True),
        StructField("customerReviewAverage", StringType(), True),
        StructField("customerTopRated", StringType(), True),
        StructField("format", StringType(), True),
        StructField("freeShipping", StringType(), True),
        StructField("shippingCost", StringType(), True),
        StructField("specialOrder", StringType(), True),
        StructField("shortDescription", StringType(), True),
        StructField("pclass", StringType(), True),
        StructField("subclass", StringType(), True),
        StructField("department", StringType(), True),
        StructField("mpaaRating", StringType(), True),
        StructField("image", StringType(), True),
        StructField("condition", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("customerName", StringType(), True),
        StructField("customerAge", StringType(), True),
        StructField("customerGender", StringType(), True),
        StructField("customuerEmail", StringType(), True),
        StructField("customerCountry", StringType(), True),
        #StructField("location", StringType(), True),
        StructField("location", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ]), True),
        StructField("saleDate", StringType(), True),
        StructField("quantitySold", StringType(), True),
        StructField("paymentMethod", StringType(), True),
        StructField("totalPrice", StringType(), True)
        ])
    
    sel = (spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
        .withColumn('new',col('new').cast(BooleanType()))
        .withColumn('active',col('active').cast(BooleanType()))
        .withColumn('regularPrice',col('regularPrice').cast(FloatType()))
        .withColumn('salePrice',col('salePrice').cast(FloatType()))
        .withColumn('priceUpdateDate',to_date(col('priceUpdateDate')))
        .withColumn('digital',col('digital').cast(BooleanType()))
        .withColumn('preowned',col('preowned').cast(BooleanType()))
        .withColumn('customerReviewCount',when(col('customerReviewCount').isNotNull(),col('customerReviewCount').cast('int')).otherwise(0))
        .withColumn('customerReviewAverage',col('customerReviewAverage').cast('float'))
        .withColumn('customerTopRated',col('customerTopRated').cast("boolean"))
        .withColumn('freeShipping',col('freeShipping').cast(BooleanType()))
        .withColumn('shippingCost',col('shippingCost').cast(FloatType()))
        .withColumn('specialOrder',col('specialOrder').cast(BooleanType()))
        #
        .withColumn('customerAge',col('customerAge').cast(IntegerType()))
        #
        .withColumn('saleDate',to_timestamp(col('saleDate'),"MM/dd/yyyy HH:mm").cast(TimestampType()))
        .withColumn('quantitySold',col('quantitySold').cast(IntegerType()))
        .withColumn('totalPrice',col('totalPrice').cast(FloatType()))
        )
    
    return sel

#Configuration kafka
spark_config = create_spark_configuration()

#DataFrames
df_order = connect_to_kafka(spark_config=spark_config, topic='order-topic')

#Selections
sel_order = create_selection_df_order_from_kafka(df_order)

def write_to_elasticsearch(df,index):
    (df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.resource", f"{index}/_doc") \
        # .option("es.mapping.id", docId) \
        .save(mode="append"))


write_order_query = (sel_order.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/elasticsearch_orders/") \
    .foreachBatch(lambda batch_df, batch_id: write_to_elasticsearch(batch_df,'best-orders-index')) \
    .start())

write_order_query.awaitTermination()

#Debug
# streaming_es_query = sel_order.writeStream.outputMode("append").format("console").option("format", "json").start()

# streaming_es_query.awaitTermination()