from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType


stedi_events_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", DateType())
    ]
)

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
spark = SparkSession.builder.appName("human-balance").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_streaming_df = spark.readStream.format("kafka"
	).option("kafka.bootstrap.servers", "localhost:9092"
	).option("subscribe", "stedi-events"
	).option("startingOffsets", "earliest"
	).load()

# cast the value column in the streaming dataframe as a STRING 
customer_risk_streaming_df = raw_streaming_df.selectExpr("cast(key as string) key", "cast(value as string) value")

# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
# sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 

customer_risk_streaming_df.withColumn("value", from_json("value", stedi_events_schema)
	).select(col('value.*')
	).createOrReplaceTempView("CustomerRisk")

customer_risk_sql_df = spark.sql("select customer, score from CustomerRisk")

customer_risk_sql_df.writeStream.outputMode("append").format("console").start().awaitTermination()

