from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redis_server_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("ch", BooleanType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType( 
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType())   
            ]))                                      
        )
    ]
)

customer_json_schema = StructType (
    [
        StructField("customerName",StringType()),
        StructField("email",StringType()),
        StructField("phone",StringType()),
        StructField("birthDay",StringType())        
    ]
)

stedi_events_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", DateType())
    ]
)

spark = SparkSession.builder.appName("spark-app").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
redis_server_raw_streaming_df = spark.readStream.format("kafka"
	).option("kafka.bootstrap.servers", "localhost:9092"
	).option("subscribe", "redis-server"
	).option("startingOffsets", "earliest"
	).load()

# cast the value column in the streaming dataframe as a STRING 
redis_server_streaming_df = redis_server_raw_streaming_df.selectExpr("cast(key as string) key", "cast(value as string) value")

redis_server_streaming_df.withColumn("value", from_json("value", redis_server_schema)
	).select(col('value.*')
	).createOrReplaceTempView("RedisSortedSet")

# execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
encoded_streaming_df = spark.sql("select key, zSetEntries[0].element as customer from RedisSortedSet")

decoded_streaming_df = encoded_streaming_df.withColumn("customer", 
	unbase64(encoded_streaming_df.customer).cast("string"))

decoded_streaming_df.withColumn("customer", from_json("customer", customer_json_schema)
	).select(col('customer.*')
	).createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
email_bday_streaming_df = spark.sql("select email, birthDay from CustomerRecords where birthDay is not null")

# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
email_birth_year_streaming_df = email_bday_streaming_df.select('email', split(email_bday_streaming_df.birthDay,"-"
	).getItem(0).alias("birthYear"))

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
stedi_events_raw_streaming_df = spark.readStream.format("kafka"
	).option("kafka.bootstrap.servers", "localhost:9092"
	).option("subscribe", "stedi-events"
	).option("startingOffsets", "earliest"
	).load()
                                   
# cast the value column in the streaming dataframe as a STRING 
stedi_events_streaming_df = stedi_events_raw_streaming_df.selectExpr("cast(key as string) key", "cast(value as string) value")

stedi_events_streaming_df.withColumn("value", from_json("value", stedi_events_schema)
	).select(col('value.*')
	).createOrReplaceTempView("CustomerRisk")

# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customer_risk_sql_df = spark.sql("select customer, score from CustomerRisk")

# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 
joined_df = customer_risk_sql_df.join(email_birth_year_streaming_df, expr("""customer = email"""))

joined_df.selectExpr("cast(customer as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "customer_risk_score") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
