from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType


# the other zsetEntries is not necessary to be parsed
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

spark = SparkSession.builder.appName("redis-server-spark").getOrCreate()

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

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decoded_streaming_df.withColumn("customer", from_json("customer", customer_json_schema)
	).select(col('customer.*')
	).createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
email_bday_streaming_df = spark.sql("select email, birthDay from CustomerRecords where birthDay is not null")

# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
email_birth_year_streaming_df = email_bday_streaming_df.select('email', split(email_bday_streaming_df.birthDay,"-"
	).getItem(0).alias("birthYear"))


# sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
# 
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct 

email_birth_year_streaming_df.writeStream.outputMode("append"
	).format("console").start().awaitTermination()
