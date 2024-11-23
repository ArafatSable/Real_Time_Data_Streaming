from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType

# Define schema for incoming data from Kafka
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("membership_type", StringType(), True),
    StructField("total_spend", FloatType(), True),
    StructField("items_purchased", IntegerType(), True),
    StructField("average_rating", FloatType(), True),
    StructField("discount_applied", BooleanType(), True),
    StructField("days_since_last_purchase", IntegerType(), True),
    StructField("satisfaction_level", StringType(), True)
])

# Initialize Spark Session with MongoDB configuration
spark = SparkSession.builder \
    .appName("EcommerceDataAnalysis") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/ecommerce.analytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                                    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data from Kafka
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Create age groups
ecommerce_df_age_groups = json_df.withColumn("age_group",
    expr("""
        CASE
            WHEN age < 35 THEN 'Under 35'
            WHEN age >= 35 AND age <= 50 THEN 'Between 35-50'
            ELSE 'Over 50'
        END
    """))

# Group by Membership Type, Age Group, and City
customer_segments = ecommerce_df_age_groups.groupBy("membership_type", "age_group", "city").agg(
    avg("total_spend").alias("avg_total_spend"),
    sum("items_purchased").alias("total_items_purchased")
)

# Group by City separately
city_segments = ecommerce_df_age_groups.groupBy("city").agg(
    avg("total_spend").alias("avg_total_spend"),
    sum("items_purchased").alias("total_items_purchased")
)

# Identify at-risk customers
at_risk_customers = json_df.filter((json_df["days_since_last_purchase"] > 30) & (json_df["satisfaction_level"] == "Unsatisfied")) \
    .select("customer_id", "city", "total_spend", "days_since_last_purchase", "satisfaction_level")

# Function to write each micro-batch to MongoDB
def write_to_mongo(batch_df, epoch_id, collection_name):
    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", "ecommerce") \
        .option("collection", collection_name) \
        .save()

# Set up streaming query for customer segments
query1 = customer_segments.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: write_to_mongo(df, epoch_id, "customer_segments")) \
    .option("checkpointLocation", "/tmp/checkpoints/customer_segments") \
    .start()

# Set up streaming query for city segments
query2 = city_segments.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: write_to_mongo(df, epoch_id, "city_segments")) \
    .option("checkpointLocation", "/tmp/checkpoints/city_segments") \
    .start()

# Set up streaming query for at-risk customers
query3 = at_risk_customers.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: write_to_mongo(df, epoch_id, "at_risk_customers")) \
    .option("checkpointLocation", "/tmp/checkpoints/at_risk_customers") \
    .start()

# Await termination for all streams
spark.streams.awaitAnyTermination()
