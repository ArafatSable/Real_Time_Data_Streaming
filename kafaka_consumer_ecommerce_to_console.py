from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

class EcommerceDataAnalysis:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.spark = SparkSession.builder.appName("EcommerceDataAnalysis") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .getOrCreate()
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

    # Define JSON schema
    def define_schema(self):
        return StructType([
            StructField("customer_id", IntegerType()),
            StructField("gender", StringType()),
            StructField("age", IntegerType()),
            StructField("city", StringType()),
            StructField("membership_type", StringType()),
            StructField("total_spend", FloatType()),
            StructField("items_purchased", IntegerType()),
            StructField("average_rating", FloatType()),
            StructField("discount_applied", BooleanType()),
            StructField("days_since_last_purchase", IntegerType()),
            StructField("satisfaction_level", StringType())
        ])

    def start_streaming(self):
        # Read data from Kafka topic
        kafka_stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load()

        # Convert the value column from Kafka into a string
        kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS string)")

        # Parse JSON data using the defined schema
        parsed_data_df = kafka_stream_df.select(from_json(col("value"), self.define_schema()).alias("data")).select("data.*")

        # Filtering customers at risk based on Days Since Last Purchase and Satisfaction Level
        at_risk_customers = parsed_data_df.filter(
            (parsed_data_df["days_since_last_purchase"] > 30) & 
            (parsed_data_df["satisfaction_level"] == "Unsatisfied")
        )
        
        at_risk_customers = at_risk_customers.select(
            "customer_id",
            "city",
            "total_spend",
            "days_since_last_purchase",
            "satisfaction_level"
        )

        # Output the at-risk customers to the console
        insight_3 = at_risk_customers.writeStream \
            .outputMode("update") \
            .option("truncate", "false") \
            .format("console") \
            .start()
        insight_3.awaitTermination()

if __name__ == "__main__":
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "kafka-topic"

    ecommerce_analysis = EcommerceDataAnalysis(kafka_bootstrap_servers, kafka_topic)
    ecommerce_analysis.start_streaming()
