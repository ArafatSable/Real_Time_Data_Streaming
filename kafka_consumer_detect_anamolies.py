from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.ml import PipelineModel
from kafka import KafkaProducer
from joblib import load
import numpy as np
import json
import logging

class EcommerceDataAnalysis:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.spark = SparkSession.builder.appName("EcommerceDataAnalysis") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .getOrCreate()
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

        self.pipelineModel = PipelineModel.load("./preprocessing_pipeline_model")
        self.clf = load('./isolation_forest.joblib')

    # Define schema for JSON data
    def get_json_schema(self):
        return StructType([
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

    # Function to preprocess data
    def preprocess_data(self, df):
        df = df.drop("customer_id")
        df = df.na.drop()
        pre_processed_df = self.pipelineModel.transform(df)
        return pre_processed_df

    # Function to detect anomalies
    def detect_anomalies(self, df, batch_id):
        print("Schema of batch DataFrame:")
        df.printSchema()
        print("Content of batch DataFrame:")
        df.show()

        # Adding unique IDs to each row in df for joining
        df_with_id = df.withColumn("id", monotonically_increasing_id())
        
        # Extracting scaled features for anomaly detection
        scaled_features_rdd = df_with_id.select('scaled_features').rdd.map(lambda x: x[0])

        if not scaled_features_rdd.isEmpty():
            # Collect the scaled features as a NumPy array for model prediction
            scaled_features_array = np.array(scaled_features_rdd.collect())
            predictions = self.clf.predict(scaled_features_array)

            # Create a predictions DataFrame with unique IDs
            predictions_df = self.spark.createDataFrame(
                [(int(pred),) for pred in predictions], ['anomaly']
            ).withColumn("id", monotonically_increasing_id())

            # Join based on the unique ID column and filter for anomalies
            anomaly_df = df_with_id.join(predictions_df, "id").filter(col("anomaly") == -1)

            print("Detected Anomalies:")
            anomaly_df.show()

            # Output anomalies directly to the console
            anomaly_df.write.format("console").save()
        else:
            print("scaled_features_array is empty. Skipping anomaly detection.")

    # Function to start streaming from Kafka
    def start_streaming(self):
        kafka_stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load()

        kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

        # Parse JSON data
        parsed_data_df = kafka_stream_df.select(from_json(col("value"), self.get_json_schema()).alias("data")).select("data.*")

        # Preprocess the data
        pre_processed_df = self.preprocess_data(parsed_data_df)

        # Apply anomaly detection within the streaming query
        anomaly_df = pre_processed_df \
            .writeStream \
            .foreachBatch(self.detect_anomalies) \
            .start()

        # Wait for the streaming query to terminate
        anomaly_df.awaitTermination()

if __name__ == "__main__":
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "kafka-topic"

    ecommerce_analysis = EcommerceDataAnalysis(kafka_bootstrap_servers, kafka_topic)
    ecommerce_analysis.start_streaming()

