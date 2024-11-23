# Real-Time Data Streaming

This project demonstrates a real-time ⏳ data streaming pipeline 📦 using **⚒️ Hadoop**, **💡 Kafka**, **✨ Spark**, and **📊 MongoDB** to analyze and process 🛒 customer behavior data effectively.

## Features
- **💡 Kafka**: Acts as the message broker 🚚 to produce and consume 🛒 data streams.
- **✨ Spark**: Processes streaming data in real-time ⏳ to derive insights and detect ⚠️ anomalies.
- **📊 MongoDB**: Stores processed data for future ⏲ analysis and 📈 reporting.
- **⚒️ Hadoop**: Provides distributed 🌐 storage for large-scale 📦 data storage and processing.

## Project Structure
```
├── 🛒 E-commerce Customer Behavior.csv           # Sample dataset 📊 for e-commerce behavior
├── 📈 E-commerce_Customer_Behavior_reports.ipynb # Analysis 📊 and visualization notebook 🎨
├── 🔄 kafka_consumer_ecommerce_to_console.py     # Kafka 🚚 consumer script to log data 🔄 to console
├── ⚠️ kafka_consumer_detect_anamolies.py         # Kafka consumer script for ⚠️ anomaly detection
├── 🔄 kafka_consumer_ecommerce_to_md.py          # Kafka consumer to process data 🔄 and save to 📊 MongoDB
├── 🚚 kafka_producer_ecommerce.py                # Kafka 🚚 producer script for 🛒 data
├── 🔄 model.py                                   # Contains 🎨 machine learning models for analytics
├── 🎨 Web/                                       # Folder for potential web interface
```

## Prerequisites

1. **💡 Kafka**
   - Install Apache Kafka and ensure 🔧 Zookeeper is running.
   - Create the required ✨ topics for the 🛒 data stream.

2. **⚒️ Hadoop**
   - Install and configure ⚒️ Hadoop on your system for distributed 🌐 data storage.
   - Ensure HDFS is set up and running.

3. **✨ Spark**
   - Install Apache ✨ Spark for real-time data ⏳ processing.
   - Ensure it is integrated with ⚒️ Hadoop (if using HDFS).

4. **📊 MongoDB**
   - Install 📊 MongoDB and set up the database 🔧 for storing processed data.

5. **🔄 Python Libraries**
   Install the required 🎨 Python libraries:
   ```
   pip install kafka-python pyspark pymongo matplotlib pandas numpy
   ```

## Workflow

### 1. Kafka Producer
- **✨ Script**: `kafka_producer_ecommerce.py`
- Reads the `🛒 E-commerce Customer Behavior.csv` file and sends data to 🔄 Kafka topics.

### 2. Kafka Consumers
- **⏳ To Console**: `kafka_consumer_ecommerce_to_console.py` logs data to the console 🔄.
- **To 📊 MongoDB**: `kafka_consumer_ecommerce_to_md.py` processes and saves data into 📊 MongoDB.
- **⚠️ Anomaly Detection**: `kafka_consumer_detect_anamolies.py` identifies unusual patterns in the 🛒 data stream.

### 3. ✨ Spark Processing
- Stream data 🔄 from Kafka topics using ✨ Spark Streaming.
- Perform transformations and actions to derive meaningful 📊 insights.

### 4. 📊 MongoDB Storage
- Save processed and cleaned data 🔄 for further use and visualization 🎨.

### 5. Analysis and Visualization
- Use the `E-commerce_Customer_Behavior_reports.ipynb` notebook for 📊 exploratory data analysis (✨ EDA) and visualizations.

## Steps to Run

1. Start 🔧 Zookeeper and Kafka:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```
2. Create 🔧 Kafka topics:
   ```bash
   bin/kafka-topics.sh --create --topic ecommerce_data --bootstrap-server localhost:9092
   ```
3. Start the Kafka producer:
   ```bash
   python kafka_producer_ecommerce.py
   ```
4. Start the Kafka consumers:
   ```bash
   python kafka_consumer_ecommerce_to_console.py
   python kafka_consumer_detect_anamolies.py
   python kafka_consumer_ecommerce_to_md.py
   ```
5. Run ✨ Spark streaming jobs:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_processing.py
   ```
6. Analyze data in 📊 MongoDB using tools like MongoDB Compass or integrate with visualization tools.

## Use Cases
- Real-time ⏳ anomaly detection in 🛒 transactions.
- 🎨 Customer behavior analysis for targeted 🌈 marketing.
- 📈 Data aggregation and reporting for ✨ business intelligence.

## Technologies Used
- **⚒️ Hadoop**: Distributed 🌐 storage and processing.
- **💡 Kafka**: Message brokering and real-time data streaming ⏳.
- **✨ Spark**: Real-time data ⏳ processing.
- **📊 MongoDB**: NoSQL database for storing processed data.
- **🔄 Python**: Programming language 🔧 for building the pipeline.

## Author
Arafat Sable

Feel free to contribute 🌍 or provide suggestions for improvements 📢!

