import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from joblib import dump
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerBehaviorAnalysis").getOrCreate()

# Load the dataset
data_path = "E-commerce Customer Behavior.csv"
customer_data = spark.read.csv(data_path, header=True, inferSchema=True)

# Drop unnecessary columns and handle missing values
customer_data = customer_data.drop("customer_id").na.drop()

# Encode categorical columns
string_cols = ['gender', 'city', 'membership_type', 'satisfaction_level']
indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid='skip').setHandleInvalid("keep") for col in string_cols]

# Assemble numeric and indexed columns into a feature vector
numeric_cols = ['age', 'total_spend', 'items_purchased', 'discount_applied', 'average_rating', 'days_since_last_purchase']
assembler = VectorAssembler(inputCols=numeric_cols + [f"{col}_index" for col in string_cols], outputCol="features")

# Scale the features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# Create a pipeline with preprocessing stages
pipeline = Pipeline(stages=indexers + [assembler, scaler])

# Split data into training and test sets
trainDF, testDF = customer_data.randomSplit([0.8, 0.2], seed=42)

# Fit and transform the data using the pipeline
pipeline_model = pipeline.fit(trainDF)
df_transformed_train = pipeline_model.transform(trainDF)
df_transformed_test = pipeline_model.transform(testDF)

# Save the preprocessing pipeline
pipeline_model.save('preprocessing_pipeline_model')

# Convert Spark DataFrames to NumPy arrays for Isolation Forest model
train_features = np.array(df_transformed_train.select('scaled_features').rdd.map(lambda x: x[0]).collect())
test_features = np.array(df_transformed_test.select('scaled_features').rdd.map(lambda x: x[0]).collect())

# Initialize and fit the Isolation Forest model
iso_forest = IsolationForest(random_state=42, contamination=0.05)
iso_model = iso_forest.fit(train_features)

# Predict anomalies in the test dataset
predictions = iso_model.predict(test_features)

# Identify indices of anomalies
anomaly_indices = np.where(predictions == -1)[0]

# Convert test DataFrame to Pandas for easier inspection
df_transformed_test = df_transformed_test.toPandas()
anomalies = df_transformed_test.iloc[anomaly_indices]
print("Number of anomalies detected:", anomalies.shape[0])

# Perform PCA for visualization
pca = PCA(n_components=2, random_state=42)
train_features_pca = pca.fit_transform(train_features)
test_features_pca = pca.transform(test_features)

# Plot results with normal points in blue and anomalies in red
plt.figure(figsize=(10, 7))
plt.scatter(test_features_pca[predictions == 1, 0], test_features_pca[predictions == 1, 1], c='blue', label='Normal')
plt.scatter(test_features_pca[predictions == -1, 0], test_features_pca[predictions == -1, 1], c='red', label='Anomaly')
plt.title('Isolation Forest Anomaly Detection (PCA)')
plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.legend()
plt.show()

# Save the Isolation Forest model
dump(iso_model, './isolation_forest.joblib')
print("Isolation Forest model saved as 'isolation_forest.joblib'")

# Stop Spark session
spark.stop()
