Data Engineering Template (ETL: S3 → EMR/PySpark → S3 or Redshift)

# Imports

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()


# Read Data from S3

def read_s3_data(bucket_name, key, schema=None, file_format="csv"):
    s3_path = f"s3://{bucket_name}/{key}"
    if file_format == "csv":
        return spark.read.option("header", True).schema(schema).csv(s3_path)
    elif file_format == "parquet":
        return spark.read.parquet(s3_path)
    else:
        raise ValueError("Unsupported file format")

# Example schema (optional)
example_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# df = read_s3_data("my-bucket", "data/input.csv", example_schema)

# Transform Data

def transform_data(df):
    df_transformed = df.withColumn("amount_double", col("amount")*2) \
                       .withColumn("category", when(col("amount")>100, "High").otherwise("Low"))
    return df_transformed

# df_transformed = transform_data(df)


# Load Data to S3

def write_s3_data(df, bucket_name, key, file_format="parquet"):
    s3_path = f"s3://{bucket_name}/{key}"
    if file_format == "csv":
        df.write.mode("overwrite").option("header", True).csv(s3_path)
    elif file_format == "parquet":
        df.write.mode("overwrite").parquet(s3_path)
    else:
        raise ValueError("Unsupported file format")

# write_s3_data(df_transformed, "my-bucket", "data/output/")


# Load Data to Redshift

def write_redshift(df, jdbc_url, table_name, user, password):
    df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", table_name) \
      .option("user", user) \
      .option("password", password) \
      .mode("append") \
      .save()

Notes:

Use def functions for modularity.

Easy to replace S3 paths, schema, and transformations.

Redshift loading uses Spark JDBC.


Data Analytics Template (Boto3, SageMaker, Sklearn, Pandas, S3)


# Imports

import boto3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import joblib


# Read Data from S3

s3_client = boto3.client('s3')
bucket_name = "my-bucket"
key = "data/input.csv"

obj = s3_client.get_object(Bucket=bucket_name, Key=key)
df = pd.read_csv(obj['Body'])


# Basic EDA / Analytics

print(df.head())
print(df.describe())
print(df.info())


# Train/Test Split

X = df.drop(columns=['target'])
y = df['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# Train Model (Sklearn)

model = LinearRegression()
model.fit(X_train, y_train)


# Evaluate Model

y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")


# Save Model to S3

joblib.dump(model, "/tmp/model.joblib")
s3_client.upload_file("/tmp/model.joblib", bucket_name, "models/model.joblib")


# SageMaker (Optional for Advanced)

# Example: Create SageMaker session and upload data
import sagemaker
sagemaker_session = sagemaker.Session()
role = "arn:aws:iam::<account_id>:role/SageMakerRole"

# Upload data to S3 for SageMaker training
train_s3_path = sagemaker_session.upload_data(path="data/input.csv", key_prefix="training_data")

Notes:

Uses simple variables and Boto3 directly—no need for functions unless you want.

Covers the typical workflow: S3 → Pandas → ML → S3.

Can be extended for SageMaker training jobs.

---

⚡ Tips for the Challenge

1. Keep function templates ready (read, transform, write) for PySpark ETL.

2. For Analytics, load CSV quickly with Pandas and perform minimal preprocessing.

3. Use S3 paths and bucket variables; no hardcoding.

4. Test locally: Have a small sample CSV ready for quick verification.

5. Stick to standard AWS services: S3, EMR, Redshift, SageMaker.