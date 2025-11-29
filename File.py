"""
FULL PYTHON DOCUMENTATION FOR DATA ENGINEERING + MACHINE LEARNING PIPELINE
"""

## Data Engineering with PySpark

# IMPORT DEPENDENCIES

# os and shutil are standard Python libraries.
# os → Interacting with file paths and environment variables.
# shutil → High-level file operations like copying or removing folders.
import os
import shutil

# pyspark → The main library for distributed data processing.
# SparkSession → Entry point for Spark execution engine.
# Window → Helps in applying window functions like running totals, rank, etc.
# `functions` → Contains column functions like col(), when(), lit(), sum(), etc.
# `types` → Contains schema types such as StringType, IntegerType, StructType, etc.
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# tracebook is not a standard library. Used in some training labs.
import tracebook


# EXTRACT FUNCTION

def read_data(spark, customSchema):
    """
    Reads CSV data from S3 into a Spark DataFrame using an explicit schema.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session used to read and process large-scale data.

    customSchema : StructType
        Explicit schema provided to avoid Spark inferring wrong data types.

    Why explicit schema?
    --------------------
    - Faster (Spark skips scanning for type inference)
    - Avoids mistakes (Spark often infers integers as strings)
    - Required for production pipelines

    Returns
    -------
    DataFrame
        Spark DataFrame containing the loaded CSV data.
    """

    print("------------------")
    print("Starting the read_data")
    print("------------------")

    bucket_name = "loan-data202511291100"
    s3_input_path = f"s3://{bucket_name}/inputfile/loan_data.csv"

    # NOTE: Your original code had a typo: "s3_inputs_path".
    df = spark.read.csv(s3_input_path, header=True, schema=customSchema)

    return df


# TRANSFORM FUNCTION

def clean_data(input_df):
    """
    Performs basic data cleaning:
    - Removes duplicates
    - Removes rows with null purpose values

    Parameters
    ----------
    input_df : DataFrame
        DataFrame returned from read_data()

    Issues in Original Code
    -----------------------
    - You wrote `input_file.drop()`, but it should be `input_df`.
    - `.drop()` without arguments deletes ALL columns, which is wrong.

    Returns
    -------
    DataFrame
        Cleaned DataFrame ready for further transformations.
    """

    print("------------------")
    print("Starting clean_data")
    print("------------------")

    df = input_df.dropDuplicates()

    # Remove rows where purpose is literally the string "null"
    df = df.filter(col("purpose") != "null")

    return df


# LOAD CLEANED DATA INTO S3

def s3_load_data(data, file_name):
    """
    Writes Spark DataFrame to S3 bucket as CSV.

    Parameters
    ----------
    data : DataFrame
        The DataFrame you want to save.

    file_name : str
        The name of the output file.

    Notes
    -----
    - data.count() triggers a full job, expensive for big data.
    - A better approach is data.rdd.isEmpty(), but Spark < 3.3 lacks it.
    """

    bucket_name = "loan-data202511291100"
    output_path = f"s3://{bucket_name}/output/{file_name}"

    if data.count() != 0:
        print("Loading the data", output_path)

        # coalesce(1) → reduces output to a single file (better for small datasets)
        data.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    else:
        print("Empty Dataframe, hence cannot save the data", output_path)

# SECOND TRANSFORMATION (FEATURE CREATION)

def result_1(input_df):
    """
    Creates engineered fields for risk scoring.

    Includes:
    - Filtering for educational + small_business loans
    - Creating income_to_installment_ratio
    - Categorizing interest rate
    - Identifying high-risk borrowers

    Returns
    -------
    DataFrame
    """

    print("------------------")
    print("Starting result_1")
    print("------------------")

    df = input_df.filter(
        (col("purpose") == "educational") |
        (col("purpose") == "small_business")
    )

    df = df.withColumn(
        "income_to_installment_ratio",
        col("log_annual_inc") / col("installment")
    )

    df = df.withColumn(
        "int_rate_category",
        when(col("int_rate") < 0.1, "low")
        .when((col("int_rate") >= 0.1) & (col("int_rate") < 0.15), "medium")
        .otherwise("high")
    )

    df = df.withColumn(
        "high_risk_borrower",
        when(
            (col("dti") > 20) |
            (col("fico") < 700) |
            (col("revol_util") > 80),
            1
        ).otherwise(0)
    )

    return df


# THIRD TRANSFORMATION (AGGREGATION)

def result_2(input_df):
    """
    Aggregates default rate per purpose.

    Issues in Original Code
    -----------------------
    - You wrote sum(col("not_fully_paid") / count("*")), which is wrong.
    - The correct formula for default rate:

        default_rate = SUM(not_fully_paid) / COUNT(*)

    - round() was incorrectly used.

    Returns
    -------
    DataFrame
    """

    print("------------------")
    print("Starting result_2")
    print("------------------")

    df = (
        input_df.groupBy("purpose")
        .agg((sum(col("not_fully_paid")) / count("*")).alias("default_rate"))
    )

    df = df.withColumn("default_rate", round(col("default_rate"), 2))

    return df


# LOAD INTO REDSHIFT TARGET

def redshift_load(data):
    """
    Loads Spark DataFrame into Redshift using JDBC connector.

    Notes:
    ------
    - Use IAM Role–based authentication for better security.
    - Hard-coding passwords is not recommended.

    """

    if data.count() != 0:
        print("Loading the data to redshift")

        jdbcUrl = (
            "jdbc:redshift://emr-spark-redshift.chrhumwuhiyle.us-east-1.redshift.amazonaws.com:5439/dev"
        )
        username = "awsuser"
        password = "Awsuser1"
        table_name = "result_2"

        data.write.format("jdbc") \
            .option("url", jdbcUrl) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .mode("overwrite") \
            .save()
    else:
        print("Empty dataframe, hence cannot load the data")


## Data Analytics in Sagemaker AI

"""
Below is the full ML pipeline:

1. Load cleaned data from S3
2. One-hot encode categorical features
3. Handle class imbalance using oversampling
4. Train a RandomForest model
5. Evaluate using classification report
6. Upload model to S3

Everything is documented inside the code.
"""

# IMPORTS

import pandas as pd
from sklearn.utils import resample
from sklearn.utils import shuffle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
import numpy as np
import warnings
import boto3
import joblib
import tempfile

from sagemaker import get_execution_role
warnings.filterwarnings('ignore')

role = get_execution_role()

# LOAD DATA

bucket_name = "loan-data202511291100"
folder_name = "loan_cleaned_data"
data_key = "loan_cleaned.csv"

data_location = f"s3://{bucket_name}/{folder_name}/{data_key}"

data = pd.read_csv(data_location)


# FEATURE ENGINEERING

data = pd.get_dummies(data, columns=["purpose"], dtype=int)


# HANDLE CLASS IMBALANCE

df_majority = data[data["not_fully_paid"] == 0]
df_minority = data[data["not_fully_paid"] == 1]

df_minority_upsampled = resample(
    df_minority,
    replace=True,
    n_samples=df_majority.shape[0],
    random_state=42
)

df_balanced = pd.concat([df_majority, df_minority_upsampled])
df_balanced = shuffle(df_balanced, random_state=42)


# TRAIN MODEL

X = df_balanced.drop("not_fully_paid", axis=1)
y = df_balanced["not_fully_paid"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.4, random_state=42
)

rf = RandomForestClassifier(random_state=42)
rf.fit(X_train, y_train)


# EVALUATE MODEL

y_pred = rf.predict(X_test)
print(classification_report(y_test, y_pred))


# SAVE MODEL

with tempfile.NamedTemporaryFile() as tmp:
    joblib.dump(rf, tmp.name)
    tmp.flush()

    s3 = boto3.client('s3')
    s3.upload_file(tmp.name, bucket_name, "model.pkl")

    print("Model saved to s3 bucket", f"s3://{bucket_name}/model.pkl")