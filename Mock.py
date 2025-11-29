## Data Engineering Hands on:

# Import Dependencies

import os
import shutil
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import tracebook

# Extract Data 

def read_data(spark, customSchema):
    '''
    spark_session : Spark
    customSchema : We have given the custom schema
    '''

    print("------------------")
    print("Starting the read_data")
    print("------------------")

    # Mention the bucket name inside the bucket name variable. 
    bucket_name = "loan-data202511291100" 
    s3_input_path = "s3://"+ bucket_name +"/inputfile/loan_data.csv"
    df = spark.read.csv(s3_inputs_path, header=True, schema=customSchema)

    return df

# Transform Data

def clean_data(input_df):
    '''
    For input file: input_df is the output of read_data function
    '''
    
    print("------------------")
    print("Starting clean_data")
    print("------------------")

    df = input_file.drop().dropDuplicates()
    df = df.filter((col("purpose") != "null"))

    return df

# Load Data

def s3_load_data(data, file_name):
    '''
    data : the output data of result_1 and result_2 function 
    file_name : the name of the output to be stored inside the s3
    '''

    # Mention the bucket name inside the bucket name variable 
    bucket_name = "loan-data202511291100" 
    output_path = "s3://"+ bucket_name +"/output"+ file_name

    if data.count() !=0:
        print("Loading the data", output_path)
        # Write the s3 load data command here
        data.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

    else:
        print("Empty Dataframe, hence cannot save the data", output_path)

# Second Transformation 

def result_1(input_df):
    '''
    For input file: input_dfis output of clean_data function
    '''
    
    print("------------------")
    print("Starting result_1")
    print("------------------")

    df = input_df.filter(((col("purpose")=="educational ") | (col("purpose") == "small_business")))
    df = df.withColumn("income_to_installment_ratio", col("log_annual_inc") / col("installment"))
    df = df.withColumn("int_rate_category", when(col("int_rate")<0.1, "low").when((col("int_rate")>=0.1)&(col("int_rate")<0.15), "medium").otherwise("high"))
    df = df.withColumn("high_risk_borrower", when((col("dti")>20) | (col("fico")<700) | (col("revol_util")>80), 1).otherwise(0))
    
    return df

# Third Transformation 

def result_2(input_df):
    '''
    For input_file: input_df is the output of clean_data function
    '''
   
    print("------------------")
    print("Starting result_2")
    print("------------------")

    df = input_df.groupBy("purpose").agg((sum(col("not_fully_paid") / count("*"))).alias("default_rate")
    df = df.withColumn("default_rate", round(col("default_rate"), 2))

    return df


# Connect to target - Redshift

df redshift_load(data):
    if data.count() != 0:
        print("Loading the data to redshift")
    
        jdbcUrl = "jdbc:redshift://emr-spark-redshift.chrhumwuhiyle.us-east-1.redshift.amazonaws.com:5439/dev" 
        username = "awsuser" 
        password = "Awsuser1" 
        table_name = "result_2" 

        # Write the redshift load command here
        data.write.format("jdbc").option("url", jdbcUrl).option("dbtable", table_name).option("user", username).option("password", password).mode("overwrite").save()


    else:
        print("Empty dataframe,hence cannot load the data")


# Main Code Here : [def main()] 


## Machine Learning Hands on:

# Import Dependencies 

import pandas as pd
from sklearn.utils import resample 
from sklearn.utils import shuffle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report

import warnings
import boto3
from sagemaker import get_execution_role

warnings.filterwarnings('ignore')

# Step 1 - Data Loading
# Hint: sample s3 URL - "s3://bucket_name/folder_name/file_name.csv" 

import numpy as np
import pandas as pd
from sklearn import datasets
import sagemaker
from sagemaker import get_execution_role
role = get_execution_role()

bucket_name = "loan-data202511291100" 
folder_name = "loan_cleaned_data" 
data_key = "loan_cleaned.csv" 

data_location = f"s3://{bucket_name}/{folder_name}/{data_key}"

data = pd.read_csv(data_location)
data.head()

# Step 2 - Feature Engineering 

data = pd.get_dummies(data, columns=["purpose"], dtype=int)
data.head()

# Step 3 - Data preprocessing 

from sklearn.utils import resample 
from sklearn.utils import shuffle

print(data["not_fully_paid"].value_counts())

df_majority = data[data["not_fully_paid"]==0]
df_minority = data[data["not_fully_paid"]==1]

# Handle the imbalanced data using resample method and oversample the minority class.
df_minority_upsampled = resample(df_minority, replace=True, n_samples= df_majority.shape[0], random_state=42)

# Concatenate the upsampled data records with the majority class records and shuffle the resultant dataframe
df_balanced = pd.concat([df_majority, df_minority_unsampled])
df_balanced = shuffle(df_balanced, random_state=42)
print(df_balanced['not_fully_paid].value_counts())

# Step 4 - Model training 

# Create X and y for Train-Test split

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier 

X = df_balanced.drop("not_fully_paid", axis=1)
y = df_balaced["not_fully_paid"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)

# Train a RandomForestClassifier Model
rf = RandomForestClassifier(random_state=42)
rf.fit(X_train, y_train)

# Step 5 - Model evaluation 

# Predict using the training RandomForestClassifier Model
from sklearn.metrics import classification_report
y_pred = rf.predict(X_test)

# Print the Classification report 
print(classification_report(y_test, y_pred))

# Step 6 - Saving the model to s3

# Uploading the model to s3 using boto3
import boto3
import joblib
import tempfile

with tempfile.NamedTemporaryFile() as tmp:
    joblib.dump(rf, tmp.name)
    tmp.flush()

    s3 = boto3.client('s3')
    s3.upload_file(tmp.name, bucket_name, "model.pkl")
    print("Model saved to s3 bucket", f"s3://{bucket_name}/model.pkl")

