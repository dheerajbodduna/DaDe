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

    df = input_df.groupBy("purpose").agg(sum(col("not_fully_paid") / count("*"))).alias("default_rate")
    df = df.withColumn("default_rate", round((col("default_rate"), 2)))

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

    