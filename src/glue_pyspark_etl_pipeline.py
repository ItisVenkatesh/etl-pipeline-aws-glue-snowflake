import boto3
import json
import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import config


# Fetch Snowflake credentials from AWS Secrets Manager
def get_snowflake_credentials(secret_name):
    # Create a Secrets Manager client
    secrets_client = boto3.client('secretsmanager', region_name='us-west-2')  # Update region if needed
    
    try:
        # Retrieve the secret value
        secret_value = secrets_client.get_secret_value(SecretId=secret_name)
        
        # Parse the secret value as JSON
        secret = json.loads(secret_value['SecretString'])
        
        return {
            "sf_account": secret['sf_account'],
            "sf_user": secret['sf_user'],
            "sf_password": secret['sf_password'],
            "sf_database": secret['sf_database'],
            "sf_schema": secret['sf_schema'],
            "sf_warehouse": secret['sf_warehouse'],
            "sf_role": secret.get('sf_role', None)  # Optional: Handle if role is not present
        }
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise


# Load data from S3 using Glue DynamicFrame
def load_data_from_s3():
    # Create a Glue DynamicFrame from the JSON file in S3
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"{config.S3_INPUT_PATH}"]},
        format="json"
    )
    return dynamic_frame

# Load data into Snowflake
def load_data_into_snowflake(dynamic_frame):
    # Convert DynamicFrame to Pyspark DataFrame for writing to Snowflake
    data_frame = dynamic_frame.toDF()
    # Fetch Snowflake credentials from Secrets Manager
    secret_name = config.SNOWFLAKE_SECRET_NAME
    sf_credentials = get_snowflake_credentials(secret_name)

    # Snowflake connection options using fetched credentials
    sf_options = {
        "sfURL": f"{sf_credentials['sf_account']}.snowflakecomputing.com",
        "sfDatabase": sf_credentials['sf_database'],
        "sfSchema": sf_credentials['sf_schema'],
        "sfWarehouse": sf_credentials['sf_warehouse'],
        "sfRole": sf_credentials.get('sf_role', ""),  # Optional: If you use a specific role
        "sfUser": sf_credentials['sf_user'],
        "sfPassword": sf_credentials['sf_password']
    }

    # Write data to Snowflake
    data_frame.write.format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "your_snowflake_table") \
        .mode("overwrite") \
        .save()

# ETL Process: Extract, Transform, Load
def etl_process():

    # Step 1: Load Data into Glue DynamicFrame
    print("Loading data from S3 into Glue DynamicFrame...")
    glue_dynamic_frame = load_data_from_s3()

    # Step 2: Perform transformations (example: selecting specific columns)
    # You can add transformations as per your need
    select_dynamic_frame = glue_dynamic_frame.select_fields(["column1", "column2"])
    # Convert Glue DynamicFrame to PySpark DataFrame
    transformed_df = select_dynamic_frame.toDF()
    # Do sample filter transformation
    transformed_df = transformed_df.filter(col['column1'] > 30)
    # Convert the transformed PySpark DataFrame back to Glue DynamicFrame
    transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "dyf_transformed")

    # Step 3: Load Data into Snowflake
    print("Loading data into Snowflake...")
    load_data_into_snowflake(transformed_dynamic_frame)
    
    print("ETL Process Completed.")

# Run the ETL Process
if __name__ == "__main__":
    # Initialize Spark session and Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Initialize the Glue Job
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    etl_process()
    job.commit()