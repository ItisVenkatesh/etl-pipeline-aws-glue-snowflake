# etl-pipeline-aws-glue-snowflake
ETL pipeline using AWS Glue, Snowflake, and Python
# ETL Pipeline with AWS Glue & Snowflake

## 🚀 Project Overview
This project demonstrates an **ETL pipeline** using **AWS Glue**, **Snowflake**, and **Python**.  
It ingests, transforms, and loads data from **S3** to **Snowflake**.

## 🛠️ Tech Stack
- **AWS Services**: AWS S3, AWS Secrets manager
- **ETL Tools**: AWS Glue, PySpark
- **Database**: Snowflake
- **Language**: Python

## 📌 Features
✅ Data ingestion from AWS S3  
✅ Transformation using AWS Glue, PySpark  
✅ Loading data into Snowflake  

## 🚀 Running the Pipeline
1. Clone the repository:
    ``` bash
    git clone https://github.com/ItisVenkatesh/etl-pipeline-aws-glue-snowflake.git
    cd etl-pipeline-aws-glue-snowflake

2. Create a virtual environment
    ``` bash
    python3 -m venv venv

    # Activate the virtual environment
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`

3. Install the required dependencies
    ``` bash
    pip install -r requirements.txt

4. Configure AWS Secrets Manager
    Store your Snowflake credentials securely in AWS Secrets Manager. Create a secret with the following JSON structure:
    {
        "sf_account": "your_snowflake_account",
        "sf_user": "your_snowflake_user",
        "sf_password": "your_snowflake_password",
        "sf_database": "your_snowflake_database",
        "sf_schema": "your_snowflake_schema",
        "sf_warehouse": "your_snowflake_warehouse",
        "sf_role": "your_snowflake_role"  // Optional
    }

5. Set Secret Name in the Script
    Update the secret_name variable to match the name of the secret you stored in AWS Secrets Manager in the script.

    secret_name = "your-snowflake-credentials-secret"

6. Configure S3 Bucket
    Ensure your S3 bucket is configured properly. The ETL script will fetch a file from the specified bucket. Make sure the AWS Glue (or whichever service you are using to run the script) has the necessary permissions to read the file from S3.

7. IAM Permissions
    Make sure the IAM role or user executing the script has the following permissions:
	•	Secrets Manager:
	    secretsmanager:GetSecretValue
	•	S3:
	    s3:GetObject
	•	Snowflake:
	    Permissions to write data to Snowflake.

8. Run the ETL Script
    ``` bash
    python etl_script.py

## License

This project is licensed under the MIT License - see the LICENSE file for details.