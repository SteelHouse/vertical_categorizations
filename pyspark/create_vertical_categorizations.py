# Job needs 16g on executor memory to complete in Spark
# spark-submit create_verticals.py --s3_output_path s3://mntn-data-archive-dev/vertical_categorizations/ip_vertical_associations/ --run_date 2024-01-08
import argparse
from datetime import datetime
from datetime import timedelta

import boto3
from botocore.exceptions import ClientError
from typing import Dict
import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--s3_output_path", dest="s3_output_path")
parser.add_argument("--run_date", dest="run_date")

args = parser.parse_args()
run_date = datetime.strptime(args.run_date, "%Y-%m-%d")
start_date = (run_date - timedelta(8)).date()

S3_BASE_PATH = "s3://mntn-data-archive-prod/guid_log/"

def get_secret(secret_name: str) -> Dict:
    region_name = "us-west-2"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secrets = get_secret("redshift-prod")
secrets['jdbcUrl'] = f"""jdbc:{secrets.get("engine")}://{secrets.get("host")}:{secrets.get("port")}/coredw"""

redshiftOptions = {
    "url": secrets.get("jdbcUrl"),
    "tempdir": "s3://aws-glue-assets-077854988703-us-west-2/temporary/",
    "unload_s3_format": "PARQUET",
    "aws_iam_role": "arn:aws:iam::077854988703:role/service-role/prod-redshift_commands_access_role",
    "user": secrets.get("username"),
    "password": secrets.get("password"),
}

def loadRedshiftQuery(query: str):
    redshiftOptions['query'] = query
    return spark.read.format("io.github.spark_redshift_community.spark.redshift") \
        .options(**redshiftOptions) \
        .load()


spark = (SparkSession
         .builder
         .appName("Create IP Verticals Table")
         .getOrCreate())


spark.sparkContext.setLogLevel("ERROR")

guid_log_lookback_days = 30

date_list = [datetime.strptime(args.run_date, "%Y-%m-%d").date() - timedelta(days=x) for x in range(guid_log_lookback_days)]
guid_log_paths = list(f"{S3_BASE_PATH}dt={date}/" for date in date_list)
guid_logs_df = spark.read.option("basePath", f"{S3_BASE_PATH}").format("parquet").load(guid_log_paths)

grouped_df = guid_logs_df.filter("ip != '0.0.0.0'").groupBy("advertiser_id", "ip").agg(F.count("*").alias("count")).orderBy("ip", "advertiser_id").drop("count")

verticals_df = loadRedshiftQuery("SELECT * FROM fpa.advertiser_verticals")

joined_df = grouped_df.join(verticals_df, "advertiser_id", "left_outer")

ipdsc_df = (
    joined_df.select("ip",
                     F.lit(f"{args.run_date}").cast("date").alias("date"),
                     F.lit(13).alias("data_source_id"),
                     F.col("vertical_id").alias("data_source_category_id"))
)

# Save somewhere
ipdsc_df.coalesce(200).write.format("parquet").save(f"{args.s3_output_path}/dt={args.run_date}")


