import os
import zipfile
from configparser import RawConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from Handlers.S3Handler import S3Handler
from CommonUtils.PropertyUtils import PropertyReader
from .DataFrameReaderWriter import DataFrameReaderWriter
from .Spark_ETL import DataTransformer


def main():
    print("Initiating Data Transformation Job!")
    print("Reading configuration file")
    config = PropertyReader().readPropertyFile()

    print("Establishing connection with S3")
    s3 = S3Handler()
    s3_client = s3.create_connection(config.get('credentials', 'aws_access_key_id'),
                                     config.get('credentials', 'aws_secret_access_key'))

    print("Creating Spark Session for executing the job")
    spark = SparkSession.builder.master("local").appName("Flight-Data-Analysis").getOrCreate()

    print("Fetching records from CONTROL_INFO using SparkSQL")
    records_df = DataFrameReaderWriter().read(spark, "mysql", config.get("Database", "mysql.hostname"),
                                              config.get("Database", "mysql.database"),
                                              config.get("Database", "mysql.username"),
                                              config.get("Database", "mysql.password"), "CONTROL_INFO")
    latest_datetime = records_df.filter("IS_PROCESSED = 1").sort(col('CREATED_DATETIME').desc()).first().asDict()\
        .get('CREATED_DATETIME')
    records_df = records_df.filter('IS_PROCESSED = 0 and CREATED_DATETIME > {}'.format(latest_datetime))
    records_df.rdd.map(lambda x: s3.download_file(x[0], x[1], s3_client))

    directories = [os.path.join("/tmp/", file) for file in os.listdir("/tmp/")]
    # Unzipping the files and deleting the zip files present in the directory
    unzip_files(directories)

    # Initiating spark processing for the datasets
    for bucket in os.listdir('\tmp'):
        if bucket == config.get('Bucket', 'bucket_name_airline'):
            for file in os.listdir(os.path.join('\tmp', bucket)):
                airline_df = DataFrameReaderWriter().read(spark, path=file)
                DataTransformer().transform_airline_data(airline_df, config)
                insert_into_control_info(spark, file.split("\\")[-1], bucket)
        if bucket == config.get('Bucket', 'bucket_name_airport'):
            for file in os.listdir(os.path.join('\tmp', bucket)):
                airport_df = DataFrameReaderWriter().read(spark, path=file)
                DataTransformer().transform_airport_data(airport_df, config)
                insert_into_control_info(spark, file.split("\\")[-1], bucket)
        if bucket == config.get('Bucket', 'bucket_name_flight'):
            for file in os.listdir(os.path.join('\tmp', bucket)):
                flight_df = DataFrameReaderWriter().read(spark, path=file)
                DataTransformer().transform_flight_data(flight_df, config)
                insert_into_control_info(spark, file.split("\\")[-1], bucket)
    print("Job Completed!!")


def unzip_files(directories: list):
    for path in directories:
        files = [os.path.join(path, f) for f in os.listdir(path)]
        for file in files:
            with zipfile.ZipFile(file, "r") as zip_ref:
                zip_ref.extractall(path)
                os.remove(file)


def insert_into_control_info(spark: SparkSession, fileName: str, bucket: str, config: RawConfigParser):
    data = [fileName, bucket, True]
    df = spark.sparkContext.parallelize(data).toDF(['FILE_NAME', 'BUCKET_NAME', 'IS_PROCESSED'])
    DataFrameReaderWriter().write(df, "jdbc", config.get("Database", "mysql.hostname"),
                                  config.get("Database", "mysql.database"), config.get("Database", "mysql.username"),
                                  config.get("Database", "mysql.password"), "CONTROL_INFO")


if __name__ == "__main__":
    main()
