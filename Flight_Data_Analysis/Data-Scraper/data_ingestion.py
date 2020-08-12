from CommonUtils.PropertyUtils import PropertyReader
from Handlers.S3Handler import S3Handler
from Handlers.SeleniumHandler import SeleniumHandler
from Handlers.MySQLHandler import MySQLHandler


def main():
    print("Starting scraping the flight data!")
    print("Reading the configuration file")
    config = PropertyReader().readPropertyFile()

    print("Trying to establish connection with s3")
    s3 = S3Handler()
    s3_client = s3.create_connection(config.get('credentials', 'aws_access_key_id'),
                                     config.get('credentials', 'aws_secret_access_key'))
    print("Creating a bucket in S3")
    s3.create_bucket(config.get('Bucket', 'bucket_name_flight'), s3_client, config.get('Bucket', 'region'))
    s3.create_bucket(config.get('Bucket', 'bucket_name_airport'), s3_client, config.get('Bucket', 'region'))
    s3.create_bucket(config.get('Bucket', 'bucket_name_airline'), s3_client, config.get('Bucket', 'region'))

    ingestor = SeleniumHandler()
    sql_handler = MySQLHandler()

    print("Trying to establish a connection with MySQL server")
    sql_connection, sql_cursor = sql_handler.create_connection(config)
    sql_handler.create_database(sql_cursor, config.get('Database', 'mysql.database'))
    sql_handler.create_table(sql_cursor, "CONTROL_INFO")

    print("Starting scraping of data.")
    files = ingestor.download_files(config)

    print("Files downloaded successfully")
    for key in files.keys():
        for file in files[key]:
            if key == "Downloads-Flight":
                s3.upload_file(file, config.get('Bucket', 'bucket_name_flight'), s3_client)
            if key == "Downloads-Airport":
                s3.upload_file(file, config.get('Bucket', 'bucket_name_airport'), s3_client)
            if key == "Downloads-Airline":
                s3.upload_file(file, config.get('Bucket', 'bucket_name_airline'), s3_client)
            sql_handler.insert(sql_cursor, "CONTROL_INFO", (file.split("\\")[-1], False))
    sql_handler.close_connection(sql_cursor, sql_connection)


if __name__ == "__main__":
    main()
