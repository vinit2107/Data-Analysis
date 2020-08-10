from CommonUtils.PropertyUtils import PropertyReader
from Handlers.S3Handler import S3Handler
from Handlers.SeleniumHandler import SeleniumHandler


def main():
    print("Starting scraping the flight data!")
    print("Reading the configuration file")
    config = PropertyReader().readPropertyFile()

    print("Trying to establish connection with s3")
    s3 = S3Handler()
    # s3_client = s3.create_connection(config.get('credentials', 'aws_access_key_id'),
    #                                  config.get('credentials', 'aws_secret_access_key'))
    # s3.create_bucket(config.get('Bucket', 'bucket_name'), s3_client, config.get('Bucket', 'region'))
    scraper = SeleniumHandler()
    scraper.get_browser_instance(config)


if __name__ == "__main__":
    main()
