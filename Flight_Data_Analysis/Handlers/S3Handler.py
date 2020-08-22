import os
import boto3
from botocore.exceptions import ConnectionError, ClientError


class S3Handler:
    def create_connection(self, access_key: str, secret_key: str):
        """
        This function is used to create a connection to the s3. The function uses credentials provided in the
        configuration.properties
        :type access_key: string
        :param access_key: aws_access_key_id configured in the property file

        :type secret_key: string
        :param secret_key: aws_secret_access_key configured in the property file
        :return: resource object
        """
        try:
            print("Trying to establish a connection with the AWS s3")
            s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            print("Connection successful!!")
            return s3_client
        except ConnectionError as ex:
            print("Error connecting to the s3")
            raise ex

    def check_if_bucket_exists(self, bucket_name: str, s3_client) -> bool:
        """
        Function used to check if the bucket of a given name exists in the already existing buckets
        :param s3_client: s3 object obtained from create_connection method
        :type bucket_name: string
        :param bucket_name: name of the bucket to be validated against
        :return: boolean value
        """
        try:
            print("Checking if the bucket already exists")
            if bucket_name in [bucket['Name'] for bucket in s3_client.list_buckets()['Buckets']]:
                print("Bucket of " + bucket_name + " already exists!")
                return True
            else:
                return False
        except ClientError as ex:
            print("Error obtaining list of buckets")
            raise ex

    def create_bucket(self, bucket_name: str, s3_client, region: str):
        """
        Function to create a bucket of the provided name in s3. The function internally calls the function to check
        if the bucket already exists. If not, it will go on and create one with the name provided
        :param region: name of the region in which the bucket has to be created
        :param bucket_name: name of the bucket to be created
        :param s3_client: client object obtained from boto3.client
        """
        try:
            if self.check_if_bucket_exists(bucket_name, s3_client):
                pass
            else:
                if region == "":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    location = {'LocationConstraint': region}
                    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        except ClientError as ex:
            print("Error creating a new bucket")
            raise ex

    def upload_file(self, fileName: str, bucket: str, client, objectName: str = None):
        """
        Function to upload file in s3
        :param client: s3 client object
        :param fileName: file to upload
        :param bucket: name of the bucket to which the file is to be uploaded
        :param objectName: s3 object name. If None fileName is used instead
        """
        if objectName is None:
            objectName = fileName
        try:
            print("Uploading file to bucket {}".format(bucket))
            client.upload_file(fileName.split("\\")[-1], bucket, objectName)
        except ClientError as er:
            print("Error uploading file to s3")
            raise er

    def download_file(self, fileName: str, bucketName: str,  client):
        """
        Function to download files from S3 onto the local system
        :param client: s3 client object
        :param fileName: Name of the file to be downloaded
        :param bucketName: name of the bucket from which the file has to be downloaded
        """
        try:
            print("Downloading file {}".format(fileName))
            print("Downloaded? ", end="")
            dest_path = os.path.join("\\tmp", bucketName)
            client.Bucket(bucketName).download_file(fileName, dest_path)
            print("Success")
        except Exception as ex:
            print("Failed")
            raise ex
