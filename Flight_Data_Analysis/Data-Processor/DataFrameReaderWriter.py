from pyspark.sql import SparkSession, DataFrame


class DataFrameReaderWriter:
    urls = {"mysql": "jdbc:mysql://{}/{}?user={}&password={}",
            "postgres": "jdbc:postgres://{}/{}?user={}&password={}"}

    def read(self, spark: SparkSession, source: str, hostname: str, dbname: str, user: str, password: str,
             tableName: str) -> DataFrame:
        """
        Function to read data using spark
        :param spark: SparkSession object
        :param source: Type of source from which the data has to be read
        :param hostname: Hostname for the server
        :param dbname: Name of the database to be connected to
        :param user: Username which has the previledge to access the database
        :param password: password for the username
        :param tableName: Name of the table to be read
        :return: Spark Dataframe
        """
        try:
            print("Reading data from {} database".format(source))
            url = self.urls[source]
            url = url.format(hostname, dbname, user, password)
            if source.lower() == "mysql" or source.lower() == "postgres":
                df = spark.read.jdbc(url=url, table=tableName)
            else:
                df = spark.read.format(source).option("url", url).option("dbname", dbname).option("user", user) \
                    .option("password", password).load()
            return df
        except Exception as ex:
            print(" Error reading the data from {}".format(source))
            raise ex

    def read(self, spark: SparkSession, path: str):
        """
        Function to read the files stored on the local system
        :param spark: SparkSession of the current job
        :param path: Path to the file to be read
        """
        try:
            return spark.read.csv(path=path, header=True)
        except Exception as ex:
            print("Error reading csv file")
            raise ex

    def write(self, df: DataFrame, target: str, hostname: str, dbname: str, user: str, password: str,
              tableName: str):
        """
        Function to write the data onto the server where the data is hosted
        :param df: DataFrame which needs to be written
        :param target: Format where the data needs to be stored
        :param hostname: Hostname of the server
        :param dbname: Database name which contains the table
        :param user: Username which has the previledges to access the data
        :param password: Password of the user
        :param tableName: Name of the table to which data has to be written
        """
        try:
            print("Writing data onto the table {}: ".format(tableName), end="")
            df.write.format(target).option('url', hostname).option("dbname", dbname).option("user", user) \
                    .option("password", password)
            print("OK")
        except Exception as ex:
            print("Error writing data in the table {}".format(tableName))
            raise ex
