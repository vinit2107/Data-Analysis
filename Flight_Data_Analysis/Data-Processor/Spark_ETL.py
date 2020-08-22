import datetime as dt
from configparser import RawConfigParser
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, posexplode, year, concat_ws, expr, col, month, dayofweek, \
    date_format, to_timestamp, count, lit, udf


class DataTransformer:

    redshift_url = "jdbc:postgres://{}/{}?user={}&password={}"

    def transform_airline_data(self, airline_df: DataFrame, config: RawConfigParser):
        """
        Function to transform the airline data and insert the data in the respective table in AWS RedShift.
        The output will be a table with three columns
        1. Primary Key, which will be a combination of AirlineID and Carrier
        2. Carrier Short Key
        3. Carrier Name
        :param config: config object
        :param airline_df: Dataframe to be transformed
        """
        try:
            print("Initiating processing Airline Data")
            print("Replacing missing values in the THRU_DATE_SOURCE column with 12-31-2020: ", end="")
            airline_df = airline_df.withColumn("THRU_DATE_SOURCE", when(col("THRU_DATE_SOURCE").isNull(),
                                                                        dt.date(2020, 12, 31)).
                                               otherwise(col("THRU_DATE_SOURCE")))
            print("OK")
            airline_df = airline_df.withColumn("YEARS", year(col("THRU_DATE_SOURCE")) - year(col("START_DATE_SOURCE")))
            airline_df = airline_df.withColumn("repeat", expr("split(repeat(',', YEARS), ',')")).\
                select("*", posexplode("repeat").alias("YEAR", "val")).\
                drop("repeat", "val", "YEARS").withColumn("YEAR", col("YEAR") + year(col("START_DATE_SOURCE")))
            print("Generating Primary Key: ", end="")
            airline_df = airline_df.withColumn("AIRLINE_PRIMARY_KEY", concat_ws(":", col("AIRLINE_ID"),
                                                                                col("CARRIER"), col("YEAR")))
            print("Writing data in the table: ", end="")
            airline_df.write.jdbc(self.redshift_url.format(config.get("Redshift", "reshift.hostname"),
                                                           config.get("Redshift", "redshift.database"),
                                                           config.get("Redshift", "redshift.username"),
                                                           config.get("Redshift", "redshift.password")),
                                  table="DIM_AIRLINE")
            print("OK")
        except Exception as ex:
            print("Error transforming airline data")
            raise ex

    def transform_airport_data(self, airport_data: DataFrame, config: RawConfigParser):
        """
        Function to transform the airport data and store data in AWS Reshift. The obtained data is already in the
        required format. Hence, we'll just write the dataframe onto the AWS Redshift table
        :param config: config object
        :param airport_data: DataFrame obtained for airport data
        """
        try:
            print("Initiating transformation for Airport data: ", end="")
            print("OK")
            print("Writing data onto the Redshift: ", end="")
            airport_data.write.jdbc(url=self.redshift_url.format(config.get("Redshift", "reshift.hostname"),
                                                                 config.get("Redshift", "redshift.database"),
                                                                 config.get("Redshift", "redshift.username"),
                                                                 config.get("Redshift", "redshift.password")),
                                    table="DIM_AIRPORT")
            print("OK")
        except Exception as ex:
            print("Error writing data onto the Redshift")
            raise ex

    def transform_flight_data(self, flight_df: DataFrame, config: RawConfigParser):
        """
        Function to transform flight data and insert the data into AWS Redshift. This table will act as the
        fact table for the database which will include the KPIs and foreign keys to the dimension table.
        :param flight_df: Dataframe to be transformed
        :param config: configuration file object
        """
        try:
            print("Transforming flight data")
            print("Extracting month, year and weekday columns FL_DATE")
            flight_df = flight_df.withColumn('YEAR', year(col("FL_DATE")))
            flight_df = flight_df.withColumn('MONTH', month(col("FL_DATE")))
            flight_df = flight_df.withColumn('WEEKDAY', dayofweek(col("FL_DATE")))
            print("Transforming time format from HHmm to HH:mm:ss in CRS_DEP_TIME")
            flight_df = flight_df.withColumn("DEP_TIME", date_format(to_timestamp("CRS_DEP_TIME", "HHmm"), "HH:mm:ss"))
            flight_df = flight_df.withColumn("ARR_TIME", date_format(to_timestamp("CRS_ARR_TIME", "HHmm"), "HH:mm:ss"))
            flight_df = flight_df.withColumn("FLIGHT_SCHEDULE_PRIMARY_KEY", concat_ws(":", col("OP_CARRIER_AIRLINE_ID"),
                                                                                      col("ORIGIN_AIRPORT_ID"),
                                                                                      col("DEST_AIRPORT_SEQ_ID"),
                                                                                      col("TAIL_NUM"), col("DEP_TIME")))
            flight_schedule_df = flight_df.select(['FLIGHT_SCHEDULE_PRIMARY_KEY', 'OP_CARRIER_AIRLINE_ID',
                                                   'ORIGIN_AIRPORT_ID', 'ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR'
                                                   , 'DEST_AIRPORT_ID', 'DEST', 'DEST_CITY_NAME', 'DEST_STATE_ABR',
                                                   'DEP_TIME', 'ARR_TIME', 'WEEKDAY', 'MONTH'])
            flight_schedule_df = flight_schedule_df.drop_duplicates()
            print("Writing the Flight Schedule Data")
            flight_schedule_df.write.jdbc(url=self.redshift_url.format(config.get("Redshift", "reshift.hostname"),
                                                                       config.get("Redshift", "redshift.database"),
                                                                       config.get("Redshift", "redshift.username"),
                                                                       config.get("Redshift", "redshift.password")),
                                          table="DIM_FLIGHT_SCHEDULE").mode("ignore").save()
            flight_df = flight_df.withColumn("AIRLINE_PRIMARY_KEY", concat_ws(":", col("AIRLINE_ID"), col("CARRIER"),
                                                                              col("YEAR")))
            grouped_df = flight_df.groupby(['FLIGHT_SCHEDULE_PRIMARY_KEY', 'AIRLINE_PRIMARY_KEY', 'ORIGIN_AIRPORT_ID',
                                            'DEST_AIRPORT_ID', 'MONTH'])
            grouped_df = grouped_df.agg(sum(col('DEP_DELAY')).alias('TOTAL_DEP_DELAY'),
                                        sum(col('ARR_DELAY')).alias('TOTAL_ARRIVAL_DELAY'),
                                        count(lit(1)).alias('TOTAL_FLIGHTS'))
            grouped_df = grouped_df.withColumn('TOTAL_DELAY', col('TOTAL_DEP_DELAY') + col('TOTAL_ARRIVAL_DELAY'))
            filtered_delay = grouped_df.filter(col('TOTAL_DELAY') > 0)

            # Calculating minimum and maximum delay
            min_delay = filtered_delay.groupby().min('TOTAL_DELAY').first().asDict()['min(TOTAL_DELAY)']
            max_delay = filtered_delay.groupby().max('TOTAL_DELAY').first().asDict()['max(TOTAL_DELAY)']

            # Registering the udf
            udf_ratings = udf(lambda x: self.calculate_ratings(x, min_delay, max_delay))

            print("Calculating ratings")
            final_flight_df = grouped_df.withColumn('RATINGS', udf_ratings(col('TOTAL_DELAY')))

            print("Writing data onto the AWS Redshift")
            final_flight_df.write.jdbc(url=self.redshift_url.format(config.get("Redshift", "reshift.hostname"),
                                                                    config.get("Redshift", "redshift.database"),
                                                                    config.get("Redshift", "redshift.username"),
                                                                    config.get("Redshift", "redshift.password")),
                                       table="DIM_AIRPORT").mode('append')
        except Exception as ex:
            print("Error transforming flight data")
            raise ex

    def calculate_ratings(self, data, min_delay, max_delay):
        """
        Function to calculate ratings for the given data. We will first scale down the data to a range 0 to 1. The
        formula for scaling is (max - value)/(max - min). This makes sure that the values which have lower delays
        have a higher rating. This rating will be relative for each batch.
        :param data: row of data
        :param min_delay: Minimum delay from all the flights in a batch
        :param max_delay: Maximum delay from all the flights in a batch
        :return:
        """
        if data is not None:
            if data <= 0:
                return 5
            else:
                return round(5 * (max_delay - data) / (max_delay - min_delay), 2)
        else:
            return None
