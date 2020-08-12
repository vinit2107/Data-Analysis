flight_schedule_columns = ['FlightDate', 'Reporting_Airline', 'DOT_ID_Reporting_Airline', 'Tail_Number',
                           'Flight_Number_Reporting_Airline', 'Origin', 'OriginCityName', 'OriginState',
                           'OriginStateName', 'Dest', 'DestCityName', 'DestState', 'DestStateName', 'CRSDepTime',
                           'DepTime', 'DepDelay', 'DepDelayMinutes', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'Cancelled',
                           'CancellationCode', 'Diverted', 'CRSElapsedTime', 'ActualElapsedTime', 'AirTime', 'Flights',
                           'Distance', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']


airport_details_columns = ['AirportWac', 'AirportCountryCodeISO', 'AirportStateFips', 'CityMarketID', 'CityMarketName',
                           'CityMarketWac', 'LatDegrees', 'LatHemisphere', 'LatMinutes', 'LatSeconds', 'LonDegrees',
                           'LonHemisphere', 'LonMinutes', 'LonSeconds']

airline_detail_columns = ['AirlineID', 'Carrier', 'CarrierName', 'UniqueCarrierName', 'Region', 'StartDate', 'EndDate']

create_database_query = 'CREATE DATABASE IF NOT EXISTS `%s`;'

use_database_query = 'USE `%s`;'

control_info_create_table = """
                            CREATE TABLE IF NOT EXISTS `CONTROL_INFO` (
                            `ID` int auto_increment PRIMARY KEY,
                            `FILE_NAME` varchar(100) NOT NULL,
                            `IS_PROCESSED` bool NOT NULL,
                            `CREATED_DATETIME` timestamp DEFAULT CURRENT_TIMESTAMP);
                            """

tables = {'CONTROL_INFO': control_info_create_table}