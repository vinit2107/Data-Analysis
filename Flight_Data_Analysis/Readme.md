# Flight Data Analysis

![](https://images.unsplash.com/photo-1436491865332-7a61a109cc05?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=600&q=100)

Image source: [Unsplash](https://images.unsplash.com/photo-1436491865332-7a61a109cc05?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=600&q=100)

## Objective

The aim of the project is to create a pipeline for completing data analysis of the flight data. The project consists of three stages. Initial stage involves scraping data from
[Bureau of Transportation Statistics](https://www.transtats.bts.gov/ErrPage.asp). **Selenium** will be the primary tool for scraping the data. Three types of files will be downloaded namely for on-time arrival of flights, 
details of the airports and details of airlines. Links to the dataset are given below. Scraped files will be stored on **AWS S3** which will act like a staging area for the dataset. 
Next step would be to perform transformation to the data. This will be done using **PySpark**. The output of the transformation level will be stored in **AWS RedShift**. This will act like
a warehouse which will facilitate analysis. The final stage will be to create a dashboard in **Tableau**. This project simulates batch processing. To maintain the files that have 
been ingested and files that have been processed, a table 'CONTROL_INFO' will be maintained in **MySQL**.

## Links to Datasets

1. [On-Time Arrival](https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time)
Select the month and year for which the data needs to be downloaded
2. [Airport Details](https://www.transtats.bts.gov/Tables.asp?DB_ID=595&DB_Name=Aviation%20Support%20Tables) 
Click Master Coordinate
3. [Airline Details](https://www.transtats.bts.gov/Tables.asp?DB_ID=595&DB_Name=Aviation%20Support%20Tables) 
Click Carrier Decode

## Stage - 1: Data Ingestion

This stage involves scraping data from the website and storing it onto S3. Simultaneously, a entry will be made into the CONTROL_INFO table after uploading data onto S3. Three
buckets will have to be maintained in S3. Multiple configurations need to be configured in the properties file. The properties file can be found under ***resources/config***.

1. **AWS Credentials** 

Two properties, ***aws_access_key_id*** and ***aws_secret_access_key*** will need to be configured. This will be used to create connection with the S3 server using boto3. 

2. **Bucket Name**

Four properties ***bucket_name_flight***, ***bucket_name_airport***, ***bucket_name_airline*** and ***region*** will need to be configured. THe bucket names should have the same 
characteristics required for describing a bucket name, with no numeric values and no spaces. Please follow the regions defined for AWS. Mention one of them. If the region required 
is 'us-east-1', leave the property empty.

3. **Downloading datasets**

Datasets that need to be downloaded are governed by three flags, ***enable_flight_details***, ***enable_airline_details*** and ***enable_airport_details*** for doenloading 
on-time flight data, airline data and airport data resp. In order to enable the flag, place *true* in front of the resp properties. ***flight_schedule_years*** and ***flight_schedule_months***
will be used to specify the years and months for which the data needs to be downloaded. The values need to be comma separated if multiple values need to be provided.

4. **MySQL Connection Details**

***mysql.hostname***, ***mysql.username***, ***mysql.password*** and ***mysql.database*** need to be provided for establishing a connection with MySQL server. It should be in
same case as required for establishing connection.
