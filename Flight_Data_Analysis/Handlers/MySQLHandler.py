import mysql.connector
from mysql.connector import errorcode, cursor, Error, connection
from configparser import RawConfigParser
from Scripts.DDL.ddl_scripts import *
from Scripts.DML.dml_scripts import *


class MySQLHandler:
    def create_connection(self, config: RawConfigParser):
        """
        Function to establish a connection with a MySQL server using the credentials provided in the configuration file.
        :param config: configuration file object obtained using configparser
        """
        try:
            conn = mysql.connector.connect(user=config.get('Database', 'mysql.username'),
                                           password=config.get('Database', 'mysql.password'),
                                           host=config.get('Database', 'mysql.hostname'))
            conn.autocommit = True
            cur = conn.cursor(buffered=True)
            return conn, cur
        except mysql.connector.Error as er:
            print("Error connecting to the database")
            if er.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Error in the configuration file. Please check the credentials!")
            print(er)
            raise er

    def create_database(self, cur: cursor, dbname: str):
        """
        Function to create a database of the name defined in the configuration.properties
        :param cur: cursor obtained through connection to the database
        :param dbname: name of the database to be created
        """
        try:
            print(create_database_query.format(dbname))
            # cur.execute("CREATE DATABASE IF NOT EXISTS `flight-analysis`;")
            cur.execute(create_database_query, (dbname, ))
            print("executed")
            cur.execute(use_database_query, (dbname, ))
            # cur.execute("USE `flight-analysis`;")
        except Error as er:
            print("Error creating database in MySQL")
            raise er

    def create_table(self, cur: cursor, tableName: str):
        """
        Function used to create a table of the given table name. The queries will be picked up from the Scripts folder.
        :param cur: cursor obtained from the
        :param tableName: name of the table
        """
        try:
            print("Creating table {}:".format(tableName), end='')
            cur.execute(tables.get(tableName))
            print('OK')
        except Error as er:
            if er.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists")
            else:
                raise er

    def insert(self, cur: cursor, tableName: str, data):
        """
        Function to insert records in table of the given name
        :param data: data to be inserted in the table
        :param cur: cursor obtained for MySQL
        :param tableName: name of the table in which the data has to be inserted
        """
        try:
            print("Inserting single record in {}: ".format(tableName), end='')
            cur.execute(insert_map.get(tableName), data)
            # cur.commit()
            print("OK")
        except Error as er:
            print("Error inserting records in {}".format(tableName))
            raise er

    def close_connection(self, cur: cursor, con: connection):
        """
        Function to close the connection with MySQL server
        :param cur: cursor obtained after connecting with the server
        :param con: connection object obtained after connecting with the server
        """
        try:
            print("Closing connection with MySQL.")
            cur.execute("Select * from CONTROL_INFO;")
            print(cur.fetchone())
            cur.close()
            con.close()
        except Error as er:
            print("Error disconnecting with the server")
            raise er
