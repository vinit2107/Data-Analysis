import os
import time
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import Select
from configparser import RawConfigParser
from Scripts.DDL.ddl_scripts import *
from selenium.webdriver.common.keys import Keys


class SeleniumHandler:
    def download_files(self, config: RawConfigParser) -> dict:
        """
        Function to get the browser instance through which the dataset will be downloaded
        :param config: object for configuration file
        :return Dictionary with keys as folder name and values as the list of complete paths of the downloaded files
        """
        try:
            files = {}
            if config.get('Links', 'enable_flight_details').lower() == 'true':
                files['Downloads-Flight'] = self.download_flight_schedule_data(config)
            if config.get('Links', 'enable_airport_details').lower() == 'true':
                files['Downloads-Airport'] = self.download_airport_data(config)
            if config.get('Links', 'enable_airline_details').lower() == 'true':
                files['Downloads-Airline'] = self.download_airline_data(config)
            return files
        except Exception as ex:
            print("Error opening Chrome")
            raise ex

    def select_required_columns(self, browser, columns: list):
        """
        Function to select the columns on the webpage for the data to be downloaded
        :param browser: webriver object obtained for Chrome
        :param columns: list of columns that need to selected
        :return:
        """
        try:
            for column in columns:
                browser.find_element_by_xpath("//input[@type='checkbox' and @title='{}']".format(column)).click()
        except Exception as ex:
            print("Error selecting columns in the table")
            raise ex

    def select_year(self, browser: webdriver, year: str):
        """
        Function to select the year from the dropdown menu on the website
        :param year: year which has to be selected from the dropdown
        :param browser: webdriver object obtained for Chrome
        """
        try:
            select = Select(browser.find_element_by_id('XYEAR'))
            select.select_by_visible_text(year)
        except Exception as ex:
            print("Error selecting year from the dropdown menu")
            raise ex

    def select_month(self, browser: webdriver, month: str):
        """
        Function to select month from the dropdown menu on the website
        :param browser: webriver object otained for Chrome
        :param month: month to be selected from the dropdown menu
        """
        try:
            select = Select(browser.find_element_by_id('FREQUENCY'))
            select.select_by_visible_text(month)
        except Exception as ex:
            print("Error selecting year from the dropdown menu")
            raise ex

    def download_flight_schedule_data(self, config: RawConfigParser):
        """
        Function to download the on-time flight data. The data will be downloaded in the Downloads-Flight folder inside
        Data-Scraper
        :param config: object obtained for oonfiguration.properties using RawConfigParser
        :return:
        """
        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
        try:
            chromeOptions = webdriver.ChromeOptions()
            prefs = {"download.default_directory": os.path.join(os.getcwd(), "Downloads-Flight\\"),
                     "directory_upgrade": True}
            chromeOptions.add_experimental_option("prefs", prefs)
            browser = webdriver.Chrome(executable_path='../resources/lib/chromedriver.exe',
                                       chrome_options=chromeOptions)
            browser.get(config.get('Links', 'flight_schedule'))
            self.select_required_columns(browser, flight_schedule_columns)
            time.sleep(10)
            for year in config.get('Links', 'flight_schedule_years').split(','):
                self.select_year(browser, year)
                for month in list(set(months) & set(config.get('Links', 'flight_schedule_months').split(','))):
                    self.select_month(browser, month)
                    element = browser.find_element_by_xpath("//*[@id='content']/table[1]/tbody/tr/td[2]/table[3]/tbody/"
                                                            "tr/td[2]/button[1]")
                    ActionChains(browser).click(element).perform()
                    time.sleep(40)
                    self.renamefiles(year + "_" + month, "Downloads-Flight")
            browser.close()
            return self.list_files("Downloads-Flight")
        except Exception as ex:
            print("Error downloading files for flight schedule")
            raise ex

    def download_airport_data(self, config: RawConfigParser):
        """
        Function to download the airport data. The data will be downloaded in the Downloads-Airport folder inside
        Data-Scraper
        :param config: object obtained for oonfiguration.properties using RawConfigParser
        :return:
        """
        try:
            chromeOptions = webdriver.ChromeOptions()
            prefs = {"download.default_directory": os.path.join(os.getcwd(), "Downloads-Airport\\"),
                     "directory_upgrade": True}
            chromeOptions.add_experimental_option("prefs", prefs)
            browser = webdriver.Chrome(executable_path='../resources/lib/chromedriver.exe',
                                       chrome_options=chromeOptions)
            browser.get(config.get('Links', 'airport_details'))
            browser.find_element_by_xpath('//*[@id="form1"]/table[3]/tbody/tr[9]/td[2]/a[2]').click()
            self.select_required_columns(browser, airport_details_columns)
            element = browser.find_element_by_xpath("//*[@id='content']/table[1]/tbody/tr/td[2]/table[3]/tbody/"
                                                    "tr/td[2]/button")
            ActionChains(browser).click(element).perform()
            time.sleep(20)
            browser.close()
            return self.list_files("Downloads-Airport")
        except Exception as ex:
            print("Error downloading data for aiport details")
            raise ex

    def download_airline_data(self, config: RawConfigParser):
        """
        Function to download the airport data. The data will be downloaded in the Downloads-Airline folder inside
        Data-Scraper
        :param config: object obtained for oonfiguration.properties using RawConfigParser
        :return:
        """
        try:
            chromeOptions = webdriver.ChromeOptions()
            prefs = {"download.default_directory": os.path.join(os.getcwd(), "Downloads-Airline"),
                     "directory_upgrade": True}
            chromeOptions.add_experimental_option("prefs", prefs)
            browser = webdriver.Chrome(executable_path='../resources/lib/chromedriver.exe',
                                       chrome_options=chromeOptions)
            browser.get(config.get('Links', 'airline_details'))
            browser.find_element_by_xpath('//*[@id="form1"]/table[3]/tbody/tr[6]/td[2]/a[2]').click()
            self.select_required_columns(browser, airline_detail_columns)
            browser.find_element_by_tag_name('body').send_keys(Keys.CONTROL + Keys.HOME)
            time.sleep(5)
            element = browser.find_element_by_xpath("//*[@id='content']/table[1]/tbody/tr/td[2]/table[3]/tbody/"
                                                    "tr/td[2]/button")
            ActionChains(browser).click(element).perform()
            time.sleep(10)
            browser.close()
            return self.list_files("Downloads-Airline")
        except Exception as ex:
            print("Error downloading data for aiport details")
            raise ex

    def renamefiles(self, fileName: str, directory: str):
        """
        Function to rename the files downloaded using selenium
        :param directory: directory where the files have to be renamed
        :param fileName: new name of the file
        """
        files = self.list_files(directory)
        latest_file = max(files, key=os.path.getctime)
        name = latest_file.split("\\")
        name[-1] = fileName + ".zip"
        os.rename(latest_file, "\\".join(name))

    def list_files(self, directory: str):
        """
        Function to list files present in the given directory
        :param directory: name of the directory
        :return: list of files
        """
        try:
            path = os.path.join(os.getcwd(), directory)
            return [os.path.join(path, file) for file in os.listdir(path)]
        except Exception as ex:
            print("Error listing the files")
            raise ex
