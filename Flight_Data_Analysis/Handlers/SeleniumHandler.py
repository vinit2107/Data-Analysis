from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import Select
from configparser import RawConfigParser
from Scripts.DDL.ddl_scripts import *
import time
import os


class SeleniumHandler:
    def get_browser_instance(self, config: RawConfigParser):
        """
        Function to get the browser instance through which the dataset will be downloaded
        """
        try:
            self.download_flight_schedule_data(config)
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
        Function to download the on-time flight data. The data will be downloaded in the Downloads folder inside
        Data-Scraper
        :return:
        """
        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
                  'November', 'December']
        try:
            chromeOptions = webdriver.ChromeOptions()
            prefs = {"download.default_directory": os.path.join(os.getcwd(), "Downloads\\"),
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
                    # ActionChains(browser).click(element).perform()
                    time.sleep(10)
        except Exception as ex:
            print("Error downloading files for flight schedule")
            raise ex
