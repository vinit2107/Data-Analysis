from selenium import webdriver
from selenium.webdriver import ActionChains
from configparser import RawConfigParser
from Scripts.DDL.ddl_scripts import *
import time


class SeleniumHandler():
    def get_browser_instance(self, config: RawConfigParser):
        """
        Function to get the browser instance through which the dataset will be downloaded
        """
        try:
            chromeOptions = webdriver.ChromeOptions()
            path = {"download.default_directory": "../downloads//"}
            chromeOptions.add_experimental_option("prefs", path)
            browser = webdriver.Chrome('../resources/lib/chromedriver.exe', chrome_options=chromeOptions)
            browser.get(config.get('Links', 'flight_schedule'))
            self.select_required_columns(browser, flight_schedule_columns)
            time.sleep(10)
            element = browser.find_element_by_xpath("//*[@id='content']/table[1]/tbody/tr/td[2]/table[3]/tbody/tr/td[2]"
                                                    "/button[1]")
            ActionChains(browser).click(element).perform()
            time.sleep(60)
        except Exception as ex:
            print("Error opening Chrome")
            raise ex

    def select_required_columns(self, browser, columns: list):
        try:
            for column in columns:
                browser.find_element_by_xpath("//input[@type='checkbox' and @title='{}']".format(column)).click()
        except Exception as ex:
            print("Error selecting columns in the table")
            raise ex
