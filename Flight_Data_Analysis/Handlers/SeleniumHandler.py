from selenium import webdriver
from configparser import RawConfigParser


class SeleniumHandler():
    def get_browser_instance(self, config: RawConfigParser):
        """
        Function to get the browser instance through which the dataset will be downloaded
        """
        try:
            browser = webdriver.Chrome('../resources/lib/chromedriver.exe')
            browser.get(config.get('Links', 'flight_schedule'))
        except Exception as ex:
            print("Error opening Chrome")
            raise ex
