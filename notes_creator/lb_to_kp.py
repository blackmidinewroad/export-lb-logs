import os

from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()


class Kinopoisk:
    KP_URL = 'https://www.kinopoisk.ru/'

    def __init__(self):
        self.setup_driver()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.driver.quit()

    def setup_driver(self) -> None:
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--headless=new')
        options.add_argument('--log-level=3')
        options.add_argument('--disable-gpu')
        options.add_argument('--enable-unsafe-swiftshader')
        options.add_argument(f'--user-data-dir={os.getenv('CHROME_USER_DATA_DIR')}/Profile 1')

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def is_element_present(self, by: str, value: str, timeout: int = 10) -> bool:
        try:
            WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((by, value)))
            return True
        except (NoSuchElementException, TimeoutException):
            return False

    def transfer_rating_to_kp(self, movie_data: dict) -> bool:
        """Trying to search for the movie using english/original title and release year.
        If found rates movie with rating from letterboxd log and returns True, otherwise return False.
        """

        titles = [movie_data.get('title', ''), movie_data.get('original_title', '')]
        year = movie_data.get('year', '')
        searches = [f'{title} {year}' for title in titles]
        rating = int(movie_data.get('rating', 0) * 2)

        self.driver.get(self.KP_URL)

        for search in searches:
            if self.is_element_present(By.XPATH, "//input[@name='kp_query']"):
                self.driver.find_element(By.XPATH, "//input[@name='kp_query']").send_keys(search)
            else:
                return False

            if self.is_element_present(By.XPATH, "//article[@role='presentation']"):
                self.driver.find_element(By.XPATH, "//article[@role='presentation']").click()
            else:
                self.driver.find_element(By.XPATH, "//input[@name='kp_query']").clear()
                continue

            if self.is_element_present(By.XPATH, f"//label[@data-value='{rating}']"):
                self.driver.find_element(By.XPATH, f"//label[@data-value='{rating}']").click()
            else:
                return False

            if self.is_element_present(By.XPATH, "//span[@class='passp-add-account-page-title']", 5):
                return False
            else:
                return True

        return False
