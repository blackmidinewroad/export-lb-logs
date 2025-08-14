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

CHROME_DRIVER_FILE = os.getenv('CHROME_DRIVER_FILE')
CHROME_USER_DATA_DIR = os.getenv('CHROME_USER_DATA_DIR')
KP_URL = 'https://www.kinopoisk.ru/'


def setup_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless=new')
    options.add_argument('--log-level=3')
    options.add_argument('--disable-gpu')
    options.add_argument(f'--user-data-dir={CHROME_USER_DATA_DIR}/Profile 1')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver


def is_element_present(driver: webdriver.Chrome, by: str, value: str, timeout: int = 10) -> bool:
    try:
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
        return True
    except (NoSuchElementException, TimeoutException):
        return False


def transfer_rating_to_kp(movie_data: dict) -> None:
    '''Trying to search for the movie using english/original/russian title and release year.
    If found rates movie with rating from letterboxd log.
    '''

    titles = [movie_data.get('title', '')]
    for other_title in movie_data['other_titles']:
        titles.append(other_title)
    year = movie_data.get('year', '')
    searches = [f'{title} {year}' for title in titles]
    rating = int(movie_data.get('rating', 0) * 2)
    driver = setup_driver()
    driver.get(KP_URL)

    for search in searches:
        if is_element_present(driver, By.XPATH, "//input[@name='kp_query']"):
            driver.find_element(By.XPATH, "//input[@name='kp_query']").send_keys(search)
        else:
            return False

        if is_element_present(driver, By.XPATH, "//article[@role='presentation']"):
            driver.find_element(By.XPATH, "//article[@role='presentation']").click()
        else:
            driver.find_element(By.XPATH, "//input[@name='kp_query']").clear()
            continue

        if is_element_present(driver, By.XPATH, f"//label[@data-value='{rating}']"):
            driver.find_element(By.XPATH, f"//label[@data-value='{rating}']").click()
        else:
            return False

        if is_element_present(driver, By.XPATH, "//span[@class='passp-add-account-page-title']", 5):
            return False
        else:
            return True

    driver.quit()
