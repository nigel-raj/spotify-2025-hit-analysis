"""
Spotify Charts Scraper

Automates downloading daily Spotify Global Top 200 CSV files
from charts.spotify.com using Selenium.
"""

import os
import time
import random
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


# =========================
# Configuration
# =========================

LOGIN_URL = "https://charts.spotify.com/login"
BASE_CHART_URL = "https://charts.spotify.com/charts/view/regional-global-daily"

DOWNLOAD_FOLDER = "downloads"
START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

LOGIN_WAIT_TIME = 60
MIN_DELAY = 3
MAX_DELAY = 6


# =========================
# Logging Setup
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("spotify_scraper.log"),
        logging.StreamHandler()
    ]
)


# =========================
# Utility Functions
# =========================

def setup_driver(download_folder: str):
    """Initialize Chrome WebDriver with download preferences."""
    options = webdriver.ChromeOptions()

    prefs = {
        "download.default_directory": os.path.abspath(download_folder),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }

    options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    return driver


def wait_for_manual_login(driver, wait_time: int):
    """Allow user to manually log in."""
    logging.info("Please complete login in the browser window.")
    time.sleep(wait_time)
    logging.info("Login wait complete.")


def generate_dates(start_date: str, end_date: str):
    """Generate date strings between start and end date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def click_download_button(driver) -> bool:
    """Attempt to locate and click the CSV download button."""
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(2)

        button = driver.find_element(By.XPATH, "//button[@aria-labelledby='csv_download']")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        button.click()

        return True

    except Exception:
        logging.warning("Download button not found.")
        return False


def rename_latest_csv(download_folder: str, date_str: str, timeout: int = 10):
    """Rename most recent CSV file to match date."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        files = [
            f for f in os.listdir(download_folder)
            if f.endswith(".csv")
        ]

        if files:
            files.sort(
                key=lambda f: os.path.getmtime(os.path.join(download_folder, f)),
                reverse=True
            )

            latest = files[0]
            source_path = os.path.join(download_folder, latest)
            target_path = os.path.join(download_folder, f"{date_str}.csv")

            if not os.path.exists(target_path):
                os.rename(source_path, target_path)
                return True

        time.sleep(0.5)

    logging.warning(f"Could not rename file for {date_str}")
    return False


# =========================
# Main Workflow
# =========================

def main():

    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

    driver = setup_driver(DOWNLOAD_FOLDER)

    try:
        driver.get(LOGIN_URL)
        wait_for_manual_login(driver, LOGIN_WAIT_TIME)

        dates = list(generate_dates(START_DATE, END_DATE))
        logging.info(f"Processing {len(dates)} dates.")

        for index, date_str in enumerate(dates, start=1):

            file_path = os.path.join(DOWNLOAD_FOLDER, f"{date_str}.csv")
            if os.path.exists(file_path):
                logging.info(f"Skipping {date_str} (already exists)")
                continue

            url = f"{BASE_CHART_URL}/{date_str}"
            driver.get(url)

            if click_download_button(driver):
                rename_latest_csv(DOWNLOAD_FOLDER, date_str)

            if index < len(dates):
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                time.sleep(delay)

        logging.info("Download process completed.")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()