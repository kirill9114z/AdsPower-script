import pandas as pd
import threading
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementClickInterceptedException
import requests
import logging
from config import (
    DATA_FILE_PATH, TOTAL_PROFILES, PROFILES_PER_BATCH, API_KEY, CHROME_DRIVER_VERSION,
    PROFILE_DELAY_MIN, PROFILE_DELAY_MAX, MESSAGE_DELAY_MIN, MESSAGE_DELAY_MAX, MESSAGE_MIN, MESSAGE_MAX, SENDING_SAME_MESSAGE
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

data = pd.read_excel(DATA_FILE_PATH)
current_index = 0
lock = threading.Lock()
sent_messages = 0
text_index = 0
text = data['текст'].iloc[0].split('\n')
texts = list(filter(None, text))


def get_all_profile_ids():
    profile_ids = []
    try:
        response = requests.get(
            f"http://local.adspower.net:50325/api/v1/user/list?page=1&page_size=100",
            headers={"Content-Type": "application/json", "Authorization": API_KEY}
        )

        data = response.json()
        if data["code"] != 0:
            print(f"Ошибка API: {data['msg']}")
            return []

        profiles = data["data"]["list"]
        for profile in profiles:
            profile_ids.append(profile["user_id"])

    except Exception as e:
        print(f"Произошла ошибка: {e}")

    return profile_ids

def check_current_url(driver):
    current_url = driver.current_url
    logging.info(f"Текущий URL: {current_url}")
    return 'mail.google.com' in current_url


def get_next_row():
    """Возвращает следующую строку из данных или None, если данные закончились."""
    global current_index
    with lock:
        if current_index < len(data):
            row = data.iloc[current_index]
            current_index += 1
            return row
        return None


def start_ads_power_browser(profile_id, api_token):
    try:
        response = requests.get(f"http://local.adspower.net:50325/api/v1/browser/start?user_id={profile_id}", headers={
            'Authorization': f'Bearer {api_token}',
        })
        if response.status_code != 200:
            print(f"Ошибка HTTP: {response.status_code}")
            return None

        data = response.json()
        if data.get("code") != 0:
            raise Exception(f"Ошибка API: {data.get('msg', 'Неизвестная ошибка')}")

        selenium_address = data["data"]["ws"]["selenium"]
        logging.info(f"Получен адрес для Selenium: {selenium_address}")

        options = webdriver.ChromeOptions()
        options.debugger_address = selenium_address
        options.add_argument("--headless")
        options.add_argument("--start-maximized")

        service = Service(ChromeDriverManager(driver_version=CHROME_DRIVER_VERSION).install())
        driver = webdriver.Chrome(service=service, options=options)
        logging.info("Успешно подключен к браузеру")
        return driver
    except Exception as e:
        logging.error(f"Ошибка подключения к профилю {profile_id}: {str(e)}")
        return None


def is_logged_in(driver):
    """Проверяет, авторизован ли пользователь в Gmail."""
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[6]/div[3]/div/div[2]/div[1]/div[1]/div/div"))
        )
        return True
    except:
        return False


def try_login(driver):
    """Пытается войти в Gmail-аккаунт."""
    try:
        if not is_logged_in(driver):
            try:
                cl_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//div[@role='link' and @data-authuser='-1']"))
                )
                cl_button.click()
                time.sleep(2)
            except:
                time.sleep(1)
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[text()='Next']"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                next_button.click()
            except ElementClickInterceptedException:
                next_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//span[text()='Next']"))
                )
                driver.execute_script("arguments[0].click();", next_button)
            except Exception as e:
                print(f"Ошибка при клике на 'Next': {e}")
            time.sleep(2)
            return is_logged_in(driver)
        return True
    except Exception as e:
        logging.error(f"Ошибка при входе: {str(e)}")
        return False


def send_email(driver, recipient_email, subject, body):
    """Отправляет письмо через Gmail."""
    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div[6]/div[3]/div/div[2]/div[1]/div[1]/div/div'))
        ).click()
        time.sleep(2)

        try:
            # Ждём до 5 секунд, пока крестик не станет кликабельным
            close_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='bBe']"))
            )
            close_button.click()
        except TimeoutException:
            logging.info('1')
        try:
            to_field = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, "//input[@aria-label='Получатели в поле \"Кому\"']"))
            )
            to_field.clear()
            to_field.send_keys(recipient_email)
            time.sleep(1)
        except Exception as e:
            print(f"Ошибка при вводе в поле 'Кому': {e}")

        subject_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'aoT'))
        )
        subject_field.send_keys(subject)
        time.sleep(2)

        body_field = WebDriverWait(driver, 15).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@aria-label='Текст письма']"))
        )
        body_field.send_keys(body)
        time.sleep(1)

        try:
            cl_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id=":b"]/div/div/div[2]/div'))
            )
            cl_button.click()
        except:
            pass

        send_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[@role='button' and @data-tooltip-delay='800']"))
        )
        send_button.click()
        time.sleep(2)
        logging.info(f"Письмо отправлено на {recipient_email}")
        return True
    except Exception as e:
        logging.error(f"Ошибка при отправке письма: {str(e)}")
        return False


def process_profile(profile_id, api_token):
    """Обрабатывает профиль AdsPower: открывает вкладки и отправляет письма."""
    global sent_messages, text_index
    driver = start_ads_power_browser(profile_id, api_token)
    if not driver:
        return

    try:
        tabs = driver.window_handles
        for tab_idx, tab in enumerate(tabs):
            driver.switch_to.window(tab)
            if not check_current_url(driver):
                logging.warning(f"Не та вкладка")
                continue

            if not is_logged_in(driver):
                if not try_login(driver):
                    logging.warning(f"Не удалось авторизоваться во вкладке {tab_idx + 1}")
                    continue

            emails_to_send = random.randint(MESSAGE_MIN, MESSAGE_MAX)

            for _ in range(emails_to_send):
                row = get_next_row()
                if row is None:
                    break

                recipient_email = row['email']
                subject = row['ФИО']

                with lock:
                    text_index = (sent_messages // SENDING_SAME_MESSAGE) % len(texts)
                    body = texts[text_index]
                    sent_messages += 1
                if send_email(driver, recipient_email, subject, body):
                    time.sleep(random.uniform(MESSAGE_DELAY_MIN, MESSAGE_DELAY_MAX))

            logging.info(f"Вкладка {tab_idx + 1} в профиле {profile_id} завершила отправку")

    except Exception as e:
        logging.error(f"Ошибка в профиле {profile_id}: {str(e)}")
    finally:
        requests.get(f'http://local.adspower.net:50325/api/v1/browser/stop?user_id={profile_id}')


def main():
    """Запускает обработку профилей пачками."""
    ID = get_all_profile_ids()
    for batch_start in range(0, len(ID), PROFILES_PER_BATCH):
        batch_profiles = ID[batch_start:batch_start + PROFILES_PER_BATCH]
        threads = []

        for profile_id in batch_profiles:
            t = threading.Thread(target=process_profile, args=(profile_id, API_KEY))
            threads.append(t)
            t.start()
            time.sleep(random.uniform(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX))

        for t in threads:
            t.join()

        logging.info(f"Пачка {batch_start // PROFILES_PER_BATCH + 1} завершена")


if __name__ == "__main__":
    main()
