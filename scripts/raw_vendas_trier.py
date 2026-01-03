import os
import time
import logging
import shutil
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# set up logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
username = os.getenv("trier_user")
password = os.getenv("trier_password")

if not username or not password:
    raise ValueError("Environment variables 'user' and/or 'password' not set.")

# Calculate date range
today = datetime.today()
report_date = today - timedelta(days=1)
start_date = report_date - timedelta(days=1) if report_date.weekday() == 6 else report_date
inicio = f"{start_date.strftime('%d/%m/%Y')}"  
fim = f"{report_date.strftime('%d/%m/%Y')}"

download_dir = os.getcwd()

# set up chrome options for headless mode/configure download behavior
chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--enable-downloads")  # Explicitly enable downloads
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")  # Set dimensions
chrome_options.add_argument("--start-maximized")  # Maximize window
chrome_options.add_argument("--force-device-scale-factor=1")  # Prevent scaling

prefs = {
    "download.default_directory": download_dir,  # set download path
    "plugins.always_open_pdf_externally": True, # auto-downloads pdf files instead of opening in new window
    "download.open_pdf_in_system_reader": False,
    "pdfjs.disabled": True,  # Disable built-in PDF viewer
    "download.prompt_for_download": False,  # disable prompt
    "directory_upgrade": True,  # auto-overwrite existing files
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--unsafely-treat-insecure-origin-as-secure=http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")

# initialize webdriver
driver = webdriver.Chrome(options=chrome_options)

# start download process 
try:
    logging.info("Navigate to the target URL and login")
    driver.get("http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")
    
    # Add this at startup
    logging.info(f"Download directory set to: {download_dir}")
    os.makedirs(download_dir, exist_ok=True)

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "id_cod_usuario"))).send_keys(username)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "nom_senha"))).send_keys(password)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "login"))).click()

    # wait til page loads completely
    WebDriverWait(driver, 10).until(lambda x: x.execute_script("return document.readyState === 'complete'"))
    time.sleep(5)

    # access "Relação de Vendas"
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "sideMenuSearch")))
    driver.find_element(By.ID, "sideMenuSearch").send_keys("Relação de Vendas")
    driver.find_element(By.ID, "sideMenuSearch").click()
    time.sleep(5)

    driver.find_element(By.CSS_SELECTOR, '[title="Relação de Vendas"]').click()
    time.sleep(10)
    
    tipo_cartao = ["1", "9", "10", "11", "16", "17"]
        
    for codigo in tipo_cartao:
        input_element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "cod_cartaoEntrada"))
        )
        input_element.send_keys(codigo)
        input_element.send_keys(Keys.ENTER)
        time.sleep(2)
        
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "tabTabdhtmlgoodies_tabView1_1"))).click()
    time.sleep(2)
    
    # start and end dates
    driver.find_element(By.ID, "dat_inicio").send_keys(inicio)
    time.sleep(5)
    driver.find_element(By.ID, "dat_fim").send_keys(fim)
    time.sleep(2)
        
    # report format; downloads pdf file
    driver.find_element(By.ID, "saida_4").click()
    time.sleep(2)
          
    # trigger report download
    logging.info("Triggering report download...")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "runReport"))).click()

    # log download start
    logging.info("Download has started.")
    time.sleep(10)

    # get the most recent downloaded file
    files = os.listdir(download_dir)
    downloaded_files = [f for f in files if f.endswith('.xls')]
    if downloaded_files:
        downloaded_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)))
        most_recent_file = downloaded_files[-1]
        downloaded_file_path = os.path.join(download_dir, most_recent_file)

        # rename the file 
        new_filename = f"relacao_vendas.xls"
        new_filepath = os.path.join(download_dir, new_filename)

        # make sure not to overwrite existing file
        if os.path.exists(new_filepath):
            os.remove(new_filepath)

        shutil.move(downloaded_file_path, new_filepath)

        file_size = os.path.getsize(new_filepath)
        logging.info(f"File renamed to {new_filename}. Size: {file_size} bytes")
    else:
        logging.error("Download failed. No files found.")
        time.sleep(5)

finally:
    driver.quit()
