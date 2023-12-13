# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 00:06:33 2023

@author: eesha
"""
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


# Set up the Selenium WebDriver
#driver = webdriver.Chrome()  # You need to have chromedriver installed and in your PATH
chrome_options = Options()
chrome_options.add_argument('--headless')  # This line enables headless mode

driver = webdriver.Chrome(ChromeDriverManager().install(), options = chrome_options)
url = "https://projects.worldbank.org/en/projects-operations/project-detail/P177876?lang=en"
driver.get(url)

# Find and click the "show more" button using Selenium
elem = driver.find_element(By.XPATH, '//*[@id="abstract"]/div/div/div[2]/div/div/div/div/a')
driver.execute_script("arguments[0].scrollIntoView();", elem)
elem.click()

#show_more_button = WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.XPATH, '//*[@id="abstract"]/div/div/div[2]/div/div/div/div/a')))
#show_more_button.click()

# Wait for the content to load (adjust the time based on the webpage's behavior)
#sleep(2)

# Get the page source after clicking the "show more" button
page_source = driver.page_source

# Use Beautiful Soup to parse the updated HTML
soup = BeautifulSoup(page_source, "html.parser")

# Now you can extract the desired information from the updated HTML using Beautiful Soup
# For example, if the hidden text is in a div with class "hidden-text"
abs_text = soup.find("div", class_="more _loop_lead_paragraph_sm").get_text()

# Close the browser
driver.quit()

# Process the hidden_text variable as needed
print(abs_text)
