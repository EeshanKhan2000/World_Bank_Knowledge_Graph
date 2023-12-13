# -*- coding: utf-8 -*-
"""
Created on Fri Nov 24 19:49:28 2023

@author: eesha
"""

from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from lxml import html
from webdriver_manager.chrome import ChromeDriverManager

import json


def worker(pid, prefix, postfix, scrape_xpaths, scrape_titles, options = None):
    url = prefix + pid + postfix
    text_dict = {"pid" : pid}
    try:
        driver = webdriver.Chrome(ChromeDriverManager().install(), options = options)
        driver.get(url)
        
    except Exception as e:
        return f"Error in opening link {url}: {e}"
        
    # Click and scroll
    
    for i in range(len(scrape_xpaths)):
        x = scrape_xpaths[i]
        try:
            find_path = x["find"]
            click_path = x["click"]
        
            elem = driver.find_element(By.XPATH, find_path)
            driver.execute_script("arguments[0].scrollIntoView();", elem)
        
            if len(click_path) != 0:
                elem.click()
                
        except Exception as e:
            print(f"Error in the xpath for {scrape_titles[i]}: {e}")
            continue
        
    page_source = driver.page_source
    driver.close()
    
    tree = html.fromstring(page_source)
    
    # Retrieve text
    
    for i in range(len(scrape_titles)):
        try:
            content_path = scrape_xpaths[i]["content"]
            selection = tree.xpath(content_path)
            
            if selection is not None:
                text = selection[0].text
                text = selection.strip()
                text_dict[scrape_titles[i]] = text 
        except Exception as e:
            print(f"Error in getting text content of the xpath for {scrape_titles[i]}: {e}")
            continue            
        
    return text_dict
    

# impliment limited threads here
def scrape(pids, base_folder):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    config_file = base_folder + "/params.json" 

    with open(config_file, 'r') as f:
        config = json.load(f)

    prefix = config["scrape_data"]["url_prefix"]
    postfix = config["scrape_data"]["url_postfix"]
    scrape_xpaths = config["scrape_data"]["xpaths"]
    scrape_titles = config["scrape_data"]["titles"]
    
    #urls = [prefix + pid + postfix for pid in pids]
    
    with ThreadPoolExecutor(max_workers=len(pids)) as executor:
        results = list(executor.map(lambda p: worker(p, prefix, postfix, scrape_xpaths, scrape_titles, options = chrome_options), pids))
    
    return results


if __name__ == "__main__":
    pids = [
        "P177876",
        "P502499",
        "P502491",
        "P502223",
        "P501071",
        "P500564"
    ]
    #pids = ["P177876"]

    base_folder = "D:/Coding/ScriptsProjects/ML/NLP/Taiyo/code"
    results = scrape(pids, base_folder)

# =============================================================================
# pids = [
#     "P177876",
#     "P502499",
#     "P502491",
#     "P502223",
#     "P501071",
#     "P500564"
# ]
# 
# urls = [prefix + pid + postfix for pid in pids]
# 
# result = worker(urls[0], options = chrome_options)
# 
# ####
# with ThreadPoolExecutor(max_workers=len(urls)) as executor:
#     # Submit tasks to the executor
#     results = list(executor.map(lambda u: worker(u, options=chrome_options), urls))
# =============================================================================

# Now make a main script. Make all needed functions callable in it from other scripts. 
# Then make a script which reads from csv
# Also try to get the all.xls from api, and then filter it, the above step can follow this one.
# Then also modify code to scrape other needed items from here (passed in dict)
# In main, there should be option to do bulk call (async KG) or one call at a time (test with this 1st). 

# Copy KG code from repo as is, modify and run. 
# Query it, and then make demo (as well as front end type thing). 
# Then make doc, diagram, and write further recommendations. 
# Also write neo4j code code to use BERT embeddings as well 
# also NER on pdfs code. 

# Also, further add contracts functionality (1st explore the API for it). 
