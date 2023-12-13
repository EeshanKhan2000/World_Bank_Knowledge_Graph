import pandas as pd
from datetime import datetime
import json
import requests
from joblib import Parallel, delayed
from tqdm_joblib import tqdm_joblib
import time

'''
Following are other interesting params to capture for llm:
    
project_name
sectors
themes
pdo
'''

def download_pdf(url, save_path):
    try:
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            
            with open(save_path, 'wb') as pdf_file:
                for chunk in response.iter_content(chunk_size=1024):
                    pdf_file.write(chunk)

            print(f"PDF downloaded successfully and saved at: {save_path}")
        else:
            print(f"Failed to download PDF. Status code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred: {e}")
        

def apply_filters(df, config):
    exact_dict = config["filters"]["exact"]
    range_dict = config["filters"]["range"]
    
    for v in exact_dict.values():
        if v["value"] != None:
            df = df[(df[v["colname"]] == v["value"])]
    
    for k,v in range_dict.items():
        low, high = v["range"][0], v["range"][1]
        
        if low != None:
            if k == "date_range":
                low = datetime.strptime(low, "%Y-%m-%d").date()
            df = df[(df[v["colname"]] >= low)]
            
        if high != None:
            if k == "date_range":
                high = datetime.strptime(high, "%Y-%m-%d").date()
            df = df[(df[v["colname"]] <= high)]
    
    return df


def get(pid, save_folder):
    try:
        url = f"https://search.worldbank.org/api/v2/wds?format=json&qterm={pid}&fl=pdfurl"
        time.sleep(2)
        response = requests.get(url)
        
        if response.status_code == 200:
            content_dict = json.loads(response.text)
            
            docs = list(content_dict["documents"].values())
            
            if len(docs) > 0 and len(docs[0]) > 0:
                pdf_url = docs[0]["pdfurl"]
                save_path = save_folder + f'/{pid}.pdf' 
                download_pdf(pdf_url, save_path)
            else:
                print(f"No docs found for {pid}")
            
        else:
            print(f"error in retrieving {pid}")
            
    except Exception as e:
        print(f"An error occurred for {pid}: {e}")
        

def get_all_pdf(pids, save_folder, n_jobs = 15):
    n = len(pids)
    with tqdm_joblib(desc="processed download requests", total=n) as progress_bar:
        Parallel(n_jobs = n_jobs)(delayed(get)(pid, save_folder) for pid in pids)
    
# apply hard-coded nan filters
if __name__ == "__main__":
    save_csv = False

    base_folder = "D:/Coding/ScriptsProjects/ML/NLP/Taiyo/code"
    config_file = base_folder + "/params.json" 
    save_folder = "D:/Coding/ScriptsProjects/ML/NLP/Taiyo/code/wb_pdfs"


    with open(config_file ,'r') as f:
        config_data = json.load(f)


    base_data = pd.read_excel("D:/Coding/ScriptsProjects/ML/NLP/Taiyo/all.xls")
    base_data.columns = base_data.iloc[1]
    base_data = base_data.drop([0,1])
    base_data = base_data.reset_index(drop=True)

    base_data["closingdate"] = pd.to_datetime(base_data["closingdate"])

    df = apply_filters(base_data, config_data)

    if save_csv:
        df.to_csv(base_folder + "/wb-india.csv")
    pids = list(df["id"])

    num_pdfs = config_data["num_pdfs"]
    get_all_pdf(pids[:num_pdfs], save_folder)


###
#get("P502499", save_folder)

