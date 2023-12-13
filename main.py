# -*- coding: utf-8 -*-
"""
Created on Sun Dec  3 18:36:14 2023

@author: eesha
"""

import pandas as pd
import os
import json

from getAPI import apply_filters, get_all_pdf
from multiScrape import scrape
from helpers import documents2Dataframe, df2Graph, graph2Df


def make_final_df(scraped_results, df):
    n = len(scraped_results)
    data = {"pid": [], "project_name": []}

    for i in range(n):
        d = scraped_results[i]
        for k in d:
            if k not in data:
                data[k] = []
            data[k].append(d[k])

        data["project_name"] = df["project_name"][df["id"] == d["pid"]]
    
    save_df = pd.DataFrame(data)
    return save_df

            
def make_source_df(source_data_file_path, save_folder, graph_source_path, config_data, base_folder, save_filtered_csv = False, use_pdf_ner_embeddings = False):
    if not os.path.exists(save_folder) and use_pdf_ner_embeddings:
        os.makedirs(save_folder)

    # read source data and filter
    base_data = pd.read_excel(source_data_file_path)
    base_data.columns = base_data.iloc[1]
    base_data = base_data.drop([0,1])
    base_data = base_data.reset_index(drop=True)

    base_data["closingdate"] = pd.to_datetime(base_data["closingdate"])

    df = apply_filters(base_data, config_data)

    if save_filtered_csv:
        df.to_csv(base_folder + "/wb-india.csv")

    pids = list(df["id"])
    num = config_data["filters"]["num_pdfs"]

    # scrape and get scraped results
    scraped_results = scrape(pids[:num], base_folder)

    save_df = make_final_df(scraped_results, df)
    save_df.to_csv(graph_source_path)

    # save project pdfs. note: further processing needs to be done here, but will be coded later. 
    if use_pdf_ner_embeddings:
        get_all_pdf(pids[:num], save_folder)
    
    #return scraped_results


def contextual_proximity(df: pd.DataFrame) -> pd.DataFrame:
    ## Melt the dataframe into a list of nodes
    dfg_long = pd.melt(
        df, id_vars=["chunk_id"], value_vars=["node_1", "node_2"], value_name="node"
    )
    dfg_long.drop(columns=["variable"], inplace=True)
    # Self join with chunk id as the key will create a link between terms occuring in the same text chunk.
    dfg_wide = pd.merge(dfg_long, dfg_long, on="chunk_id", suffixes=("_1", "_2"))
    # drop self loops
    self_loops_drop = dfg_wide[dfg_wide["node_1"] == dfg_wide["node_2"]].index
    dfg2 = dfg_wide.drop(index=self_loops_drop).reset_index(drop=True)
    ## Group and count edges.
    dfg2 = (
        dfg2.groupby(["node_1", "node_2"])
        .agg({"chunk_id": [",".join, "count"]})
        .reset_index()
    )
    dfg2.columns = ["node_1", "node_2", "chunk_id", "count"]
    dfg2.replace("", np.nan, inplace=True)
    dfg2.dropna(subset=["node_1", "node_2"], inplace=True)
    # Drop edges with 1 count
    dfg2 = dfg2[dfg2["count"] != 1]
    dfg2["edge"] = "contextual proximity"
    return dfg2


# declare paths and read config
base_folder = "D:/Coding/ScriptsProjects/ML/NLP/Taiyo/code"
config_file = base_folder + "/params.json" 
graph_source_path = base_folder + "/graph_source.csv"

with open(config_file ,'r') as f:
    config_data = json.load(f)

save_folder = config_data["scrape_data"]["save_pdfs_folder"]
source_data_file_path = config_data["scrape_data"]["source_file_path"]
use_pdf_ner_embeddings = config_data["use_pdf_ner_embedddings"]

# graph
if not os.path.exists(graph_source_path):
    # make graph
    make_source_df(source_data_file_path, save_folder, graph_source_path, config_data, base_folder, use_pdf_ner_embeddings = use_pdf_ner_embeddings)
    source_df = pd.read_csv(graph_source_path)
    df = documents2Dataframe(source_df)

    concepts_list = df2Graph(df) #, model='zephyr:latest')
    dfg1 = graph2Df(concepts_list)
    
    dfg1.to_csv(graph_source_path, sep="|", index=False)
    df.to_csv(base_folder + "/chunks.csv", sep="|", index=False)

else:
    dfg1 = pd.read_csv(graph_source_path, sep="|")

dfg2 = contextual_proximity(dfg1)

dfg = pd.concat([dfg1, dfg2], axis=0)
dfg = (
    dfg.groupby(["node_1", "node_2"])
    .agg({"chunk_id": ",".join, "edge": ','.join, 'count': 'sum'})
    .reset_index()
)


# Will have to make functionality for adding new items to this saved df. Or else will need to use SQL. 