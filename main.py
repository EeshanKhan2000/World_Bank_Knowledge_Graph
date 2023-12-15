# -*- coding: utf-8 -*-
"""
Created on Sun Dec  3 18:36:14 2023

@author: eesha
"""

from numpy import nan as nan
import pandas as pd
import os
import json

from py2neo import Graph
from langchain.graphs import Neo4jGraph as Graph

from getAPI import apply_filters, get_all_pdf
from multiScrape import scrape
from helpers import documents2Dataframe, df2Graph, graph2Df, contextual_proximity


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

def get_source_data_world_bank(source_data_file_path):
    '''
    This function does not belong here. It belongs in get_source, which is currently called getAPI
    '''
    # read source data and filter
    base_data = pd.read_excel(source_data_file_path)
    base_data.columns = base_data.iloc[1]
    base_data = base_data.drop([0,1])
    base_data = base_data.reset_index(drop=True)

    base_data["closingdate"] = pd.to_datetime(base_data["closingdate"])
    return base_data
            

def make_graph_source(base_folder, graph_source_path, config_data, save_filtered_csv = False):

    source_data_file_path = config_data["scrape_data"]["source_file_path"]
    save_folder = config_data["scrape_data"]["save_pdfs_folder"]
    use_pdf_ner_embeddings = config_data["use_pdf_ner_embedddings"]
    num = config_data["filters"]["num_pdfs"]

    base_data = get_source_data_world_bank(source_data_file_path)    

    if not os.path.exists(save_folder) and use_pdf_ner_embeddings:
        os.makedirs(save_folder)

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


def get_and_save_source_component(base_folder, graph_source_path, config_path):
    with open(config_path ,'r') as f:
        config_data = json.load(f)
    
    make_graph_source(base_folder, graph_source_path, config_data)


def make_graph(graph_source_path, graph_path, **kwargs):
    
    online_graph = kwargs.get("online_graph", None)

    if not os.path.exists(graph_source_path):
        base_folder = kwargs.get("base_folder", '.')
        config_path = kwargs.get("config_path", base_folder + "/params.json")
        get_and_save_source_component(base_folder, graph_source_path, config_path)
    
    source_df = pd.read_csv(graph_source_path)
    df = documents2Dataframe(source_df)

    concepts_list = df2Graph(df) #, model='zephyr:latest')
    dfg1 = graph2Df(concepts_list, online_graph = online_graph)
    
    dfg1.to_csv(graph_path, sep="|", index=False)
    df.to_csv(base_folder + "/chunks.csv", sep="|", index=False)
    

def add_to_graph(graph_path, graph_component_source_path, online_graph = None):
    '''
    graph should already be existing
    '''
    comp_df = pd.read_csv(graph_component_source_path)
    df = documents2Dataframe(comp_df)

    concepts_list = df2Graph(df) #, model='zephyr:latest')
    dfg_comp = graph2Df(concepts_list, online_graph = online_graph)

    dfg = pd.read_csv(graph_path, sep='|')

    dfg = pd.concat([dfg, dfg_comp], axis = 1)
    dfg.to_csv(graph_path, sep = '|')



if __name__ == '__main__':
    url = "neo4j+s://databases.neo4j.io"
    username ="neo4j"
    password = ""
    online_graph = Graph(url = url, username = username, password = password)

    # declare paths and read config
    base_folder = "D:/Coding/ScriptsProjects/ML/NLP/Taiyo/code"
    config_path = base_folder + "/params.json" 
    graph_source_path = base_folder + "/graph_source.csv"
    graph_path = base_folder + "/graph.csv"

    if not os.path.exists(graph_path):
        make_graph(graph_source_path, graph_path, online_graph = online_graph, base_folder = base_folder, config_path = config_path)
    
    # add components if you wish to

    # calculate proximities. Figure if this makes dense graph.

    # query

'''
dfg2 = contextual_proximity(dfg1)

dfg = pd.concat([dfg1, dfg2], axis=0)
dfg = (
    dfg.groupby(["node_1", "node_2"])
    .agg({"chunk_id": ",".join, "edge": ','.join, 'count': 'sum'})
    .reset_index()
)
'''

