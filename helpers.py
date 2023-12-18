import uuid
import pandas as pd
import numpy as np
from py2neo import Node, Relationship
from numpy import nan
from .prompts import Prompt


def mergeCols(row, column_titles):
    text = ""
    for col in column_titles:
        text += row[col]
        text += '\n'
    text = text[:-1]
    return text


def documents2Dataframe(documents, is_df = True) -> pd.DataFrame:
    rows = []
    if is_df:
        for _, chunk in documents.iterrows():
            row = {
                "text": mergeCols(chunk, documents.columns),
                "chunk_id": row["pid"],
            }
            rows = rows + [row]
    else:
        for chunk in documents:
            row = {
                "text": chunk.page_content,
                **chunk.metadata,
                "chunk_id": uuid.uuid4().hex,
            }
            rows = rows + [row]

    df = pd.DataFrame(rows)
    return df


def df2ConceptsList(dataframe: pd.DataFrame) -> list:
    # dataframe.reset_index(inplace=True)
    p = Prompt()
    results = dataframe.apply(
        lambda row: p.make_prompt(row.text, metadata={"chunk_id": row.chunk_id, "type": "concept"}, extract=True),
        axis = 1
    )
    
    # invalid json results in NaN
    results = results.dropna()
    results = results.reset_index(drop=True)

    ## Flatten the list of lists to one single list of entities.
    concept_list = np.concatenate(results).ravel().tolist()
    return concept_list


def concepts2Df(concepts_list) -> pd.DataFrame:
    ## Remove all NaN entities
    concepts_dataframe = pd.DataFrame(concepts_list).replace(" ", np.nan)
    concepts_dataframe = concepts_dataframe.dropna(subset=["entity"])
    concepts_dataframe["entity"] = concepts_dataframe["entity"].apply(
        lambda x: x.lower()
    )

    return concepts_dataframe


def df2Graph(dataframe: pd.DataFrame, model=None) -> list:
    # dataframe.reset_index(inplace=True)
    p = Prompt(model_name=model)
    results = dataframe.apply(lambda row: p.make_prompt(row.text, metadata={"chunk_id": row.chunk_id}), axis=1)
    
    # invalid json results in NaN
    results = results.dropna()
    results = results.reset_index(drop=True)

    ## Flatten the list of lists to one single list of entities.
    concept_list = np.concatenate(results).ravel().tolist()
    return concept_list


def graph2Df(nodes_list, online_graph = None) -> pd.DataFrame:
    ## Remove all NaN entities
    graph_dataframe = pd.DataFrame(nodes_list).replace(" ", np.nan)
    graph_dataframe = graph_dataframe.dropna(subset=["node_1", "node_2"])
    graph_dataframe["node_1"] = graph_dataframe["node_1"].apply(lambda x: x.lower())
    graph_dataframe["node_2"] = graph_dataframe["node_2"].apply(lambda x: x.lower())

    if online_graph != None:
        for row in graph_dataframe:
            node1 = Node("Node", properties={"node_text": row["node_1"]})
            node2 = Node("Node", properties={"node_text": row["node_2"]})
            relationship = Relationship(node1, "CONNECTED", node2, properties={"edge": row["edge"]})
            online_graph.create(node1, node2, relationship)

    return graph_dataframe


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
    dfg2.replace("", nan, inplace=True)
    dfg2.dropna(subset=["node_1", "node_2"], inplace=True)
    # Drop edges with 1 count
    dfg2 = dfg2[dfg2["count"] != 1]
    #dfg2["edge"] = "contextual proximity"
    return dfg2