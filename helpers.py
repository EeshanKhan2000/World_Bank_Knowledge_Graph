import uuid
import pandas as pd
import numpy as np
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


def graph2Df(nodes_list) -> pd.DataFrame:
    ## Remove all NaN entities
    graph_dataframe = pd.DataFrame(nodes_list).replace(" ", np.nan)
    graph_dataframe = graph_dataframe.dropna(subset=["node_1", "node_2"])
    graph_dataframe["node_1"] = graph_dataframe["node_1"].apply(lambda x: x.lower())
    graph_dataframe["node_2"] = graph_dataframe["node_2"].apply(lambda x: x.lower())

    return graph_dataframe