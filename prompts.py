import json

import transformers
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
from langchain.llms import HuggingFacePipeline
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate

device = 'cuda' if torch.cuda.is_available() else 'cpu'

class Prompt:
    extraction_prompt = (
        "Your task is extract the key concepts (and non personal entities) mentioned in the given context. "
        "Extract only the most important and atomistic concepts, if  needed break the concepts down to the simpler concepts."
        "Categorize the concepts in one of the following categories: "
        "{categories}\n"
        "Format your output as a list of json with the following format:\n"
        "[\n"
        "   {\n"
        '       "entity": The Concept,\n'
        '       "importance": The concontextual importance of the concept on a scale of 1 to 5 (5 being the highest),\n'
        '       "category": The Type of Concept,\n'
        "   }, \n"
        "{ }, \n"
        "]\n"
        "{context}"
    )

    graph_prompt = (
        "You are a network graph maker who extracts terms and their relations from a given context. "
        "You are provided with a context chunk (delimited by ```) Your task is to extract the ontology "
        "of terms mentioned in the given context. These terms should represent the key concepts as per the context. \n"
        "Thought 1: While traversing through each sentence, Think about the key terms mentioned in it.\n"
            "\tTerms may include object, entity, location, organization, person, \n"
            "\tcondition, acronym, documents, service, concept, etc.\n"
            "\tTerms should be as atomistic as possible\n\n"
        "Thought 2: Think about how these terms can have one on one relation with other terms.\n"
            "\tTerms that are mentioned in the same sentence or the same paragraph are typically related to each other.\n"
            "\tTerms can be related to many other terms\n\n"
        "Thought 3: Find out the relation between each such related pair of terms. \n\n"
        "Format your output as a list of json. Each element of the list contains a pair of terms"
        "and the relation between them, like the follwing: \n"
        "[\n"
        "   {\n"
        '       "node_1": "A concept from extracted ontology",\n'
        '       "node_2": "A related concept from extracted ontology",\n'
        '       "edge": "relationship between the two concepts, node_1 and node_2 in one or two sentences"\n'
        "   }, {...}\n"
        "]\n"
        "{context}"
    )

    def __init__(self, model_name = "mistralai/Mistral-7B-Instruct-v0.1"):
        model = AutoModelForCausalLM.from_pretrained(model_name, load_in_4bit=True, device_map='auto')
        tokenizer = AutoTokenizer.from_pretrained(model_name)

        text_generation_pipeline = transformers.pipeline(
            model=model,
            tokenizer=tokenizer,
            task="text-generation",
            temperature=0.2,
            repetition_penalty=1.1,
            return_full_text=True,
            max_new_tokens=1000,
        )

        self.mistral_llm = HuggingFacePipeline(pipeline = text_generation_pipeline)

    def make_prompt(self, input, metadata = {}, extract = False, extract_list = ["event", "concept", "place", "object", "document", "organisation", "condition", "misc"]):
        
        if extract:
            prompt_template = Prompt.extraction_prompt
            prompt = PromptTemplate(
                input_variables=["categories", "context"],
                template=prompt_template,
            )

            input_text = {"categories": str(extract_list), "context": input}

        else:
            prompt_template = Prompt.graph_prompt
            prompt = PromptTemplate(
                input_variables=["context"],
                template=prompt_template,
            )

            input_text = {"context": input}

        llm_chain = LLMChain(llm = self.mistral_llm, prompt = prompt)
        response = llm_chain(input_text)

        try:
            result = json.loads(response)
            result = [dict(item, **metadata) for item in result]
        except:
            print("\n\nERROR ### Here is the buggy response: ", response, "\n\n")
            result = None
        
        return result

