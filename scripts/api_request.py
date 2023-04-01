import requests
import os
import json
import pandas as pd
import numpy as np
from datetime import date
import time
import concurrent.futures
from utils.exceptions import InvalidFutureResponse
from dotenv import load_dotenv

load_dotenv() 

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

def split_list(list_a, chunk):
 
    """
    Function that splis a list into even-sized chunks
    """
    
    for i in range(0, len(list_a), chunk):
        yield list_a[i:i + chunk]

def chunkSize(numberofRequests):
    
    if numberofRequests <= 1000:
            chunk_size = 10 
    elif (numberofRequests > 1000) & (numberofRequests < 10000):
            chunk_size = 50 
    else:
            chunk_size = 100  

    return chunk_size

def createApiKey(client_id:str, client_secret:str):

    """
    Creates an API Key and append it to a list.
    """

    url = "https://api.pluggy.ai/auth"

    payload = {
        "clientId": client_id,
        "clientSecret": client_secret
    }

    headers = {
        "accept":"application/json",
        "content-type":"application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        raise response.raise_for_status()

    apiKey = json.loads(response.text)["apiKey"]

    return apiKey

def split_dataframe(df, chunk_size = 100): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

def csvToJson(df:pd.DataFrame):

    """
    Converts the CSV input file to a JSON Array

    The csv must present the following columns:
    
        - description
        - id (accepts NULL values)
        - amount (accepts NULL values)
        - date (accepts NULL values)
        
    """

    df = df.astype(str)

    if df.shape[0] > 100:

        chunks = df.shape[0] / 100 

        if df.shape[0] % 100 != 0:
            chunks += 1

        dfs = np.array_split(df, chunks)
    
        json_strs = [df.to_json(orient="records") for df in dfs]

        json_array_lst = [json.loads(json_str) for json_str in json_strs]

        json_formatted = [json.dumps(json_array, indent=2) for json_array in json_array_lst]

        return json_array_lst

    else:

        json_str = df.to_json(orient="records")

        json_array = json.loads(json_str)

        json_formatted_str = json.dumps(json_array, indent=2)

        return [(json_array)]


def enrichmentApiCall(json_array, clientUserId=None):

    """
    Calls the Data Enrichment API.

    Input: JSON Array no longer than 100 objects
    Output: CSV file categorized and with Merchant if available

    """

    url = "https://enrichment-api.pluggy.ai/categorize"

    today = date.today()
    date_now = today.strftime("%Y-%m-%d")

    headers = {
        "content-type":"application/json",
        "connection":"keep-alive",
        "date": date_now,
        "X-API-KEY": os.environ.get("X_API_KEY")
    }

    payload = {
        "clientUserId": "123456789",
        "transactions": json_array #needs to be an array
    }

    try:

        print("trying to make the request")
        response = requests.post(url, json = payload, headers=headers)
        if response.status_code == 200:
            print("Sucessful request!")

            json_str = response.text

            json_dict = json.loads(json_str)

            json_formatted_str = json.dumps(json_dict, indent=2)       

            results = json_dict["results"]

            df = pd.json_normalize(results).astype("str")

            return df
        else:
            print(response.raise_for_status())
    
    except BaseException as e:
        raise e
        

def concat_results(lst):
    
    df_final = pd.concat(lst, ignore_index=True)
    return df_final

def export_to_csv(df:pd.DataFrame, fileName:str="result"):
    
    return df.to_csv(f"{fileName}.csv", index=False, encoding='utf-8')


def concat_export_csv(lst_dfs, fileName:str="result") -> pd.DataFrame:

    if len(lst_dfs) > 1:
        dfs = concat_results(lst_dfs)

        export_to_csv(dfs, fileName=fileName)

    else:
        export_to_csv(lst_dfs[0], fileName=fileName)


def callSync(df:pd.DataFrame, fileFinalName:str):

        arrays_lst = csvToJson(df)

        responses = []

        if len(arrays_lst) > 1:
        
            for index, json_array in enumerate(arrays_lst):
                    print(f"Request {index+1}")
                    try:
                        response = enrichmentApiCall(json_array)
                        if type(response) != pd.DataFrame:
                            raise "The output is not a DataFrame"
                        responses.append(response)    
                    except BaseException as e:
                        raise e
                             
        else:
            
            try:
                response = enrichmentApiCall(arrays_lst[0]) #list
                if type(response) != pd.DataFrame:
                    raise "The output is not a DataFrame"
                responses.append(response)

            except BaseException as e: 
                raise e

        concat_export_csv(responses, fileFinalName) 


def callAsync(df:pd.DataFrame, fileFinalName:str, numberofRequests:int):

    async_results = []

    arrays_lst = csvToJson(df)

    chunk_size = chunkSize(numberofRequests)

    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:

        t3 = time.perf_counter()

        future_lst = []

        splitted_arrays_lst = split_list(arrays_lst, chunk_size)

        for lst in splitted_arrays_lst:

            future_lst = []

            for index,json_array in enumerate(lst):
                print(f"Request {index+1} sent", flush=True)
                result = executor.submit(enrichmentApiCall, json_array)
                print("Result", result)
                future_lst.append(result)      

            for index2, future in enumerate(concurrent.futures.as_completed(future_lst)):
                print(f"Received request {index2 + 1}", flush=True)
                # print(future.result())
                if type(future.result()) != pd.DataFrame:
                    raise InvalidFutureResponse()
                async_results.append(future.result())

        t4 = time.perf_counter()

        print("All set!")

        print(f"Requests made and received in {t4 - t3} secs")


    print("Concatenating dfs")

    t1 = time.perf_counter()

    concat_export_csv(async_results, fileFinalName)

    t2 = time.perf_counter()
    
    print(f"Concatenated in {t2 - t1} secs")

    return "The categorized df was exported"