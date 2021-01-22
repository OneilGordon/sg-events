import logging
import pandas as pd
import azure.functions as func
from io import StringIO
import os

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
  
    if req.method == "GET":
        return func.HttpResponse("¯\_(ツ)_/¯", status_code=200)


    elif req.method == "POST":
        data = req.get_json()
        new_event_dataframe = pd.json_normalize(data)

        #connect to azure storage client
        connect_str = os.environ["sgeventactivity_STORAGE"]
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client("sgeventdata")
        blob_client = container_client.get_blob_client("sgEventDataHOOK.csv")

        #download the current csv file and convert bytes datastream to pandas dataframe
        try:
            download_stream = blob_client.download_blob()
            s = str(download_stream.readall(), "utf-8")
            old_event_data = StringIO(s)
            current_event_dataframe = pd.read_csv(old_event_data)

            #concat old and new dataframes, drop duplicates then upload to azure
            combined_dataframe = pd.concat([new_event_dataframe, current_event_dataframe])
            combined_data = combined_dataframe.to_csv(index=False)
            blob_client.upload_blob(combined_data, overwrite=True)

        #if the blob file does not exist create a new one
        except ResourceNotFoundError as e:
            new_event_data = new_event_dataframe.to_csv(index=False, header=True)
            blob_client.upload_blob(new_event_data)
       
    return func.HttpResponse("(~_^)", status_code=200)
