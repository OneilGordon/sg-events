import datetime
import logging
import requests
import pandas as pd
from io import StringIO
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError

#get activity data from api and convert to pandas dataframe
def get_api_data():
    url = "https://api.sendgrid.com/v3/messages?limit=1000"
    headers = {
    "Authorization": os.environ["sgeventactivity_BEARER"]
    }

    response = requests.request("GET", url, headers=headers)
    data = response.json()
    data = data["messages"]
    new_event_dataframe = pd.json_normalize(data)
    return new_event_dataframe

#connect to azure storage client
def azure_connect():
    connect_str=os.environ["sgeventactivity_STORAGE"]
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client("sgeventdata")
    blob_client = container_client.get_blob_client("sgEventData.csv")
    return blob_client

def download_combine(new_event_dataframe, blob_client):
    #download the current csv file and convert bytes datastream to pandas dataframe
    try:
        download_stream = blob_client.download_blob()
        s = str(download_stream.readall(), "utf-8")
        old_event_data = StringIO(s)
        current_event_dataframe = pd.read_csv(old_event_data)

        #concat old and new dataframes, drop duplicates then upload to azure
        combined_dataframe = pd.concat([new_event_dataframe, current_event_dataframe]).drop_duplicates(subset=["msg_id"])
        combined_data = combined_dataframe.to_csv(index=False)
        blob_client.upload_blob(combined_data, overwrite=True)

    #if the blob file does not exist create a new one
    except ResourceNotFoundError as e:
        new_event_data = new_event_dataframe.to_csv(index=False, header=True)
        blob_client.upload_blob(new_event_data)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    #run api download and update tasks
    download_combine(get_api_data(), azure_connect())
