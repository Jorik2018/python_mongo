from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import os
load_dotenv()
connection_string = os.getenv('AZURE_CONN_STR') 
container_name = os.getenv('AZURE_CONTAINER') 

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def download(blob_name, file_path):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(file_path) as my_blob:
        download_stream = blob_client.download_blob()
        my_blob.write(download_stream.readall())

def upload(file_path, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)

