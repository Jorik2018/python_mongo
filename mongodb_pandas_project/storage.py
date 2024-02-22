from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import os
load_dotenv()
connection_string = os.getenv('AZURE_CONN_STR') 
container_name = os.getenv('AZURE_CONTAINER') 

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def download(blob_name, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(file_path, 'wb') as my_blob:
        try:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())
            return True
        except Exception as e:
            print(e)
            

def upload(file_path, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

