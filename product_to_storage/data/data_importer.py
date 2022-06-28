import requests
import json
import os
from dotenv import load_dotenv

from azure.storage.blob import BlobClient, BlobServiceClient

class DataImporter:

    def __init__(self, raw_file_name=None):
        if raw_file_name is None:
            raw_file_name = "products.json"
        self.raw_file_name = raw_file_name
        load_dotenv()
    
    def upload_data_to_raw_blob(self):
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))
        blob_client = blob_service_client.get_blob_client(os.getenv("RAW_CONTAINER_NAME"), self.raw_file_name)
        
        data = requests.get(os.getenv("DATA_API_NAME")).json()
        data_to_upload = json.dumps(data)
        blob_client.upload_blob(data_to_upload, overwrite=True)

if __name__ == "__main__":
    DataImporter().upload_data_to_raw_blob()