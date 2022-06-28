import os
from pathlib import Path
import json
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

from azure.storage.blob import ContainerClient, BlobClient, BlobServiceClient
import data.interim as interim_data_path
import data.raw as raw_data_path

class DataTransformer:

    def __init__(self, interim_file_name=None):
        if interim_file_name is None:
            interim_file_name = "products.csv"
        self.interim_file_name = interim_file_name
        self.interim_data_file_path = Path(interim_data_path.__file__).parent / self.interim_file_name
        self.raw_data_path = Path(raw_data_path.__file__).parent
        load_dotenv()

    def download_data_from_raw_blob(self):
        container_client = ContainerClient.from_connection_string(
            os.getenv('BLOB_CONNECTION_STRING'), os.getenv('RAW_CONTAINER_NAME')
        )
        blob_list = [blob["name"] for blob in container_client.list_blobs()]
        existing_files = [f.name for f in self.raw_data_path.iterdir() if ".json" in f.name]
        blobs_to_download = [blob for blob in blob_list if blob not in existing_files]

        for blob in tqdm(
            blobs_to_download, desc=f"Downloading new blobs from {os.getenv('RAW_CONTAINER_NAME')}"
        ):
            with open(Path(self.raw_data_path, blob), "w", encoding="utf8") as file:
                data = container_client.get_blob_client(blob=blob).download_blob().readall()
                file.write(data.decode("utf-8"))
    
    def convert_raw_data_to_csv(self):
        """Convert multiple JSON into csv."""
        files = sorted([file for file in self.raw_data_path.rglob("*") if file.suffix == ".json"])

        for index, path in enumerate(tqdm(files, desc="Loading CSV")):
            try:
                with open(path, encoding="utf-8") as file:
                    data = json.load(file)

                dataframe = pd.DataFrame(data)
                dataframe.to_csv(
                    self.interim_data_file_path,
                    mode="w" if index == 0 else "a",
                    index=False,
                    header=(index == 0),
                    sep=";"
                )

            except TypeError as e:
                print(f"Something is wrong in file {path}:", e)
            except json.JSONDecodeError:
                print(f"Something is wrong with JSON formatting in file {path}")

    def upload_transformed_data_to_interim_blob(self):
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))
        blob_client = blob_service_client.get_blob_client(os.getenv("INTERIM_CONTAINER_NAME"), self.interim_file_name)
        
        with open(self.interim_data_file_path, "rb") as data_to_upload:
            blob_client.upload_blob(data_to_upload, overwrite=True)

if __name__ == "__main__":
    data_transformer = DataTransformer()
    data_transformer.download_data_from_raw_blob()
    data_transformer.convert_raw_data_to_csv()
    data_transformer.upload_transformed_data_to_interim_blob()