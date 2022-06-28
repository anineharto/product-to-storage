import os
from tqdm import tqdm
from dotenv import load_dotenv

from azure.storage.blob import ContainerClient

class DataTransformer:

    def __init__(self):
        load_dotenv()

    def get_data_from_raw(self):
        container_client = ContainerClient.from_connection_string(
            os.getenv("BLOB_CONNECTION_STRING"), os.getenv("RAW_CONTAINER_NAME")
        )
        blob_list = [blob["name"] for blob in container_client.list_blobs()]
        existing_files = [f.name for f in data_path.iterdir() if ".json" in f.name]
        blobs_to_download = [blob for blob in blob_list if blob not in existing_files]

        for blob in tqdm(
            blobs_to_download, desc=f"Downloading new blobs from {container_name}"
        ):
            with open(Path(data_path, blob), "w", encoding="utf8") as file:
                data = container_client.get_blob_client(blob=blob).download_blob().readall()
                file.write(data.decode("utf-8"))
        