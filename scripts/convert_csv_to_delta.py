from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import tempfile
from dotenv import load_dotenv

load_dotenv()

# Configuration Azure
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")  
ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")
CONTAINERS = ["bronze", "silver", "gold"]

# Configuration Spark
builder = SparkSession.builder \
    .appName("CSV to Delta Conversion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.pyspark.python", "C:/Users/mouss/AppData/Local/Programs/Python/Python310/python.exe") \
    .config("spark.pyspark.driver.python", "C:/Users/mouss/AppData/Local/Programs/Python/Python310/python.exe")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Initialisation du service Blob
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=ACCOUNT_KEY
)

def download_csv(container_name, blob_name):
    """
    Télécharge un fichier CSV depuis Azure Blob Storage.
    """
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    download_stream = blob_client.download_blob()
    local_path = os.path.join(tempfile.gettempdir(), blob_name)

    with open(local_path, "wb") as file:
        file.write(download_stream.readall())

    return local_path

def convert_csv_to_delta(local_csv_path, delta_output_path):
    """
    Convertit un fichier CSV en format Delta.
    """
    spark.sql("SET spark.databricks.delta.properties.defaults.enableColumnMapping=true")
    spark.sql("SET spark.databricks.delta.properties.defaults.columnMapping.mode=name")
    
    df = spark.read.format("csv").option("header", "true").load(local_csv_path)
    df.write.format("delta").mode("overwrite").save(delta_output_path)


def upload_delta_to_azure(local_delta_path, container_name, delta_blob_path):
    """
    Upload un dossier Delta vers Azure Blob Storage.
    """
    container_client = blob_service_client.get_container_client(container_name)

    for root, _, files in os.walk(local_delta_path):
        for file in files:
            file_path = os.path.join(root, file)
            blob_path = os.path.join(delta_blob_path, os.path.relpath(file_path, local_delta_path)).replace("\\", "/")
            blob_client = container_client.get_blob_client(blob_path)

            with open(file_path, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)

def process_container(container_name):
    """
    Traite tous les fichiers CSV dans un conteneur Azure.
    """
    print(f"Traitement des fichiers dans le conteneur : {container_name}")

    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()

    for blob in blob_list:
        if blob.name.endswith(".csv"):
            print(f"Traitement du fichier : {blob.name}")

            # Télécharger le fichier CSV
            local_csv_path = download_csv(container_name, blob.name)

            # Convertir en Delta
            delta_output_path = os.path.join(tempfile.gettempdir(), "delta", blob.name.split(".")[0])
            convert_csv_to_delta(local_csv_path, delta_output_path)

            # Upload Delta dans Azure
            delta_blob_path = blob.name.split(".")[0]
            upload_delta_to_azure(delta_output_path, container_name, delta_blob_path)

            print(f"Conversion et upload terminés pour : {blob.name}")

def main():
    """
    Point d'entrée principal du script.
    """
    for container in CONTAINERS:
        process_container(container)

if __name__ == "__main__":
    main()
