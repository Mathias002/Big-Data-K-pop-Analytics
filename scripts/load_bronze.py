import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

load_dotenv()

# Configurations Azure
ACCOUNT_NAME = "kpopdatasets"
ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

# Local directories
LOCAL_CSV_BRONZE = "../data/csv/bronze"
LOCAL_CSV_SILVER = "../data/csv/silver"
LOCAL_DELTA_BRONZE = "../data/delta/bronze"
LOCAL_DELTA_SILVER = "../data/delta/silver"

# Liste des fichiers à charger
FILES = [
    "kpop_idols.csv",
    "kpop_idols_boy_groups.csv",
    "kpop_idols_girl_groups.csv",
    "kpop_music_videos.csv"
]

# Initialisation du service client Azure Blob
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=ACCOUNT_KEY
)

# Configuration de Spark
builder = SparkSession.builder \
    .appName("CSV to Delta Conversion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

def read_csv_from_blob(container_name, blob_name):
    """
    Lit un fichier CSV depuis Azure Blob Storage.
    """
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob().readall()
    return pd.read_csv(StringIO(stream.decode('utf-8')))

def save_csv_to_blob(container_name, blob_name, dataframe):
    """
    Enregistre un DataFrame sous forme de CSV dans Azure Blob Storage.
    """
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    csv_data = dataframe.to_csv(index=False)
    blob_client.upload_blob(csv_data, overwrite=True)

def save_to_local_csv(path, dataframe):
    """
    Enregistre un DataFrame en local au format CSV.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    dataframe.to_csv(path, index=False)

def convert_to_delta_and_save_local(input_csv_path, delta_path):
    """
    Convertit un fichier CSV en format Delta et l'enregistre en local.
    """
    os.makedirs(delta_path, exist_ok=True)
    # Lire le fichier CSV
    df = spark.read.format("csv").option("header", "true").load(input_csv_path)
    
    # Activer Column Mapping pour accepter les caractères spéciaux
    spark.sql("SET spark.databricks.delta.properties.defaults.enableColumnMapping=true")
    spark.sql("SET spark.databricks.delta.properties.defaults.columnMapping.mode=name")
    
    # Écrire en Delta
    df.write.format("delta").mode("overwrite").save(delta_path)


def upload_delta_to_blob(container_name, delta_local_path, delta_blob_path):
    """
    Upload un dossier Delta local vers Azure Blob Storage.
    """
    container_client = blob_service_client.get_container_client(container_name)
    for root, dirs, files in os.walk(delta_local_path):
        for file in files:
            file_path = os.path.join(root, file)
            blob_path = os.path.join(delta_blob_path, os.path.relpath(file_path, delta_local_path))
            blob_path = blob_path.replace("\\", "/")  # Ensure correct path format for Azure
            blob_client = container_client.get_blob_client(blob_path)
            with open(file_path, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)

def load_and_check_duplicates(blob_name):
    """
    Charge un fichier CSV depuis le conteneur Bronze, vérifie les doublons,
    et retourne un DataFrame nettoyé.
    """
    print(f"\n--- Chargement du fichier : {blob_name} ---")

    # Charger les données depuis Azure Blob
    df = read_csv_from_blob(BRONZE_CONTAINER, blob_name)

    # Sauvegarder une copie locale
    save_to_local_csv(os.path.join(LOCAL_CSV_BRONZE, blob_name), df)

    # Convertir en Delta et enregistrer localement
    convert_to_delta_and_save_local(
        os.path.join(LOCAL_CSV_BRONZE, blob_name),
        os.path.join(LOCAL_DELTA_BRONZE, os.path.splitext(blob_name)[0])
    )

    # Vérification et nettoyage des doublons
    print("\nVérification des doublons...")
    total_duplicates = df.duplicated().sum()
    print(f"Nombre total de doublons dans {blob_name} : {total_duplicates}")
    df_cleaned = df.drop_duplicates()

    # Sauvegarder une copie nettoyée localement
    save_to_local_csv(os.path.join(LOCAL_CSV_SILVER, blob_name), df_cleaned)

    # Convertir la copie nettoyée en Delta
    convert_to_delta_and_save_local(
        os.path.join(LOCAL_CSV_SILVER, blob_name),
        os.path.join(LOCAL_DELTA_SILVER, os.path.splitext(blob_name)[0])
    )

    return df_cleaned

def main():
    """
    Charge et vérifie les doublons pour tous les fichiers dans la couche Bronze,
    puis les enregistre nettoyés dans la couche Silver.
    """
    for file_name in FILES:
        try:
            # Charger et nettoyer le fichier
            df_cleaned = load_and_check_duplicates(file_name)

            # Enregistrer les données nettoyées (CSV) dans Azure Silver
            save_csv_to_blob(SILVER_CONTAINER, file_name, df_cleaned)
            print(f"\nFichier CSV nettoyé enregistré dans le conteneur Silver : {file_name}")

            # Upload Delta fichiers Bronze et Silver
            upload_delta_to_blob(
                BRONZE_CONTAINER,
                os.path.join(LOCAL_DELTA_BRONZE, os.path.splitext(file_name)[0]),
                os.path.splitext(file_name)[0]
            )
            upload_delta_to_blob(
                SILVER_CONTAINER,
                os.path.join(LOCAL_DELTA_SILVER, os.path.splitext(file_name)[0]),
                os.path.splitext(file_name)[0]
            )
            print(f"\nFichiers Delta uploadés pour {file_name}")
        except Exception as e:
            print(f"Erreur lors du traitement de {file_name} : {e}")

if __name__ == "__main__":
    main()
