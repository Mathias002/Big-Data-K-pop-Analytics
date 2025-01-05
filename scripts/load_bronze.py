import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

load_dotenv()

# Initialisation de Spark avec Delta
builder = SparkSession.builder \
    .appName("CSV to Delta Conversion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Configure Spark avec Delta
spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Configurations Azure
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")  
ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

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

def convert_csv_to_delta(dataframe, delta_path):
    """
    Convertit un DataFrame Pandas en Delta et l'enregistre localement avec Column Mapping activé.
    """
    # Convertir le DataFrame Pandas en DataFrame Spark
    spark_df = spark.createDataFrame(dataframe)

    # Activer Column Mapping pour accepter les caractères spéciaux
    spark.sql("SET spark.databricks.delta.properties.defaults.enableColumnMapping=true")
    spark.sql("SET spark.databricks.delta.properties.defaults.columnMapping.mode=name")

    # Sauvegarder au format Delta avec Column Mapping
    spark_df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"Fichier Delta créé localement avec Column Mapping : {delta_path}")


def upload_delta_to_blob(delta_local_path, container_name, delta_blob_path):
    """
    Upload un dossier Delta local vers Azure Blob Storage.
    """
    container_client = blob_service_client.get_container_client(container_name)
    for root, dirs, files in os.walk(delta_local_path):
        for file in files:
            file_path = os.path.join(root, file)
            blob_path = os.path.join(delta_blob_path, os.path.relpath(file_path, delta_local_path))
            blob_path = blob_path.replace("\\", "/")  # Assure un format de chemin compatible Azure
            blob_client = container_client.get_blob_client(blob_path)
            with open(file_path, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)
    print(f"Fichier Delta uploadé dans Azure : {delta_blob_path}")


def load_and_check_duplicates(blob_name):
    """
    Charge un fichier CSV depuis le conteneur Bronze, vérifie les doublons,
    et retourne un DataFrame nettoyé.
    """
    print(f"\n--- Chargement du fichier : {blob_name} ---")

    # Charger les données depuis Azure Blob
    df = read_csv_from_blob(BRONZE_CONTAINER, blob_name)

    # Afficher les premières lignes
    print(f"\nAperçu des données de {blob_name} :")
    print(df.head())

    # Informations générales
    print("\nRésumé des données :")
    print(df.info())

    # Statistiques descriptives
    print("\nStatistiques descriptives :")
    print(df.describe(include='all'))

    # Vérification des doublons
    total_duplicates = df.duplicated().sum()
    print(f"\nNombre total de doublons dans {blob_name} : {total_duplicates}")

    if total_duplicates > 0:
        print("\nExemples de doublons :")
        print(df[df.duplicated()].head())

    # Nettoyage des doublons
    df_cleaned = df.drop_duplicates()
    print(f"\nTaille après suppression des doublons : {df_cleaned.shape}")

    return df_cleaned

def main():
    """
    Charge et vérifie les doublons pour tous les fichiers dans la couche Bronze,
    puis les enregistre nettoyés dans la couche Silver.
    """
    # Itérer sur tous les fichiers
    for file_name in FILES:
        try:
            # Charger et vérifier le fichier
            df_cleaned = load_and_check_duplicates(file_name)

            # Enregistrer les données nettoyées dans le conteneur Silver
            save_csv_to_blob(SILVER_CONTAINER, file_name, df_cleaned)

            # Chemin local temporaire pour Delta
            temp_delta_path = f"/tmp/{file_name.replace('.csv', '_delta')}"

            # Convertir en Delta et uploader
            convert_csv_to_delta(df_cleaned, temp_delta_path)
            upload_delta_to_blob(temp_delta_path, SILVER_CONTAINER, file_name.replace(".csv", "_delta"))

            print(f"\nDonnées nettoyées enregistrées dans le conteneur Silver : {file_name}")
        except Exception as e:
            print(f"Erreur lors du traitement de {file_name} : {e}")

if __name__ == "__main__":
    main()