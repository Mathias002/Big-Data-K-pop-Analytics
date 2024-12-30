import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os

# Configurations Azure
ACCOUNT_NAME = "kpopdatasets"  # Remplacez par le nom de votre compte Azure Storage
ACCOUNT_KEY = "PRZvJTJMc3A4ozn1e1wYwR/AkwnHWa5dOPlad1LY0DwQJZbp3HfAFzf/pE8pmee5Y5g9sQGhhlL1+ASt/MuNBg=="     # Remplacez par votre clé d'accès
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
            print(f"\nDonnées nettoyées enregistrées dans le conteneur Silver : {file_name}")
        except Exception as e:
            print(f"Erreur lors du traitement de {file_name} : {e}")

if __name__ == "__main__":
    main()
