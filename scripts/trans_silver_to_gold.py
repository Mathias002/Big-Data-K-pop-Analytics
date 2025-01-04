import os
import pandas as pd
import numpy as np
import io
from azure.storage.blob import BlobServiceClient
from deltalake.writer import write_deltalake
from dotenv import load_dotenv

load_dotenv()

# Variables de connexion Azure
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SILVER_CONTAINER = "silver"
GOLD_CONTAINER = "gold"

# Répertoires locaux
LOCAL_CSV_GOLD_PATH = "../data/csv/gold"
LOCAL_DELTA_GOLD_PATH = "../data/delta/gold"

os.makedirs(LOCAL_CSV_GOLD_PATH, exist_ok=True)
os.makedirs(LOCAL_DELTA_GOLD_PATH, exist_ok=True)

# Initialisation du service Blob
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def read_csv_from_blob(container_name, blob_name, sep=';'):
    """
    Lit un fichier CSV depuis Azure Blob Storage avec gestion des séparateurs.
    """
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob().readall()
    try:
        return pd.read_csv(io.BytesIO(stream), sep=sep, encoding='utf-8')
    except pd.errors.ParserError:
        print(f"Erreur avec le séparateur '{sep}' pour {blob_name}. Tentative avec ','")
        return pd.read_csv(io.BytesIO(stream), sep=',', encoding='utf-8')


# Fonction pour sauvegarder les fichiers CSV en local et dans Azure
def save_csv_local_and_azure(dataframe, local_path, container_name, blob_name):
    """
    Sauvegarde un fichier CSV en local et le télécharge dans Azure Blob Storage.
    """
    # Sauvegarde locale
    dataframe.to_csv(local_path, index=False, sep=';')
    print(f"Fichier CSV sauvegardé en local : {local_path}")

    # Téléchargement dans Azure
    blob_client = blob_service_client.get_container_client(container_name).get_blob_client(blob_name)
    with open(local_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Fichier CSV uploadé dans Azure : {container_name}/{blob_name}")

# Fonction pour sauvegarder les fichiers Delta en local et dans Azure
def save_delta_local_and_azure(dataframe, local_delta_path, container_name, delta_name):
    """
    Sauvegarde un fichier Delta en local et le télécharge dans Azure Blob Storage.
    """
    write_deltalake(local_delta_path, dataframe, mode="overwrite")
    print(f"Fichier Delta sauvegardé en local : {local_delta_path}")

    for root, _, files in os.walk(local_delta_path):
        for file in files:
            file_path = os.path.join(root, file)
            blob_name = os.path.relpath(file_path, local_delta_path).replace("\\", "/")
            blob_name = f"{delta_name}/{blob_name}"

            blob_client = blob_service_client.get_container_client(container_name).get_blob_client(blob_name)
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"Fichier Delta uploadé dans Azure : {container_name}/{blob_name}")

def load_silver_data():
    idols = read_csv_from_blob(SILVER_CONTAINER, "kpop_idols.csv", sep=',')
    boy_groups = read_csv_from_blob(SILVER_CONTAINER, "kpop_idols_boy_groups.csv", sep=',')
    girl_groups = read_csv_from_blob(SILVER_CONTAINER, "kpop_idols_girl_groups.csv", sep=',')
    videos = read_csv_from_blob(SILVER_CONTAINER, "kpop_music_videos_enriched.csv", sep=',')  # Séparateur spécifique

    print("\n--- Aperçu des données après lecture ---")
    print("Boy Groups :\n", boy_groups.head())
    print("Girl Groups :\n", girl_groups.head())
    
    return idols, boy_groups, girl_groups, videos

def create_dimensions_and_facts(idols, boy_groups, girl_groups, videos):
    # Ajouter des identifiants uniques
    idols['id_idol'] = idols.index + 1
    boy_groups['id_group'] = boy_groups.index + 1
    girl_groups['id_group'] = girl_groups.index + 1
    videos['id_video'] = videos.index + 1

    # Renommer les colonnes pour `boy_groups` et `girl_groups`
    boy_groups = boy_groups.rename(columns={"Name": "nom_du_groupe"})
    girl_groups = girl_groups.rename(columns={"Name": "nom_du_groupe"})

    # Vérifier le renommage
    print("Colonnes de 'boy_groups' après renommage :", boy_groups.columns)
    print("Colonnes de 'girl_groups' après renommage :", girl_groups.columns)

    # Ajouter une colonne de genre
    boy_groups['genre'] = 'Boy Group'
    girl_groups['genre'] = 'Girl Group'

    # Concaténer les groupes
    groups = pd.concat([boy_groups, girl_groups], ignore_index=True)
    print("Colonnes de 'groups' après concaténation :", groups.columns)

    # Renommer les colonnes pour idols
    idols = idols.rename(columns={"Stage Name": "nom_idol"})

    # Renommer les colonnes pour vidéos
    videos = videos.rename(columns={"views": "vues", "likes": "likes", "Video": "lien_video"})
    videos['ratio_engagement'] = np.where(videos['vues'] > 0, videos['likes'] / videos['vues'], np.nan)
    videos['Artist'] = videos['Artist'].str.lower().str.strip()
    groups['nom_du_groupe'] = groups['nom_du_groupe'].str.lower().str.strip()

    # Créer la table des faits
    facts = videos.merge(groups, left_on="Artist", right_on="nom_du_groupe", how="inner")
    facts = facts.dropna(subset=['vues', 'likes', 'ratio_engagement'])
    facts = facts[['id_video', 'nom_du_groupe', 'vues', 'likes', 'ratio_engagement', 'lien_video']]
    
    return idols, groups, videos, facts


# Sauvegarde des fichiers locaux et dans Azure
def save_gold_data(idols, groups, videos, facts):
    # Sauvegarde CSV
    save_csv_local_and_azure(idols, f"{LOCAL_CSV_GOLD_PATH}/idols.csv", GOLD_CONTAINER, "idols.csv")
    save_csv_local_and_azure(groups, f"{LOCAL_CSV_GOLD_PATH}/groups.csv", GOLD_CONTAINER, "groups.csv")
    save_csv_local_and_azure(videos, f"{LOCAL_CSV_GOLD_PATH}/videos.csv", GOLD_CONTAINER, "videos.csv")
    save_csv_local_and_azure(facts, f"{LOCAL_CSV_GOLD_PATH}/facts.csv", GOLD_CONTAINER, "facts.csv")

    # Sauvegarde Delta
    save_delta_local_and_azure(idols, f"{LOCAL_DELTA_GOLD_PATH}/idols_delta", GOLD_CONTAINER, "idols_delta")
    save_delta_local_and_azure(groups, f"{LOCAL_DELTA_GOLD_PATH}/groups_delta", GOLD_CONTAINER, "groups_delta")
    save_delta_local_and_azure(videos, f"{LOCAL_DELTA_GOLD_PATH}/videos_delta", GOLD_CONTAINER, "videos_delta")
    save_delta_local_and_azure(facts, f"{LOCAL_DELTA_GOLD_PATH}/facts_delta", GOLD_CONTAINER, "facts_delta")

    print("Fichiers CSV et Delta enregistrés et uploadés avec succès.")

def main():
    idols, boy_groups, girl_groups, videos = load_silver_data()
    idols, groups, videos, facts = create_dimensions_and_facts(idols, boy_groups, girl_groups, videos)
    save_gold_data(idols, groups, videos, facts)

if __name__ == "__main__":
    main()
