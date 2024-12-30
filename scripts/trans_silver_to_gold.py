import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
import io

load_dotenv()

# Variables de connexion Azure
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SILVER_CONTAINER = "silver"
GOLD_CONTAINER = "gold"

# Initialisation du service Blob
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

# Fonction pour lire les fichiers depuis Azure Blob Storage
def read_csv_from_blob(container_name, blob_name):
    """
    Lit un fichier CSV depuis Azure Blob Storage.
    """
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob().readall()
    return pd.read_csv(io.BytesIO(stream))


# Fonction pour sauvegarder les fichiers dans Azure Blob Storage
def save_csv_to_blob(container_name, blob_name, dataframe, sep=';'):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    csv_data = dataframe.to_csv(index=False, sep=sep).encode('utf-8')
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"Fichier enregistré dans le conteneur {container_name} sous {blob_name}")

# Charger les fichiers nettoyés depuis Silver
def load_silver_data():
    idols = read_csv_from_blob(SILVER_CONTAINER, "kpop_idols.csv")
    boy_groups = read_csv_from_blob(SILVER_CONTAINER, "kpop_idols_boy_groups.csv")
    girl_groups = read_csv_from_blob(SILVER_CONTAINER, "kpop_idols_girl_groups.csv")
    videos = read_csv_from_blob(SILVER_CONTAINER, "kpop_music_videos_enriched.csv")
    return idols, boy_groups, girl_groups, videos

# Création des dimensions et de la table des faits
def create_dimensions_and_facts(idols, boy_groups, girl_groups, videos):
    idols['id_idol'] = idols.index + 1
    boy_groups['id_group'] = boy_groups.index + 1
    girl_groups['id_group'] = girl_groups.index + 1
    videos['id_video'] = videos.index + 1

    # Dimension Groupes
    boy_groups['genre'] = 'Boy Group'
    girl_groups['genre'] = 'Girl Group'
    boy_groups = boy_groups.rename(columns={"Name": "nom_du_groupe"})
    girl_groups = girl_groups.rename(columns={"Name": "nom_du_groupe"})
    groups = pd.concat([boy_groups, girl_groups], ignore_index=True)

    # Dimension Idols
    idols = idols.rename(columns={"Stage Name": "nom_idol"})

    # Dimension Vidéos
    videos = videos.rename(columns={"views": "vues", "likes": "likes", "Video": "lien_video"})
    videos['ratio_engagement'] = np.where(videos['vues'] > 0, videos['likes'] / videos['vues'], np.nan)
    videos['Artist'] = videos['Artist'].str.lower().str.strip()
    groups['nom_du_groupe'] = groups['nom_du_groupe'].str.lower().str.strip()

    # Table des Faits
    facts = videos.merge(groups, left_on="Artist", right_on="nom_du_groupe", how="inner")
    facts = facts.dropna(subset=['vues', 'likes', 'ratio_engagement'])
    facts = facts[['id_video', 'nom_du_groupe', 'vues', 'likes', 'ratio_engagement', 'lien_video']]
    return idols, groups, videos, facts

# Sauvegarder les fichiers dans le conteneur Gold
def save_gold_data(idols, groups, videos, facts):
    videos['vues'] = videos['vues'].astype(str).str.replace('.', ',', regex=False)
    videos['likes'] = videos['likes'].astype(str).str.replace('.', ',', regex=False)
    videos['comments'] = videos['comments'].astype(str).str.replace('.', ',', regex=False)
    videos['ratio_engagement'] = videos['ratio_engagement'].astype(str).str.replace('.', ',', regex=False)
    facts['vues'] = facts['vues'].astype(str).str.replace('.', ',', regex=False)
    facts['likes'] = facts['likes'].astype(str).str.replace('.', ',', regex=False)
    facts['ratio_engagement'] = facts['ratio_engagement'].astype(str).str.replace('.', ',', regex=False)

    save_csv_to_blob(GOLD_CONTAINER, "dimension_idols.csv", idols)
    save_csv_to_blob(GOLD_CONTAINER, "dimension_groupes.csv", groups)
    save_csv_to_blob(GOLD_CONTAINER, "dimension_videos.csv", videos)
    save_csv_to_blob(GOLD_CONTAINER, "table_faits.csv", facts)

    print("\nTables Gold enregistrées avec succès dans Azure Blob Storage !")

def main():
    # Charger les données Silver
    idols, boy_groups, girl_groups, videos = load_silver_data()

    # Créer les tables dimensionnelles et la table des faits
    idols, groups, videos, facts = create_dimensions_and_facts(idols, boy_groups, girl_groups, videos)

    # Sauvegarder les données Gold
    save_gold_data(idols, groups, videos, facts)

if __name__ == "__main__":
    main()
