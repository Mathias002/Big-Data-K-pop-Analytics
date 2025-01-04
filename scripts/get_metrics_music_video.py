import pandas as pd
from azure.storage.blob import BlobServiceClient
from googleapiclient.discovery import build
from io import StringIO
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration de l'API YouTube
API_KEY = os.getenv("API_KEY")
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

# Configuration Azure Blob Storage
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
SILVER_CONTAINER = "silver"

# Chemins locaux
LOCAL_CSV_SILVER = "../data/csv/silver"
LOCAL_DELTA_SILVER = "../data/delta/silver"

# Crée les dossiers locaux si nécessaires
os.makedirs(LOCAL_CSV_SILVER, exist_ok=True)
os.makedirs(LOCAL_DELTA_SILVER, exist_ok=True)

def get_video_metrics(video_id):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
    request = youtube.videos().list(part="statistics", id=video_id)
    response = request.execute()

    stats = response['items'][0]['statistics']
    return {
        'views': int(stats.get('viewCount', 0)),
        'likes': int(stats.get('likeCount', 0)),
        'comments': int(stats.get('commentCount', 0))
    }

def extract_video_id(url):
    if "youtube.com" in url:
        return url.split("v=")[-1]
    elif "youtu.be" in url:
        return url.split("/")[-1]
    return None

def read_csv_from_blob(container_name, blob_name):
    container_client = BLOB_SERVICE_CLIENT.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob().readall()
    return pd.read_csv(StringIO(stream.decode('utf-8')))

def save_csv_to_local(dataframe, local_path):
    dataframe.to_csv(local_path, index=False)
    print(f"Fichier CSV sauvegardé localement : {local_path}")

def save_csv_to_blob(dataframe, container_name, blob_name):
    container_client = BLOB_SERVICE_CLIENT.get_container_client(container_name)
    csv_data = dataframe.to_csv(index=False)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"Fichier enregistré dans Azure Blob Storage : {blob_name}")

def save_delta_local(dataframe, delta_path):
    # Simulation de la sauvegarde au format Delta
    delta_path_csv = delta_path.replace(".delta", ".csv")  # Sauvegarde temporaire au format CSV
    dataframe.to_csv(delta_path_csv, index=False)
    print(f"Delta sauvegardé localement (simulation) : {delta_path_csv}")

def enrich_videos_with_metrics(videos):
    metrics = []
    for url in videos['Video']:
        video_id = extract_video_id(url)
        if video_id:
            try:
                metrics.append(get_video_metrics(video_id))
            except Exception as e:
                print(f"Erreur lors de la récupération des métriques pour {video_id} : {e}")
                metrics.append({'views': None, 'likes': None, 'comments': None})
        else:
            metrics.append({'views': None, 'likes': None, 'comments': None})
    metrics_df = pd.DataFrame(metrics)
    return pd.concat([videos, metrics_df], axis=1)

def main():
    print("Chargement des vidéos depuis Azure Blob Storage...")
    videos = read_csv_from_blob(SILVER_CONTAINER, "kpop_music_videos.csv")

    print("Enrichissement des vidéos avec les métriques YouTube...")
    enriched_videos = enrich_videos_with_metrics(videos)

    print("Sauvegarde du fichier enrichi localement...")
    local_csv_path = os.path.join(LOCAL_CSV_SILVER, "kpop_music_videos_enriched.csv")
    save_csv_to_local(enriched_videos, local_csv_path)

    print("Conversion en Delta et sauvegarde locale...")
    local_delta_path = os.path.join(LOCAL_DELTA_SILVER, "kpop_music_videos_enriched.delta")
    save_delta_local(enriched_videos, local_delta_path)

    print("Enregistrement du fichier enrichi dans Azure Blob Storage...")
    save_csv_to_blob(enriched_videos, SILVER_CONTAINER, "kpop_music_videos_enriched.csv")
    print("Traitement terminé avec succès.")

if __name__ == "__main__":
    main()
