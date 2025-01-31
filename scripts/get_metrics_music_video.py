from googleapiclient.discovery import build
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import os
import pandas as pd
from io import StringIO

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

def get_video_metrics(video_id):
    """
    Récupère les métriques d'une vidéo via l'API YouTube.
    """
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
    """
    Extrait l'ID de la vidéo depuis une URL YouTube.
    """
    if "youtube.com" in url:
        return url.split("v=")[-1]
    elif "youtu.be" in url:
        return url.split("/")[-1]
    return None

def read_csv_from_blob(container_name, blob_name):
    """
    Lit un fichier CSV depuis Azure Blob Storage.
    """
    container_client = BLOB_SERVICE_CLIENT.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob().readall()
    return pd.read_csv(StringIO(stream.decode('utf-8')))

def write_csv_to_blob(df, container_name, blob_name):
    """
    Écrit un DataFrame en tant que CSV dans Azure Blob Storage.
    """
    container_client = BLOB_SERVICE_CLIENT.get_container_client(container_name)
    output = StringIO()
    df.to_csv(output, index=False)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(output.getvalue(), overwrite=True)
    print(f"Fichier enregistré dans Azure Blob Storage : {blob_name}")

def enrich_videos_with_metrics(videos):
    """
    Ajoute les métriques de YouTube au DataFrame des vidéos.
    """
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
    
    # Convertir les métriques en DataFrame
    metrics_df = pd.DataFrame(metrics)
    return pd.concat([videos, metrics_df], axis=1)

def main():
    """
    Pipeline principal pour enrichir les métriques des vidéos.
    """
    # Charger le fichier CSV depuis Azure Blob Storage
    print("Chargement des vidéos depuis Azure Blob Storage...")
    videos = read_csv_from_blob(SILVER_CONTAINER, "kpop_music_videos.csv")
    
    # Enrichir les vidéos avec les métriques
    print("Enrichissement des vidéos avec les métriques YouTube...")
    enriched_videos = enrich_videos_with_metrics(videos)
    
    # Sauvegarder les vidéos enrichies dans Azure Blob Storage
    print("Enregistrement des vidéos enrichies dans Azure Blob Storage...")
    write_csv_to_blob(enriched_videos, SILVER_CONTAINER, "kpop_music_videos_enriched.csv")
    print("Traitement terminé avec succès.")

if __name__ == "__main__":
    main()
