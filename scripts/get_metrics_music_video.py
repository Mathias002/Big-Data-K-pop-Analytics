from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

# Votre clé API YouTube
API_KEY = os.getenv("API_KEY")
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

def get_video_metrics(video_id):
    """
    Récupère les métriques d'une vidéo via l'API YouTube.
    """
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
    
    # Récupérer les statistiques de la vidéo
    request = youtube.videos().list(
        part="statistics",
        id=video_id
    )
    response = request.execute()
    
    stats = response['items'][0]['statistics']
    return {
        'views': int(stats.get('viewCount', 0)),
        'likes': int(stats.get('likeCount', 0)),
        'comments': int(stats.get('commentCount', 0))
    }

def extract_video_id(url):

    print(f"Traitement de la vidéo : {url}")
    """
    Extrait l'ID de la vidéo depuis une URL YouTube.
    """
    if "youtube.com" in url:
        return url.split("v=")[-1]
    elif "youtu.be" in url:
        return url.split("/")[-1]
    return None

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


# Exemple d'utilisation
videos = pd.read_csv("../data/silver/kpop_music_videos.csv")
videos = enrich_videos_with_metrics(videos)
videos.to_csv("../data/silver/kpop_music_videos_enriched.csv", index=False)
