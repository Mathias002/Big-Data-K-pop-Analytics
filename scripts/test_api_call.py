from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")
youtube = build('youtube', 'v3', developerKey=API_KEY)

request = youtube.videos().list(part="statistics", id="EZntLk9bTUw")  # Vidéo YouTube test
response = request.execute()
print(response)
