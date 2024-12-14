import pandas as pd
import os

# Chemin vers les fichiers bruts (bronze layer)
BRONZE_DIR = "../data/bronze"

# Liste des fichiers à charger
FILES = [
    "kpop_idols.csv",
    "kpop_idols_boy_groups.csv",
    "kpop_idols_girl_groups.csv",
    "kpop_music_videos.csv"
]

def load_and_check_duplicates(file_path):
    """
    Charge un fichier CSV, vérifie les doublons et retourne un DataFrame nettoyé.
    """
    print(f"\n--- Chargement du fichier : {file_path} ---")
    
    # Charger les données
    df = pd.read_csv(file_path)
    
    # Afficher les premières lignes
    print(f"\nAperçu des données de {os.path.basename(file_path)} :")
    print(df.head())
    
    # Informations générales
    print("\nRésumé des données :")
    print(df.info())
    
    # Statistiques descriptives
    print("\nStatistiques descriptives :")
    print(df.describe(include='all'))
    
    # Vérification des doublons
    total_duplicates = df.duplicated().sum()
    print(f"\nNombre total de doublons dans {os.path.basename(file_path)} : {total_duplicates}")
    
    if total_duplicates > 0:
        print("\nExemples de doublons :")
        print(df[df.duplicated()].head())
    
    # Nettoyage des doublons
    df_cleaned = df.drop_duplicates()
    print(f"\nTaille après suppression des doublons : {df_cleaned.shape}")
    
    return df_cleaned

def main():
    """
    Charge et vérifie les doublons pour tous les fichiers dans la couche bronze.
    """
    # Vérifier l'existence du répertoire Bronze
    if not os.path.exists(BRONZE_DIR):
        print(f"Erreur : Le répertoire {BRONZE_DIR} n'existe pas.")
        return

    # Itérer sur tous les fichiers
    for file_name in FILES:
        file_path = os.path.join(BRONZE_DIR, file_name)
        
        # Vérifier si le fichier existe
        if os.path.exists(file_path):
            # Charger et vérifier le fichier
            df_cleaned = load_and_check_duplicates(file_path)
            
            # Exporter les données nettoyées en Silver (temporaire)
            silver_path = file_path.replace("bronze", "silver")
            os.makedirs(os.path.dirname(silver_path), exist_ok=True)
            df_cleaned.to_csv(silver_path, index=False)
            print(f"\nDonnées nettoyées enregistrées dans : {silver_path}")
        else:
            print(f"Fichier non trouvé : {file_path}")

if __name__ == "__main__":
    main()
