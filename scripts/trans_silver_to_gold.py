import pandas as pd
import os
import numpy as np

# Répertoires Silver et Gold
SILVER_DIR = "../data/silver"
GOLD_DIR = "../data/gold"

# Charger les fichiers nettoyés
def load_silver_data():
    idols = pd.read_csv(os.path.join(SILVER_DIR, "kpop_idols.csv"))
    boy_groups = pd.read_csv(os.path.join(SILVER_DIR, "kpop_idols_boy_groups.csv"))
    girl_groups = pd.read_csv(os.path.join(SILVER_DIR, "kpop_idols_girl_groups.csv"))
    videos = pd.read_csv(os.path.join(SILVER_DIR, "kpop_music_videos_enriched.csv"))  # Fichier enrichi
    return idols, boy_groups, girl_groups, videos

def create_dimensions_and_facts(idols, boy_groups, girl_groups, videos):
    """
    Crée les tables dimensionnelles et la table des faits avec les données enrichies.
    """
    # Ajouter une colonne 'id' unique pour chaque DataFrame
    idols['id_idol'] = idols.index + 1
    boy_groups['id_group'] = boy_groups.index + 1
    girl_groups['id_group'] = girl_groups.index + 1
    videos['id_video'] = videos.index + 1

    # Dimension Groupes
    boy_groups['genre'] = 'Boy Group'
    girl_groups['genre'] = 'Girl Group'
    
    # Renommer la colonne 'Name' en 'nom_du_groupe' pour uniformité
    boy_groups = boy_groups.rename(columns={"Name": "nom_du_groupe"})
    girl_groups = girl_groups.rename(columns={"Name": "nom_du_groupe"})

    # Concaténation des groupes
    groups = pd.concat([boy_groups, girl_groups], ignore_index=True)
    
    # Dimension Idols
    idols = idols.rename(columns={"Stage Name": "nom_idol"})

    # Dimension Vidéos (avec enrichissement)
    videos = videos.rename(columns={"views": "vues", "likes": "likes", "Video": "lien_video"})

    # Calcul du ratio d'engagement et gestion des erreurs
    videos['ratio_engagement'] = np.where(videos['vues'] > 0, videos['likes'] / videos['vues'], np.nan)

    # Uniformiser les noms dans Artist et nom_du_groupe
    videos['Artist'] = videos['Artist'].str.lower().str.strip()
    groups['nom_du_groupe'] = groups['nom_du_groupe'].str.lower().str.strip()

    # Table des Faits
    facts = videos.merge(groups, left_on="Artist", right_on="nom_du_groupe", how="inner")
    
    # Supprimer les lignes avec des valeurs manquantes dans les colonnes critiques
    facts = facts.dropna(subset=['vues', 'likes', 'ratio_engagement'])

    # Ajoutez explicitement les colonnes nécessaires, y compris les ID
    facts = facts[['id_video', 'nom_du_groupe', 'vues', 'likes', 'ratio_engagement', 'lien_video']]
    
    return idols, groups, videos, facts




def save_gold_data(idols, groups, videos, facts):
    """
    Sauvegarde les tables en Gold Layer
    """
    os.makedirs(GOLD_DIR, exist_ok=True)

    videos['vues'] = videos['vues'].astype(str).str.replace('.', ',', regex=False)
    videos['likes'] = videos['likes'].astype(str).str.replace('.', ',', regex=False)
    videos['comments'] = videos['comments'].astype(str).str.replace('.', ',', regex=False)
    videos['ratio_engagement'] = videos['ratio_engagement'].astype(str).str.replace('.', ',', regex=False)

    facts['vues'] = facts['vues'].astype(str).str.replace('.', ',', regex=False)
    facts['likes'] = facts['likes'].astype(str).str.replace('.', ',', regex=False)
    facts['ratio_engagement'] = facts['ratio_engagement'].astype(str).str.replace('.', ',', regex=False)

    # Sauvegarder les fichiers avec un séparateur ";"
    idols.to_csv("../data/gold/dimension_idols.csv", sep=';', index=False)
    groups.to_csv("../data/gold/dimension_groupes.csv", sep=';', index=False)
    videos.to_csv("../data/gold/dimension_videos.csv", sep=';', index=False)
    facts.to_csv("../data/gold/table_faits.csv", sep=';', index=False)

    print("\nTables Gold enregistrées avec succès !")

def main():
    # Charger les données Silver
    idols, boy_groups, girl_groups, videos = load_silver_data()

    # Créer les tables dimensionnelles et la table des faits
    idols, groups, videos, facts = create_dimensions_and_facts(idols, boy_groups, girl_groups, videos)

    # Sauvegarder les données Gold
    save_gold_data(idols, groups, videos, facts)

if __name__ == "__main__":
    main()
