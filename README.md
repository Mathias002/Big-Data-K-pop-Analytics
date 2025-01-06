# **Projet K-Pop Data Pipeline**

## **Description Synthétique**

Ce projet vise à construire une pipeline de données robuste et efficace pour analyser l'industrie musicale K-Pop. L'objectif principal est de transformer des données brutes (groupes, idols, et vidéos) en informations exploitables prêtes pour une analyse approfondie et des visualisations interactives.

---

## **Objectifs Clés**

1. **Centralisation des données** : Collecter et organiser les données brutes issues de différentes sources.
2. **Nettoyage et Transformation** : Supprimer les doublons, structurer les colonnes et enrichir les données.
3. **Organisation en Couches** : Adopter une architecture Bronze (brutes), Silver (nettoyées) et Gold (enrichies).
4. **Enrichissement** : Ajouter des métriques clés comme le ratio d'engagement (likes/vues).
5. **Visualisation** : Créer des rapports dynamiques via Power BI pour explorer les tendances et insights.

---

## **Architecture du Pipeline**

1. **Couches Bronze, Silver et Gold** :
   - Bronze : Données brutes chargées depuis des fichiers CSV.
   - Silver : Données nettoyées (doublons supprimés, colonnes standardisées).
   - Gold : Données enrichies avec des métriques calculées (ex : ratio d'engagement).

2. **Outils Utilisés** :
   - **Azure Blob Storage** : Stockage des données.
   - **Delta Lake** : Optimisation et transactions ACID.
   - **Python avec Pandas** : Nettoyage et enrichissement.
   - **API YouTube Data v3** : Récupération des métriques des vidéos.
   - **Power BI** : Visualisation interactive.

3. **Résultats Attendus** :
   - Données structurées prêtes à l'analyse.
   - Visualisation claire des tendances de popularité et de longévité.

---

## **Visualisations Principales**

1. **Analyse Temporelle** :
   - Évolution des vues et ratios d'engagement par année.
   - Nombre de vidéos publiées annuellement.

2. **Comparaison Genres et Groupes** :
   - Performances des Boy Groups vs Girl Groups (vues, likes).
   - Groupes dominants (ex : BTS, Blackpink).

3. **Analyse Géographique** :
   - Répartition des idols par pays (carte interactive).

4. **Longévité des Groupes** :
   - Taux de survie des groupes par année.

---

## **Conclusion**

Ce projet met en œuvre des techniques avancées de traitement des données pour décrypter les dynamiques de l'industrie K-Pop. Il fournit une base solide pour des analyses futures, tout en identifiant les facteurs de popularité et les défis de longévité.

