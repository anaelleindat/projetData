# Projet : Pipeline ETL pour l'analyse des bornes de vélos en libre-service

## Introduction

Ce projet a été réalisé dans le cadre des travaux pratiques 'Introduction à la data ingénierie'. Il a pour objectif de construire un pipeline ETL permettant d'ingérer, de consolider et d'agréger des données liées à l'utilisation des bornes de vélos en libre-service dans plusieurs villes françaises.

Le pipeline traite les données des villes de Paris, Nantes et Toulouse, en les enrichissant avec des informations issues de l'API des communes françaises. Ces données permettent de réaliser des analyses simples, telles que le nombre de vélos disponibles ou d'emplacements libres dans chaque ville ou station.


## Fonctionnalités principales

Le projet est divisé en trois étapes principales :
1. **Ingestion des données** : Récupération des données temps réel des bornes de vélos et stockage local au format JSON.
2. **Consolidation des données** : Transformation des données ingérées et chargement dans une base de données DuckDB.
3. **Agrégation des données** : Création de tables dimensionnelles et de faits pour faciliter les analyses.
---

## Installation

### Prérequis
- Python 3.8 ou plus récent
- `pip` pour gérer les dépendances Python

### Étapes d'installation (requête à exécuter dans le bash)

1. Clonez le projet :

    ```bash
    git clone https://github.com/anaelleindat/projetData.git
    cd projetData
    ```

2. Créez un environnement virtuel :

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # Sous Windows, utilisez `.venv\Scripts\activate`
    ```

3. Installez les dépendances nécessaires :

    ```bash
    pip install -r requirements.txt
    ```
---

## Exécution

Pour exécuter le pipeline ETL complet, lancez la commande suivante dans le bash :
```bash
python src/main.py
```
---

## Fonctionnement du pipeline
1. **Ingestion des données** :
- Les données des bornes de vélos sont récupérées depuis les API open-source des villes (Paris, Nantes, Toulouse).
- Les données sont sauvegardées localement dans des fichiers JSON structurés par date.

2. **Consolidation des données** :
- Les données ingérées sont transformées et chargées dans des tables DuckDB (`CONSOLIDATE_STATION`, `CONSOLIDATE_STATION_STATEMENT`).
- L'API des communes françaises est utilisée pour enrichir les données des villes dans la table `CONSOLIDATE_CITY`.

3. **Agrégation des données** :
- Les tables de consolidation sont transformées en tables dimensionnelles (`DIM_CITY`, `DIM_STATION`) et en une table de faits (`FACT_STATION_STATEMENT`).

## Structure du projet

Le projet est organisé de la manière suivante :
- **data/** : Contient les fichiers liés aux données, y compris les données brutes et la base DuckDB.
  - `raw_data/` : Fichiers JSON ingérés.
  - `duckdb/` : Base de données DuckDB.
  - `sql_statements/` : Requêtes SQL pour créer les tables.
- **src/** : Contient les scripts Python pour les différentes étapes du pipeline ETL.
  - `data_ingestion.py` : Étape d'ingestion des données.
  - `data_consolidation.py` : Étape de consolidation des données.
  - `data_agregation.py` : Étape d'agrégation des données.
  - `main.py` : Script principal pour exécuter le pipeline.
- **requirements.txt** : Dépendances Python.
- **README.md** : Documentation du projet.
- **LICENSE** : Licence du projet.

## Résultats attendus

Voici les deux exemples de requêtes SQL que vous pouvez exécuter après l'agrégation des données :
1. **Nombre d'emplacements disponibles dans une ville** :
```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'toulouse');
```

2. **Nombre moyen de vélos disponibles par station** :
```sql
-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

Avant de tester les requêtes SQL fournies, vous devez vous connecter à la base de données DuckDB. Voici comment faire :

Placez-vous dans le dossier principal du projet, par exemple :

```bash
cd projetData
```

Lancez DuckDB avec la commande suivante :

```
./duckdb data/duckdb/mobility_analysis.duckdb
```

Vous pouvez ensuite exécuter les requêtes SQL directement dans l'interface DuckDB.


## Licence
Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus d'informations.
