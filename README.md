# Projet : Pipeline ETL pour l'analyse des bornes de vélos en libre-service

## Introduction

Ce projet a été réalisé dans le cadre des travaux pratiques 'Introduction à la data ingénierie'. Il a pour objectif de construire un pipeline ETL permettant d'ingérer, de consolider et d'agréger des données liées à l'utilisation des bornes de vélos en libre-service dans plusieurs villes françaises.

Le pipeline traite les données des villes de Paris, Nantes et Toulouse, en les enrichissant avec des informations issues de l'API des communes françaises. Ces données permettent de réaliser des analyses simples, telles que le nombre de vélos disponibles ou d'emplacements libres dans chaque ville ou station.
---

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
- L'API des communes françaises est utilisée pour enrichir les données des villes dans la table CONSOLIDATE_CITY.

3. **Agrégation des données** :
- Les tables de consolidation sont transformées en tables dimensionnelles (`DIM_CITY`, `DIM_STATION`) et en une table de faits (`FACT_STATION_STATEMENT`).

## Structure du projet
projetData/
│
├── data/                             # Données locales
│   ├── raw_data/                     # Fichiers JSON ingérés
│   └── duckdb/                       # Base de données DuckDB
│   └── sql_statements/                # Requêtes SQL pour créer les tables
│       ├── create_consolidate_tables.sql
│       └── create_agregate_tables.sql
│
├── src/                              # Code source
│   ├── data_ingestion.py             # Étape d'ingestion des données
│   ├── data_consolidation.py         # Étape de consolidation des données
│   ├── data_agregation.py            # Étape d'agrégation des données
│   └── main.py                       # Fichier principal pour exécuter le pipeline
│
├── requirements.txt                  # Dépendances Python
├── README.md                         # Documentation
└── LICENSE                           # Licence du projet

Résultats attendus
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

On obtiendra : 
───────────┬─────────────────────────────┐
│   NAME    │ SUM_BICYCLE_DOCKS_AVAILABLE │
│  varchar  │           int128            │
├───────────┼─────────────────────────────┤
│ Toulouse  │                        4130 │
│ Nantes    │                        1458 │
│ Paris     │                       18512 │
│ Vincennes │                         193 │
└───────────┴─────────────────────────────┘


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

On obtiendra : 
─────────────────────────────────────┬─────────┬─────────┬────────────────────┐
│                NAME                 │  CODE   │ ADDRESS │ avg_dock_available │
│               varchar               │ varchar │ varchar │       double       │
├─────────────────────────────────────┼─────────┼─────────┼────────────────────┤
│ Vigée-Lebrun - Anselme Payen        │ 15112   │         │                3.0 │
│ Chazelles - Courcelles              │ 17025   │         │                2.5 │
│ Italie - Tolbiac                    │ 13030   │         │                4.5 │
│ Moulin de Pierre - Abbé Grégoire    │ 21326   │         │                6.5 │
│ Ramponeau - Belleville              │ 20143   │         │               12.5 │
│ Octave Feuillet - Albéric Magnard   │ 16110   │         │                3.0 │
│ Saint-Cloud - Hippodrome            │ 16138   │         │                4.0 │
│ Bellefond - Poissonnière            │ 9001    │         │                3.5 │
│ Square Louis XVI                    │ 8016    │         │               17.0 │
│ Raspail Barbès                      │ 31028   │         │                3.5 │
│ De Toqueville - Terrasse            │ 17048   │         │                3.0 │
│ Michel-Ange - Parent de Rosan       │ 16118   │         │                7.0 │
│ Tour d'Auvergne - Rodier            │ 9008    │         │                2.5 │
│ Verger - Colonel Guichard           │ 21328   │         │                6.0 │
│ Marceau - Chaillot                  │ 8048    │         │                5.5 │
│ Liard - Amiral Mouchez              │ 14013   │         │                6.0 │
│ Pereire - Ternes                    │ 17040   │         │                3.5 │
│ Douai - Bruxelles                   │ 9038    │         │               14.5 │
│ Youri Gagarine - Karl Marx          │ 48003   │         │                4.5 │
│ Quai de l'Horloge - Pont Neuf       │ 1001    │         │               11.0 │
│         ·                           │  ·      │    ·    │                 ·  │
│         ·                           │  ·      │    ·    │                 ·  │
│         ·                           │  ·      │    ·    │                 ·  │
│ Boétie - Ponthieu                   │ 8050    │         │               19.5 │
│ Caire - Dussoubs                    │ 2017    │         │               26.0 │
│ Croulebarde - Corvisart             │ 13101   │         │                9.5 │
│ Artois - Berri                      │ 8103    │         │                9.5 │
│ Romainville - Vaillant-Couturier    │ 31024   │         │                2.5 │
│ Regnault - Patay                    │ 13118   │         │               42.5 │
│ Cambrai - Benjamin Constant         │ 19033   │         │                4.5 │
│ Jourdan - Stade Charléty            │ 14014   │         │               10.5 │
│ André Karman - République           │ 33006   │         │                8.0 │
│ Place Nelson Mandela                │ 25006   │         │                5.0 │
│ Hôpital Mondor                      │ 40001   │         │                0.0 │
│ Benjamin Godard - Victor Hugo       │ 16107   │         │                4.0 │
│ Toudouze - Clauzel                  │ 9020    │         │                5.0 │
│ Bas du Mont-Mesly                   │ 40011   │         │                0.0 │
│ Charonne - Robert et Sonia Delaunay │ 11104   │         │                0.5 │
│ Saint-Romain - Cherche-Midi         │ 6108    │         │               10.0 │
│ Jules Guesde - Pont du Port à l'A…  │ 44017   │         │               14.0 │
│ Beaux-Arts - Bonaparte              │ 6021    │         │               16.0 │
│ Le Brix et Mesmin - Jourdan         │ 14108   │         │                1.5 │
│ Thouin - Cardinal Lemoine           │ 5016    │         │                9.5 │
├─────────────────────────────────────┴─────────┴─────────┴────────────────────┤
│ 2005 rows (40 shown)                                               4 columns │
└──────────────────────────────────────────────────────────────────────────────┘

## Licence
Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus d'informations.
