import json  # Pour gérer la manipulation des fichiers JSON
from datetime import datetime, date  # Pour manipuler les dates
import duckdb  # Pour interagir avec la base de données DuckDB
import pandas as pd  # Pour manipuler les données sous forme de DataFrame

# Variables globales utilisées pour définir la date actuelle et les codes des villes
today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3

# Fonction pour créer les tables consolidées dans la base de données
def create_consolidate_tables():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    # Lecture du fichier contenant les instructions SQL
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        # Exécution de chaque commande SQL séparée par un point-virgule
        for statement in statements.split(";"):
            if statement.strip():  # Ignore les lignes vides
                print(statement)  # Affiche la commande exécutée (utile pour le débogage)
                con.execute(statement)
                

# Fonction pour consolider les données des villes à partir des données des communes
def consolidate_city_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}

    # Chargement des données brutes des communes
    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        data = json.load(fd)

    communes_data_df = pd.json_normalize(data) # Transformation en DataFrame

    # Sélection et renommage des colonnes nécessaires
    city_data_df = communes_data_df [[
        "code",
        "nom",
        "population"
    ]].copy()
    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population": "nb_inhabitants"
    }, inplace=True)

    city_data_df["created_date"] = date.today() # Ajout de la date actuelle

    city_data_df.drop_duplicates(inplace=True)  # Suppression des doublons
    
    # Insertion des données consolidées dans la table CONSOLIDATE_CITY
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    print("City data has been consolidated.");# Confirmation

# Fonction pour récupérer les codes des villes depuis la table CONSOLIDATE_CITY
def fetch_city_codes(con):
    """Récupère les codes de ville depuis CONSOLIDATE_CITY."""
    query = "SELECT ID, NAME FROM CONSOLIDATE_CITY"
    city_codes = pd.DataFrame(con.execute(query).fetchall(), columns=["city_code", "city_name"])
    return city_codes.set_index("city_name").to_dict()["city_code"]

# Fonction pour nettoyer les données et les insérer dans la table CONSOLIDATE_STATION
def clean_and_insert(con, df, city_name):
    """Nettoie les données et insère dans CONSOLIDATE_STATION."""
    df.drop_duplicates(subset=["id", "created_date"], inplace=True)  # Supprime les doublons
    # Supprime les données existantes pour la ville et la date actuelle
    con.execute("DELETE FROM CONSOLIDATE_STATION WHERE CREATED_DATE = ? AND CITY_NAME = ?", [date.today(), city_name])
    # Insère chaque ligne du DataFrame dans la table
    for _, row in df.iterrows():
        con.execute("""
            INSERT OR REPLACE INTO CONSOLIDATE_STATION 
            (ID, CODE, NAME, CITY_NAME, CITY_CODE, ADDRESS, LONGITUDE, LATITUDE, STATUS, CREATED_DATE, CAPACITTY) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.tolist())

# Fonction pour charger et traiter les données de Paris
def load_paris_data(con, city_codes):
    """Charge et traite les données de Paris."""
    try:
        file_path = f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json"
        # Charge les données JSON en mémoire
        with open(file_path, "r") as fd:
            data = json.load(fd)
        df = pd.json_normalize(data)  # Transforme le JSON en DataFrame
        
        # Transformation et ajout des colonnes nécessaires
        df["id"] = df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
        df["code"] = df["stationcode"]
        df["name"] = df["name"]
        df["city_code"] = df["code_insee_commune"]
        df["city_name"] = df["nom_arrondissement_communes"]
        df["address"] = None
        df["longitude"] = df["coordonnees_geo.lon"]
        df["latitude"] = df["coordonnees_geo.lat"]
        df["status"] = df["is_installed"]
        df["created_date"] = date.today()
        df["capacitty"] = df["capacity"]

        # Requête préparée pour l'insertion
        df = df[[
            "id", "code", "name", "city_name", "city_code", "address", 
            "longitude", "latitude", "status", "created_date", "capacitty"
        ]]
        # Nettoyage et insertion dans la base de données
        clean_and_insert(con, df, "Paris")
    except FileNotFoundError:
        print("Fichier Paris introuvable.")  # Message si le fichier est manquant
    except KeyError as e:
        print(f"Erreur de colonne manquante pour Paris: {e}")  # Message si une colonne est absente

# Fonction pour charger et traiter les données de Nantes
def load_nantes_data(con, city_codes):
    """Charge et traite les données de Nantes."""
    try:
        file_path = f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json"
        # Charge les données JSON en mémoire
        with open(file_path, "r") as fd:
            data = json.load(fd)
        df = pd.json_normalize(data)

        # Transformation et ajout des colonnes nécessaires
        df["id"] = df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
        df["code"] = df["number"]
        df["name"] = df["name"]
        df["city_name"] = "Nantes"
        df["city_code"] = city_codes.get("Nantes", NANTES_CITY_CODE)
        df["address"] = df["address"]
        df["longitude"] = df["position.lon"]
        df["latitude"] = df["position.lat"]
        df["status"] = df["status"]
        df["created_date"] = date.today()
        df["capacitty"] = df["bike_stands"]

        # Requête préparée pour l'insertion
        df = df[[
            "id", "code", "name", "city_name", "city_code", "address", 
            "longitude", "latitude", "status", "created_date", "capacitty"
        ]]
        # Nettoyage et insertion dans la base de données
        clean_and_insert(con, df, "Nantes")
    except FileNotFoundError:
        print("Fichier Nantes introuvable.")  # Message si le fichier est manquant
    except KeyError as e:
        print(f"Erreur de colonne manquante pour Nantes: {e}")  # Message si une colonne est absente

# Fonction pour charger et traiter les données de Toulouse
def load_toulouse_data(con, city_codes):
    """Charge et traite les données de Toulouse."""
    try:
        file_path = f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json"
        # Charge les données JSON en mémoire
        with open(file_path, "r") as fd:
            data = json.load(fd)
        df = pd.json_normalize(data)

        # Transformation et ajout des colonnes nécessaires
        df["id"] = df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
        df["code"] = df["number"]
        df["name"] = df["name"]
        df["city_name"] = "Toulouse"
        df["city_code"] = city_codes.get("Toulouse", TOULOUSE_CITY_CODE)
        df["address"] = df["address"]
        df["longitude"] = df["position.lon"]
        df["latitude"] = df["position.lat"]
        df["status"] = df["status"]
        df["created_date"] = date.today()
        df["capacitty"] = df["bike_stands"]

        # Requête préparée pour l'insertion
        df = df[[
            "id", "code", "name", "city_name", "city_code", "address", 
            "longitude", "latitude", "status", "created_date", "capacitty"
        ]]
        # Nettoyage et insertion dans la base de données
        clean_and_insert(con, df, "Toulouse")
    except FileNotFoundError:
        print("Fichier Toulouse introuvable.")  # Message si le fichier est manquant
    except KeyError as e:
        print(f"Erreur de colonne manquante pour Toulouse: {e}")  # Message si une colonne est absente

# Fonction pour consolider les données des stations pour toutes les villes (Paris, Nantes, Toulouse).
def consolidate_station_data():
    # Connexion à la base de données DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Récupérer les codes des villes depuis la table CONSOLIDATE_CITY
    city_codes = fetch_city_codes(con)

    # Chargement des données pour chaque ville
    load_paris_data(con, city_codes)  # Paris
    load_nantes_data(con, city_codes)  # Nantes
    load_toulouse_data(con, city_codes)  # Toulouse

    print("Consolidation des données des stations terminée.")  # Confirmation
    con.close()  # Fermeture de la connexion

# Fonction pour consolider les données des états des stations (disponibilités) pour Paris, Nantes et Toulouse.
def consolidate_station_statement_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    today_date = date.today().strftime("%Y-%m-%d")  # Date actuelle au format "YYYY-MM-DD"

    all_station_statements = []  # Liste pour stocker les DataFrames de chaque ville

    # Consolidate station statement data for Paris
    try:
        with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json", "r") as fd:
            paris_data = json.load(fd)  # Chargement des données JSON
        paris_df = pd.json_normalize(paris_data).copy()  # Transformation en DataFrame
        paris_df["station_id"] = paris_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")  # Ajout d'un identifiant unique
        paris_df["created_date"] = date.today()  # Ajout de la date actuelle
        # Renommage des colonnes pour correspondre au schéma de la table
        paris_station_statements = paris_df[[
            "station_id", "numdocksavailable", "numbikesavailable", "duedate", "created_date"
        ]].rename(columns={
            "numdocksavailable": "bicycle_docks_available",
            "numbikesavailable": "bicycle_available",
            "duedate": "last_statement_date"
        })
        all_station_statements.append(paris_station_statements)  # Ajout des données consolidées
    except FileNotFoundError:
        print("Fichier Paris introuvable.")  # Gestion de l'absence du fichier

    # Consolidate station statement data for Nantes
    try:
        with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json", "r") as fd:
            nantes_data = json.load(fd)
        nantes_df = pd.json_normalize(nantes_data).copy()
        nantes_df["station_id"] = nantes_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
        nantes_df["created_date"] = date.today()
        nantes_station_statements = nantes_df[[
            "station_id", "available_bike_stands", "available_bikes", "last_update", "created_date"
        ]].rename(columns={
            "available_bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date"
        })
        all_station_statements.append(nantes_station_statements)
    except FileNotFoundError:
        print("Fichier Nantes introuvable.")

    # Consolidate station statement data for Toulouse
    try:
        with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json", "r") as fd:
            toulouse_data = json.load(fd)
        toulouse_df = pd.json_normalize(toulouse_data).copy()
        toulouse_df["station_id"] = toulouse_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
        toulouse_df["created_date"] = date.today()
        toulouse_station_statements = toulouse_df[[
            "station_id", "available_bike_stands", "available_bikes", "last_update", "created_date"
        ]].rename(columns={
            "available_bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date"
        })
        all_station_statements.append(toulouse_station_statements)
    except FileNotFoundError:
        print("Fichier Toulouse introuvable.")

    # Combine all statements and handle duplicates
    if all_station_statements:
        combined_df = pd.concat(all_station_statements, ignore_index=True)  # Fusion des DataFrames
        combined_df.drop_duplicates(subset=["station_id", "created_date"], inplace=True)  # Suppression des doublons

        # Création ou mise à jour de la table CONSOLIDATE_STATION_STATEMENT
        con.execute("""
        CREATE TABLE IF NOT EXISTS CONSOLIDATE_STATION_STATEMENT (
            STATION_ID VARCHAR NOT NULL,
            BICYCLE_DOCKS_AVAILABLE INTEGER,
            BICYCLE_AVAILABLE INTEGER,
            LAST_STATEMENT_DATE TIMESTAMP,
            CREATED_DATE DATE,
            PRIMARY KEY (STATION_ID, CREATED_DATE)
        );
        """)

        # Insertion ou mise à jour des enregistrements
        for _, row in combined_df.iterrows():
            con.execute("""
            INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT (
                STATION_ID, BICYCLE_DOCKS_AVAILABLE, BICYCLE_AVAILABLE, LAST_STATEMENT_DATE, CREATED_DATE
            ) VALUES (?, ?, ?, ?, ?)
            """, row.tolist())

        print("Consolidation des données des statements terminée avec succès.")
    else:
        print("Aucune donnée à consolider pour les stations.")

    con.close() # Fermeture de la connexion
