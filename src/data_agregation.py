import duckdb  # Importation de la bibliothèque DuckDB pour gérer les bases de données.

# Fonction pour exécuter les instructions SQL nécessaires à la création des tables agrégées
def create_agregate_tables():
    # Connexion à la base de données DuckDB en mode lecture-écriture
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    # Ouverture du fichier SQL contenant les instructions pour créer les tables agrégées
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        # Parcours et exécution des instructions SQL séparées par un point-virgule
        for statement in statements.split(";"):
            print(statement)  # Affiche l'instruction SQL exécutée (utile pour le suivi)
            con.execute(statement)  # Exécute l'instruction SQL

# Fonction pour agréger les données dans la table DIM_STATION
def agregate_dim_station():
    # Connexion à la base de données DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Instruction SQL pour insérer ou mettre à jour les données dans DIM_STATION
    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,  -- Identifiant de la station
        CODE,  -- Code de la station
        NAME,  -- Nom de la station
        ADDRESS,  -- Adresse de la station
        LONGITUDE,  -- Longitude de la station
        LATITUDE,  -- Latitude de la station
        STATUS,  -- Statut de la station
        CAPACITTY  -- Capacité de la station (Attention à la possible faute de frappe ici)
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);  -- Prend les données les plus récentes
    """

    con.execute(sql_statement)  # Exécute l'instruction SQL

# Fonction pour agréger les données dans la table DIM_CITY
def agregate_dim_city():
    # Connexion à la base de données DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Instruction SQL pour insérer ou mettre à jour les données dans DIM_CITY
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """
    con.execute(sql_statement)  # Exécute l'instruction SQL

# Fonction pour agréger les données dans la table FACT_STATION_STATEMENT
def agregate_fact_station_statements():
    # Connexion à la base de données DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Instruction SQL pour insérer ou mettre à jour les données dans FACT_STATION_STATEMENT
    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    SELECT STATION_ID, cc.ID as CITY_ID, BICYCLE_DOCKS_AVAILABLE, BICYCLE_AVAILABLE, LAST_STATEMENT_DATE, current_date as CREATED_DATE
    FROM CONSOLIDATE_STATION_STATEMENT
    JOIN CONSOLIDATE_STATION ON CONSOLIDATE_STATION.ID = CONSOLIDATE_STATION_STATEMENT.STATION_ID
    LEFT JOIN CONSOLIDATE_CITY as cc ON cc.ID = CONSOLIDATE_STATION.CITY_CODE
    WHERE CITY_CODE != 0 
        AND CONSOLIDATE_STATION_STATEMENT.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION_STATEMENT)
        AND CONSOLIDATE_STATION.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        AND cc.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """
    try:
        con.execute(sql_statement)  # Exécute l'instruction SQL
        print("Agrégation des données dans FACT_STATION_STATEMENT terminée.")  # Confirmation
    except Exception as e:  # Gestion des erreurs
        print(f"Erreur lors de l'agrégation : {e}")  # Affiche l'erreur si elle survient
    finally:
        con.close()  # Ferme la connexion à la base de données
