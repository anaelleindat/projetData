# Importation des fonctions nécessaires pour chaque étape du pipeline :
# - data_agregation : pour l'agrégation des données consolidées.
# - data_consolidation : pour structurer et préparer les données.
# - data_ingestion : pour collecter les données brutes.
from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data
)
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,  # Import de la fonction pour Nantes
    get_toulouse_realtime_bicycle_data,  # Import de la fonction pour Toulouse
    get_communes_data  # Import de la fonction pour ingestion des données des communes
)

# Définition de la fonction principale qui orchestre le pipeline
def main():
    print("Process start.")  # Indication du début du pipeline.
    
    # Étape 1 : ingestion des données
    print("Data ingestion started.")  # Indique le début de l'ingestion des données.
    get_paris_realtime_bicycle_data()  # Appel de la fonction pour ingérer les données de Paris.
    print("Paris data ingestion ended.")  # Confirmation de la fin de l'ingestion pour Paris.
    
    get_nantes_realtime_bicycle_data()  # Appel de la fonction pour ingérer les données de Nantes.
    print("Nantes data ingestion ended.")  # Confirmation de la fin de l'ingestion pour Nantes.
    
    get_toulouse_realtime_bicycle_data()  # Appel de la fonction pour ingérer les données de Toulouse.
    print("Toulouse data ingestion ended.")  # Confirmation de la fin de l'ingestion pour Toulouse.
    
    get_communes_data()  # Appel de la fonction pour ingérer les données des communes.
    print("Communes data ingestion ended.")  # Confirmation de la fin de l'ingestion des données des communes.
    
    # Étape 2 : consolidation des données
    print("Consolidation data started.")  # Indique le début de la consolidation des données.
    create_consolidate_tables()  # Création des tables consolidées dans la base de données.
    consolidate_city_data()  # Consolidation des données des villes.
    consolidate_station_data()  # Consolidation des données des stations.
    consolidate_station_statement_data()  # Consolidation des états des stations.
    print("Consolidation data ended.")  # Confirmation de la fin de la consolidation des données.

    # Étape 3 : agrégation des données
    print("Agregate data started.")  # Indique le début de l'agrégation des données.
    create_agregate_tables()  # Création des tables agrégées.
    agregate_dim_city()  # Agrégation des dimensions des villes.
    agregate_dim_station()  # Agrégation des dimensions des stations.
    agregate_fact_station_statements()  # Agrégation des données factuelles sur les stations.
    print("Agregate data ended.")  # Confirmation de la fin de l'agrégation des données.

# Point d'entrée principal du script. 
# Cette condition garantit que la fonction `main` ne sera appelée que si le fichier est exécuté directement,
# et non lorsqu'il est importé comme module.
if __name__ == "__main__":
    main()
