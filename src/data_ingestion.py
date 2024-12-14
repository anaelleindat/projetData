import os  # Pour gérer les dossiers et vérifier leur existence
from datetime import datetime  # Pour générer une date au format actuel
import requests  # Pour effectuer des requêtes HTTP vers des API

# Fonction pour récupérer les données des stations Vélib' en temps réel à Paris
def get_paris_realtime_bicycle_data():
    # URL de l'API pour les données Vélib' de Paris
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    # Envoi de la requête GET pour récupérer les données au format JSON
    response = requests.request("GET", url)
    
    # Sérialisation des données dans un fichier JSON nommé "paris_realtime_bicycle_data.json"
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

# Fonction pour récupérer les données des stations de vélos en temps réel à Nantes
def get_nantes_realtime_bicycle_data():
    # URL de l'API pour les données de Nantes
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json?lang=fr&timezone=Europe%2FBerlin"
    
    # Envoi de la requête GET pour récupérer les données au format JSON
    response = requests.request("GET", url)
    
    # Sérialisation des données dans un fichier JSON nommé "nantes_realtime_bicycle_data.json"
    serialize_data(response.text, "nantes_realtime_bicycle_data.json")

# Fonction pour récupérer les données des stations de vélos en temps réel à Toulouse
def get_toulouse_realtime_bicycle_data():
    # URL de l'API pour les données de Toulouse
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"
    
    # Envoi de la requête GET pour récupérer les données au format JSON
    response = requests.request("GET", url)
    
    # Sérialisation des données dans un fichier JSON nommé "toulouse_realtime_bicycle_data.json"
    serialize_data(response.text, "toulouse_realtime_bicycle_data.json")

# Fonction pour récupérer les données des communes via l'API Open Data
def get_communes_data():
    # URL de l'API pour récupérer les données géographiques des communes françaises
    url = "https://geo.api.gouv.fr/communes?format=json&geometry=centre"
    
    # Envoi de la requête GET pour récupérer les données au format JSON
    response = requests.request("GET", url)
    
    # Sérialisation des données dans un fichier JSON nommé "communes_data.json"
    serialize_data(response.text, "communes_data.json")

# Fonction utilitaire pour sérialiser et sauvegarder les données brutes dans un fichier
def serialize_data(raw_json: str, file_name: str):
    # Génération de la date actuelle au format "YYYY-MM-DD"
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # Vérification si le dossier correspondant à la date du jour existe, sinon le créer
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    # Écriture des données JSON brutes dans le fichier spécifié
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)
