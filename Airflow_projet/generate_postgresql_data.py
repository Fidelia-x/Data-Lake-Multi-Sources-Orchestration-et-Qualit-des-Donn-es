from faker import Faker
import pandas as pd
from sqlalchemy import create_engine
import random

fake = Faker("fr_FR")

# Connexion à la base de données PostgreSQL
engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/airflow")

# Génération des clients
clients = [{
    "id_client": i + 1,
    "nom": fake.name(),
    "email": fake.email(),
    "pays": fake.country(),
    "date_creation": fake.date_between(start_date="-2y", end_date="today")
} for i in range(500)]

# Conversion de la liste de clients en DataFrame
df_clients = pd.DataFrame(clients)
# Enregistrement des clients dans la base de données
df_clients.to_sql("clients", engine, if_exists="replace", index=False)

# Génération des ventes
ventes = [{
    "id_vente": i + 1,
    "id_client": random.randint(1, 500),  # Associe chaque vente à un client aléatoire
    "quantite": random.randint(1, 10),
    "prix": round(random.uniform(50, 3000), 2),
    "date_vente": fake.date_between(start_date="-1y", end_date="today")
} for i in range(1500)]

# Conversion de la liste de ventes en DataFrame
df_ventes = pd.DataFrame(ventes)
# Enregistrement des ventes dans la base de données
df_ventes.to_sql("ventes", engine, if_exists="replace", index=False)