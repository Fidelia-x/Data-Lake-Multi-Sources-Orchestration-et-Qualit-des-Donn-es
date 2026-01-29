from fastapi import FastAPI
from faker import Faker
import random

app = FastAPI()
fake = Faker("fr_FR")

@app.get("/ventes")
def get_ventes(n: int = 100):
    return [{
        "id_vente": i + 1,
        "quantite": random.randint(1, 10),
        "prix": round(random.uniform(100, 3000), 2),
        "date": fake.date_between(start_date="-6m", end_date="today")
    } for i in range(n)]


    