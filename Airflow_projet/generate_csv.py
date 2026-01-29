from faker import Faker
import pandas as pd
import random

# Create a Faker object to generate fake data in French
fake = Faker("fr_FR")

# Generate fake products
produits = [{
    "id_produit": i + 1,
    "nom_produit": fake.word(),
    "categorie": fake.word(),
    "prix_unitaire": round(random.uniform(20, 5000), 2)
} for i in range(200)]

# Convert the list of products into a DataFrame
df_produits = pd.DataFrame(produits)

# Save the DataFrame to a CSV file
df_produits.to_csv('dags/data/produits.csv', index=False)