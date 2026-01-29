from faker import Faker
import pandas as pd
import random

# Create a Faker object to generate fake data in French
fake = Faker("fr_FR")

# Generate fake stock data
stocks = [{
    "id_produit": i + 1,
    "quantite_stock": random.randint(0, 1000),
    "entrepot": fake.city()
} for i in range(200)]

# Convert the list of stocks into a DataFrame
df_stocks = pd.DataFrame(stocks)

# Save the DataFrame to a Parquet file
df_stocks.to_parquet("dags/data/stocks.parquet",engine="pyarrow",index=False)