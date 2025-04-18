import pandas as pd

df = pd.read_csv("cleaned_fuelcheck_data.csv")

new_df = df.head(100)

new_df.to_csv("sample.csv")