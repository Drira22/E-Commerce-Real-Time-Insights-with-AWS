import pandas as pd

# Load the CSV file
file_name = 'merged_olist_dataset.csv'

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv(file_name)

print(df.head(n=5))
