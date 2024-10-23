import os

import pandas as pd

# Specify the directory containing the JSON files
directory = './BitcoinData/'

# Initialize an empty list to store the dataframes
dataframes = []

# Loop through all the files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.json'):  # Process only JSON files
        file_path = os.path.join(directory, filename)

        # Read the JSON file into a DataFrame
        df = pd.read_json(file_path)

        # Append the DataFrame to the list
        dataframes.append(df)

# Concatenate all DataFrames into one
combined_df = pd.concat(dataframes, ignore_index=True)

# Display the combined DataFrame
print(combined_df)
