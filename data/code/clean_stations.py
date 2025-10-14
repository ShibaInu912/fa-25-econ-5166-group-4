import pandas as pd
import numpy as np
import re
import os


file_path = 'C:/Users/Luca/Desktop/Course/03_Data_Science_and_Social_Inquiry/04_Final_Project/fa-25-econ-5166-group-4/data'

combined_wide_file = 'temp/combined_stations_long.csv'  
target_tidy_file = 'processed/weather_long_2025-09-28.xlsx'           
output_merged_file = 'weather_merged_stations.csv'
# ---------------------

# --- Step 1: Load Data ---
# Load the combined (still wide) data
df_wide = pd.read_csv(os.path.join(file_path, combined_wide_file))

# Load the target tidy data
df_tidy = pd.read_excel(os.path.join(file_path, target_tidy_file))

print(f"Wide data loaded. Shape: {df_wide.shape}")
print(f"Target tidy data loaded. Shape: {df_tidy.shape}")

# --- Step 2: Extract Station Name and Variable from Source_File ---
# Example: 三義1-2020-AirTemperature-month.csv -> Station: 三義1, Variable: AirTemperature

def lookup_existed_variable(variable):
    dictionary = {
        "AirTemperature": "avg_temp",
        "Precipitation": "precipitation",
        "RelativeHumidity": "avg_humidity",
        "SunshineDuration": "sunshine_hours",
        "PrecipitationDays": "rainy_days"
    }
    updated_variable = dictionary.get(variable, None)
    if(updated_variable == None):
        print(f"{variable} does NOT correspond to an existed variable.")

    return updated_variable

def extract_info(filename):
    #"""Extracts station and variable from the Source_File name."""
    # This regex looks for the station name (anything before the first dash followed by a year),
    # and the variable name (anything between the year and '-month').
    match = re.search(r'(.+?)-(\d{4})-(\D+?)-month\.csv', filename)
    if match:
        station = match.group(1)
        variable = match.group(3)
        variable = lookup_existed_variable(variable)
        return station, variable
    
    return None, None



df_wide[['station_name', 'Variable_Name']] = df_wide['Source_File'].apply(
    lambda x: pd.Series(extract_info(x))
)

# Convert the year column to an integer for a better merge key
df_wide['Year'] = pd.to_numeric(df_wide['Year'], errors='coerce')
df_wide = df_wide.dropna(subset = ['Year']).copy()
df_wide['Year'] = df_wide['Year'].astype(int)

# --- Step 3: Pivot (Melt) the Wide Data to Long Format ---
month_col = [str(i+1) for i in range(12)]

# Use pd.melt to pivot the data
df_long = pd.melt(
    df_wide,
    id_vars= ['station_name', 'Year', 'Variable_Name'], # Columns to keep as identifiers
    value_vars= month_col,                             # Columns to pivot
    var_name='month',                                  # New column for the month names
    value_name='Weather_Value'                                 # New column for the data values
)

print(f"\nPivoted data (df_long) shape: {df_long.shape}")
print(df_long.head())

output_path = os.path.join(file_path, 'temp', "melted_stations.csv")
df_long.to_csv(output_path, index=False, encoding="utf-8-sig")



# --- Step 4: Pivot the new long data WIDER (by variable) ---
# Since your target tidy file has columns for each variable (avg_temp, avg_humidity, etc.),
# the data we just created needs to be spread out again, but using the Variable_Name as the column header.
# df_long_final = df_long.pivot_table(
#     index=['station_name', 'Year', 'month'],
#     columns='Variable_Name',
#     values='Value',
#     aggfunc='first' # Use 'first' since there should only be one value per cell
# ).reset_index()

# # Clean up column names (remove the 'Variable_Name' column header)
# df_long_final.columns.name = None
# df_long_final = df_long_final.rename(columns={'Year': 'year'})

# print(f"\nFinal Tidy Wide-Variable data (df_long_final) shape: {df_long_final.shape}")
# print(df_long_final.head())


# # --- Step 5: Merge with the Target Tidy File ---
# # Merge using common identifier columns: year, month, and station_name.
# df_merged = pd.merge(
#     df_tidy,
#     df_long_final,
#     on=['year', 'month', 'station_name'],
#     how='left'  # Use left merge to keep all records from the target tidy file
# )

# # --- Step 6: Save the Result ---
# df_merged.to_csv(output_merged_file, index=False, encoding="utf-8-sig")

# print("-" * 50)
# print(f"✅ Data merging complete!")
# print(f"Final merged file saved to: {output_merged_file}")
# print(f"Final merged shape: {df_merged.shape}")