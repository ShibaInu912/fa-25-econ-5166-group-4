import pandas as pd
import glob
import os

file_path = 'C:/Users/Luca/Desktop/Course/03_Data_Science_and_Social_Inquiry/04_Final_Project/02_data'
input_folder = '00_raw/stations'
file_pattern = '*.csv'

output_folder = '01_temp/'
output_filename = '01_combined_stations_long.csv'


# 1. Get a list of all CSV files
all_files = glob.glob(os.path.join(file_path, input_folder, file_pattern))

# Check if any files were found
if not all_files:
    print(f"No files found in: {file_path} matching pattern: {file_pattern}")
    exit()

# List to hold individual DataFrames
list_ = []

print(f"Found {len(all_files)} files. Starting concatenation...")

for filename in all_files:
    try:
        # Read the CSV file
        df = pd.read_csv(filename)

        # 2. Create an identifier column (Crucial for 'long-data' format)
        # filename itself
        source_name = os.path.basename(filename)
        df.insert(0, 'Source_File', source_name)
        # df['Source_File'] = source_name

        # 3. Append the DataFrame to the list
        list_.append(df)
        print(f"  - Successfully processed: {source_name}")

    except Exception as e:
        print(f"  - Error reading file {filename}: {e}")

# 4. Concatenate all DataFrames in the list
if list_:
    combined_df = pd.concat(list_, axis=0, ignore_index=True)
    
    # 5. Save the final combined DataFrame to a new CSV file
    output_path = os.path.join(file_path, output_folder, output_filename)
    combined_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("-" * 30)
    print(f"Combination complete!")
    print(f"Output saved to: {output_path}")
    print(f"Total rows in combined file: {len(combined_df)}")
else:
    print("No data frames were successfully processed to combine.")