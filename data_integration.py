# Import necessary libraries 
import pandas as pd
from datetime import datetime
import random

def data_cleaning(fuelcheck_raw_data):
    # Drop fully empty rows
    print("Rows before dropping empty rows:", len(fuelcheck_raw_data))
    fuelcheck_raw_data.dropna(how='all', inplace=True)
    print("Rows after dropping empty rows:", len(fuelcheck_raw_data))

    # Drop duplicate rows  
    print("Rows before dropping duplicates:", len(fuelcheck_raw_data))
    fuelcheck_raw_data.drop_duplicates(inplace=True)
    print("Rows after dropping duplicates:", len(fuelcheck_raw_data))

    # Strip leading/trailing spaces in all text columns
    str_columns = fuelcheck_raw_data.select_dtypes(include='object').columns
    print("Stripping whitespace from the following string columns:")
    print(str_columns.tolist())

    fuelcheck_raw_data[str_columns] = fuelcheck_raw_data[str_columns].apply(lambda col: col.str.strip())
    print("Whitespace removed.")

    # PriceUpdatedDate column quality check
    null_count = fuelcheck_raw_data['PriceUpdatedDate'].isnull().sum()
    blank_count = fuelcheck_raw_data['PriceUpdatedDate'].astype(str).str.strip().eq('').sum()
    invalid_values = ['--', '-', 'null', 'n/a', 'na', '0', 0]
    invalid_count = fuelcheck_raw_data['PriceUpdatedDate'].astype(str).str.lower().isin(invalid_values).sum()
    total_bad = null_count + blank_count + invalid_count

    print(f"PriceUpdatedDate column quality check:")
    print(f"Null values: {null_count}")
    print(f"Blank strings: {blank_count}")
    print(f"Common invalid entries: {invalid_count}")
    print(f"Total problematic entries: {total_bad}")

    bad_rows = fuelcheck_raw_data[
        fuelcheck_raw_data['PriceUpdatedDate'].isnull() |
        fuelcheck_raw_data['PriceUpdatedDate'].astype(str).str.strip().isin(['', '--', '-', 'null', 'n/a', 'na', '0'])
    ]
    print(bad_rows[['PriceUpdatedDate', 'source_file']].head(10))

    # Convert Price column to numeric and remove outliers
    if 'Price' in fuelcheck_raw_data.columns:
        print("Filtering out prices outside 50–300 cents range")
        before_rows = len(fuelcheck_raw_data)
        fuelcheck_raw_data = fuelcheck_raw_data[
            (fuelcheck_raw_data['Price'] >= 50) & (fuelcheck_raw_data['Price'] <= 300)
        ]
        after_rows = len(fuelcheck_raw_data)
        print(f"Filtered out {before_rows - after_rows} rows with invalid prices.")
    else:
        print("'Price' column not found in dataset.")

    # Fill missing PriceUpdatedDate using source_file name
    print("Remaining nulls in 'PriceUpdatedDate':", fuelcheck_raw_data['PriceUpdatedDate'].isnull().sum())
    if 'PriceUpdatedDate' in fuelcheck_raw_data.columns and 'source_file' in fuelcheck_raw_data.columns:
        null_count = fuelcheck_raw_data['PriceUpdatedDate'].isnull().sum()
        print(f"Filling {null_count} missing dates using file name")

        fuelcheck_raw_data['PriceUpdatedDate'] = fuelcheck_raw_data.apply(
            lambda row: row['PriceUpdatedDate'] if (pd.notnull(row['PriceUpdatedDate']) and str(row['PriceUpdatedDate']).strip() != '') 
            else infer_date_from_filename(row['source_file']),
            axis=1
        )

        print("Remaining nulls in 'PriceUpdatedDate':", fuelcheck_raw_data['PriceUpdatedDate'].isnull().sum())
    else:
        print("Required columns for date fill not found.")

    # Convert PriceUpdatedDate to datetime format
    if 'PriceUpdatedDate' in fuelcheck_raw_data.columns:
        print("Converting 'PriceUpdatedDate' to datetime")
        fuelcheck_raw_data['PriceUpdatedDate'] = pd.to_datetime(
            fuelcheck_raw_data['PriceUpdatedDate'], errors='coerce'
        )
        print("Nulls in 'PriceUpdatedDate' after conversion:", fuelcheck_raw_data['PriceUpdatedDate'].isnull().sum())
    else:
        print("'PriceUpdatedDate' column not found in dataset.")

    # Drop rows missing any of the key fields
    required_fields = ['ServiceStationName', 'PriceUpdatedDate', 'Price']
    print("Dropping rows with missing values in required fields:", required_fields)
    before_drop = len(fuelcheck_raw_data)

    fuelcheck_raw_data.dropna(
        subset=[col for col in required_fields if col in fuelcheck_raw_data.columns],
        inplace=True
    )

    after_drop = len(fuelcheck_raw_data)
    print(f"Dropped {before_drop - after_drop} rows. Final dataset shape: {fuelcheck_raw_data.shape}")

    # Dataset summary
    print("Shape (rows, columns):", fuelcheck_raw_data.shape)
    print("\nRemaining nulls per column:")
    print(fuelcheck_raw_data.isnull().sum())
    print("\nColumn data types:")
    print(fuelcheck_raw_data.dtypes)
    print("\nPrice column summary:")
    print(fuelcheck_raw_data['Price'].describe())

    if 'FuelCode' in fuelcheck_raw_data.columns:
        print("\nUnique Fuel Types:")
        print(fuelcheck_raw_data['FuelCode'].value_counts())

    print("\nSample rows:")
    print(fuelcheck_raw_data.sample(5, random_state=42))
    print("NULL:", fuelcheck_raw_data.isna().sum())

    return fuelcheck_raw_data

# Extract months from source link to apply default date to null values
def infer_date_from_filename(filename):
    month_map = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }

    if not isinstance(filename, str):
        return pd.NaT
    filename = filename.lower().replace('-', '').replace('_', '')

    for key, month in month_map.items():
        if key in filename:
            if '2024' in filename:
                base_date = datetime(2024, month, 1)
            elif '2025' in filename or '25' in filename:
                base_date = datetime(2025, month, 1)
            else:
                return pd.NaT

            # ADD RANDOM HOURS, MINUTES, SECONDS
            random_hours = random.randint(0, 23)
            random_minutes = random.randint(0, 59)
            random_seconds = random.randint(0, 59)

            final_date = base_date.replace(
                hour=random_hours, 
                minute=random_minutes, 
                second=random_seconds
            )
            return final_date
    return pd.NaT

# Convert cleaned data to CSV
def convert_cleaned_data_to_csv(fuelcheck_raw_data):
    output_file = "cleaned_fuelcheck_data.csv"
    fuelcheck_raw_data = fuelcheck_raw_data.drop(columns=["source_file"])
    print(fuelcheck_raw_data.head())
    print("NULL:", fuelcheck_raw_data.isna().sum())
    fuelcheck_raw_data.to_csv(output_file, index=False)
    print(f"Converted Cleaned data saved to {output_file}")