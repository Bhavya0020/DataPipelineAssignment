import pandas as pd
import os
import csv
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

def fuel_details(output_csv='data/fuel.csv'):
    # check if output already exists
    if os.path.exists(output_csv):
        print(f"File '{output_csv}' already exists. Skipping fuel processing.")
        return

    # Load fuel sales dataset
    df = pd.read_csv("data/ProductSales - Sheet1.csv")

    # Melt df
    df_long = pd.melt(df, 
                      id_vars=['Month'], 
                      var_name='Product', 
                      value_name='SalesValue')

    # Clean and convert values to numeric
    df_long['SalesValue'] = (
        df_long['SalesValue']
        .astype(str)
        .str.strip()
        .str.replace(',', '', regex=False)
        .replace(
            to_replace=['n.a.', 'n.a', 'NA', 'NaN', '-', '--', ''],
            value=None
        )
    )
    # Convert 'Month' to datetime and filter for 2024 and 2025
    df_long['Month'] = pd.to_datetime(df_long['Month'], format='%b %Y', errors='coerce')
    df_long = df_long[df_long['Month'].dt.year.isin([2024, 2025])]

    # Format Month to MM-YYYY
    df_long['Month'] = df_long['Month'].dt.strftime('%m-%Y')

    # Convert to float
    df_long['SalesValue'] = pd.to_numeric(df_long['SalesValue'], errors='coerce')

    fuel_code_map = {
        'U91': 'Regular (<95 RON) (ML)',
        'P95': 'Premium (95-97 RON) (ML)',
        'P98': 'Premium (98+ RON) (ML)',
        'PDL': 'Diesel oil: premium diesel (ML)',
        'DL': 'Diesel oil: total (ML)',
        'E10': 'Ethanol-blended fuel (ML)',
        'E85': 'E85 (if exists, else custom)',
        'B20': 'B20 (if exists, else custom)',
        'LPG': 'LPG Automotive use (ML)',
    }

    # Filter only relevant products
    valid_products = list(fuel_code_map.values())
    df_filtered = df_long[df_long['Product'].isin(valid_products)].copy()

    # Add fuel code
    reverse_map = {v: k for k, v in fuel_code_map.items()}
    df_filtered['FuelCode'] = df_filtered['Product'].map(reverse_map)

    # Impute missing values
    df_filtered['SalesValue'] = df_filtered.groupby('Product')['SalesValue'].transform(
        lambda x: x.fillna(x.mean())
    )

    # CSV
    os.makedirs(os.path.dirname(output_csv), exist_ok=True) 
    df_filtered.to_csv(output_csv, index=False)
    print(f"Fuel data processed and saved to {output_csv}")

def geocode_unique_addresses(input_csv_file='cleaned_fuelcheck_data.csv', output_csv_file='data/unique_addresses_with_lat_lng.csv'):
    # check if output already exists
    if os.path.exists(output_csv_file):
        print(f"File '{output_csv_file}' already exists. Skipping geocoding.")
        return

    geolocator = Nominatim(user_agent="fuel_address_locator", timeout=10)

    # Sets to hold unique (Address, Suburb) pairs
    unique_entries = set()

    # Extract unique address + suburb pairs from input CSV
    with open(input_csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            address = row['Address'].strip()
            suburb = row['Suburb'].strip()
            unique_entries.add((address, suburb))

    # Function to get lat/lng with retry
    def get_lat_lng(query):
        try:
            location = geolocator.geocode(query)
            if location:
                return location.latitude, location.longitude
            else:
                return None, None
        except (GeocoderTimedOut, GeocoderUnavailable):
            print(f"Retrying for: {query}")
            time.sleep(1)
            return get_lat_lng(query)

    # Open output CSV
    with open(output_csv_file, mode='w', encoding='utf-8', newline='') as output_file:
        fieldnames = ['Address', 'Latitude', 'Longitude']
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        # Loop through unique addresses
        for address, suburb in unique_entries:
            lat, lng = get_lat_lng(address)
            
            if lat is None or lng is None:
                print(f"Address not found: {address}. Trying suburb: {suburb}")
                lat, lng = get_lat_lng(suburb)

            if lat and lng:
                print(f"Location found: {address} -> Lat: {lat}, Lng: {lng}")
            else:
                print(f"Could not find location for Address or Suburb: {address}")

            writer.writerow({'Address': address, 'Latitude': lat, 'Longitude': lng})

            # Respect rate limit
            time.sleep(1)

    print(f"Geocoding complete! Results saved to {output_csv_file}")
