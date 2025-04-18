#Retrieve Data
#Import neceassary libraries 
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
DOWNLOAD_DIR = 'fuelcheck_monthly_files'

def retrieve_fuelcheck_monthly_data():
    # Create directory if it doesn't exist
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Created directory: {DOWNLOAD_DIR}")
    else:
        print(f"Directory already exists: {DOWNLOAD_DIR}")
        # return

    print("Retrieving NSW FuelCheck monthly data from Jan 2024 – Mar 2025...")

    base_url = "https://data.nsw.gov.au/data/dataset/fuel-check"
    html_response = requests.get(base_url)
    soup = BeautifulSoup(html_response.text, "html.parser")

    allowed_extensions = ['.xlsx', '.xls', '.csv']
    long_months = ['january', 'february', 'march', 'april', 'may', 'june',
                   'july', 'august', 'september', 'october', 'november', 'december']
    short_months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                    'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    target_patterns = [m + "2024" for m in long_months] + [m + "2024" for m in short_months]
    target_patterns += [m + "2025" for m in long_months[:3]] + [m + "2025" for m in short_months[:3]]
    target_patterns += [m + "25" for m in long_months[:3]] + [m + "25" for m in short_months[:3]]

    download_links = []
    for tag in soup.find_all("a", href=True):
        href = tag['href'].lower()
        if any(href.endswith(ext) for ext in allowed_extensions):
            clean_href = href.replace('-', '').replace('_', '')
            if any(pattern in clean_href for pattern in target_patterns):
                download_links.append(tag['href'])

    print(f"Found {len(download_links)} monthly files.")

    monthly_dataframes = []
    for file_link in download_links:
        filename = file_link.split('/')[-1]  # Extract file name from URL
        local_path = os.path.join(DOWNLOAD_DIR, filename)

        if os.path.exists(local_path):
            print(f"Using cached file: {local_path}")
            if filename.endswith(('.xls', '.xlsx')):
                df_month = pd.read_excel(local_path)
            elif filename.endswith('.csv'):
                df_month = pd.read_csv(local_path)
        else:
            try:
                print(f"Downloading: {file_link}")
                response = requests.get(file_link)
                with open(local_path, 'wb') as f:
                    f.write(response.content)

                if filename.endswith(('.xls', '.xlsx')):
                    df_month = pd.read_excel(local_path)
                elif filename.endswith('.csv'):
                    df_month = pd.read_csv(local_path)
                else:
                    continue
            except Exception as e:
                print(f"Failed to load {file_link}: {e}")
                continue

        df_month['source_file'] = file_link
        monthly_dataframes.append(df_month)

    if monthly_dataframes:
        combined_df = pd.concat(monthly_dataframes, ignore_index=True)
        print(f"Combined dataset shape: {combined_df.shape}")
        return combined_df
    else:
        print("No data loaded.")
        return pd.DataFrame()
    
def test_retrieve_fuelcheck_monthly_data(fuelcheck_raw_data):
    #Total number of rows and columns
    print("\nDataset shape (rows, columns):")
    print(fuelcheck_raw_data.shape)

    #Count missing (null) values in each column
    print("\nNull values per column:")
    print(fuelcheck_raw_data.isnull().sum())

    #First 10 rows of the dataset
    print("\n First 10 rows of the dataset:")
    print(fuelcheck_raw_data.head(10))

    #Check the datatypes of type columns 
    fuelcheck_raw_data.dtypes

    # #Check dates
    # print(fuelcheck_raw_data['PriceUpdatedDate'].astype(str).unique()[:5])
