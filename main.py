from data_retrieval import *
from data_integration import *
from data_augmentation import *
from data_transformation import *

# convert csv to text
def convert_csv_to_txt_and_cleanup(folder_path='data'):
    # check if folder exist
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
        return

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            csv_path = os.path.join(folder_path, filename)
            txt_filename = filename.replace('.csv', '.txt')
            txt_path = os.path.join(folder_path, txt_filename)

            # read csv
            with open(csv_path, 'r', encoding='utf-8') as csv_file:
                content = csv_file.read()

            with open(txt_path, 'w', encoding='utf-8') as txt_file:
                txt_file.write(content)

            # remove csv
            os.remove(csv_path)
            print(f"Converted and deleted: {filename}")

# convert txt to csv
def convert_txt_to_csv_and_cleanup(folder_path='data'):
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
        return

    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            txt_path = os.path.join(folder_path, filename)
            csv_filename = filename.replace('.txt', '.csv')
            csv_path = os.path.join(folder_path, csv_filename)

            with open(txt_path, 'r', encoding='utf-8') as txt_file:
                content = txt_file.read()

            with open(csv_path, 'w', encoding='utf-8') as csv_file:
                csv_file.write(content)

            os.remove(txt_path)
            print(f"Converted and deleted: {filename}")


def main():
    # Convert All txt files to csv
    convert_txt_to_csv_and_cleanup()

    #Step 1: Retrieving the data
    fuelcheck_raw_data = retrieve_fuelcheck_monthly_data()
    print("Raw Data", fuelcheck_raw_data)
    test_retrieve_fuelcheck_monthly_data(fuelcheck_raw_data) 

    #Step 2: Data Cleaning
    fuelcheck_clean_data = data_cleaning(fuelcheck_raw_data)

    #Save the cleaned data to CSV
    convert_cleaned_data_to_csv(fuelcheck_clean_data) 

    # Step 3: Data Augmentation
    # This will only run when additional dataset files will not exist 
    # Make Fuel Table
    fuel_details()

    # Make Geo Mapping Table
    geocode_unique_addresses()

    # Step 4: Data Transformation and Storage
    # fetch fuel data
    fuel = pd.read_csv("data/fuel.csv")
    # fetch geo mapping data
    mapping = pd.read_csv("data/fuel_prices_with_lat_lng.csv")
    # Transform and store data into duckdb
    store_to_duckdb(fuelcheck_clean_data, fuel, mapping)

    # get data to test if data is properly uploaded
    test_fuel_data_queries()

    # convert all text files to csv
    convert_csv_to_txt_and_cleanup()

if __name__ == "__main__":
    main() 