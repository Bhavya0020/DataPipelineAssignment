import duckdb
import pandas as pd
import os

def store_to_duckdb(fuel_df, fuel_details_df, geo_mapping_df):
    print(fuel_df.shape, fuel_details_df.shape, geo_mapping_df.shape)
    
    # make directory
    os.makedirs("db", exist_ok=True)
    con = duckdb.connect("db/fuelcheck.duckdb")

    # Drop existing tables and sequence
    con.execute("DROP TABLE IF EXISTS fuel_data")
    con.execute("DROP TABLE IF EXISTS FUEL_DETAILS")
    con.execute("DROP TABLE IF EXISTS GEO_MAPPING")
    con.execute("DROP SEQUENCE IF EXISTS station_id_seq")

    # Create sequence
    con.execute("CREATE SEQUENCE station_id_seq START 1")

    # Create FUEL_DETAILS table
    con.execute("""
        CREATE TABLE FUEL_DETAILS (
            FuelCode VARCHAR(3),
            FuelType VARCHAR(20) NOT NULL,
            Sales DECIMAL(10, 2),
            Date DATE,
            PRIMARY KEY (FuelCode, Date)
        );
    """)

    # Create GEO_MAPPING table
    con.execute("""
        CREATE TABLE GEO_MAPPING (
            Address VARCHAR(100) PRIMARY KEY,
            Latitude DECIMAL(9,6),
            Longitude DECIMAL(9,6)
        );
    """)

    # Create fuel_data table with foreign keys
    con.execute("""
        CREATE TABLE fuel_data (
            station_tracking_id INTEGER DEFAULT nextval('station_id_seq') PRIMARY KEY,
            servicestationname TEXT,
            address VARCHAR(100),
            suburb TEXT,
            postcode INTEGER,
            brand TEXT,
            fuelcode VARCHAR(3),
            fuel_date DATE,
            priceupdateddate DATE,
            price FLOAT,
            FOREIGN KEY (fuelcode, fuel_date) REFERENCES FUEL_DETAILS(FuelCode, Date),
            FOREIGN KEY (address) REFERENCES GEO_MAPPING(Address)
        );
    """)

    # Convert 'PriceUpdatedDate' to datetime, set day=1, and format as 'YYYY-MM-DD'
    fuel_df['fuel_date'] = pd.to_datetime(fuel_df['PriceUpdatedDate'], errors='coerce')

    # Drop rows with invalid dates
    fuel_df = fuel_df.dropna(subset=['fuel_date'])

    # Force the day to be 1 and format as 'YYYY-MM-DD'
    fuel_df['fuel_date'] = fuel_df['fuel_date'].apply(lambda x: x.replace(day=1)).dt.strftime('%Y-%m-%d')

    # Convert 'MM-YYYY' to 'YYYY-MM-DD'
    fuel_details_df['Month'] = pd.to_datetime(fuel_details_df['Month'], format='%m-%Y').dt.strftime('%Y-%m-%d')

    fuel_details_df = fuel_details_df.rename(columns={
        'Month': 'Date',
        'Product': 'FuelType',
        'SalesValue': 'Sales'
    })

    print("FUEL DF", fuel_df.head())
    print("FUEL DETAILS DF", fuel_details_df.head())

    invalid_rows = fuel_df.merge(
    fuel_details_df[['FuelCode', 'Date']],
        left_on=['FuelCode', 'fuel_date'],
        right_on=['FuelCode', 'Date'],
        how='left',
        indicator=True
    ).query("_merge == 'left_only'")

    print("Invalid rows due to missing foreign keys:")
    print(invalid_rows[['FuelCode', 'fuel_date']].drop_duplicates())

    # Find missing combinations
    missing_combinations = fuel_df.merge(
        fuel_details_df[['FuelCode', 'Date']],
        left_on=['FuelCode', 'fuel_date'],
        right_on=['FuelCode', 'Date'],
        how='left',
        indicator=True
    ).query("_merge == 'left_only'")[['FuelCode', 'fuel_date']].drop_duplicates()

    # Create placeholder rows
    placeholder_fuel_details = missing_combinations.rename(columns={
        'fuel_date': 'Date'
    })
    placeholder_fuel_details['FuelType'] = 'UNKNOWN'
    placeholder_fuel_details['Sales'] = 0.0

    # Append to original FUEL_DETAILS
    fuel_details_df = pd.concat([fuel_details_df, placeholder_fuel_details], ignore_index=True)

    
    # Register DataFrames
    con.register("fuel_df", fuel_df)
    con.register("fuel_details_df", fuel_details_df)
    con.register("geo_mapping_df", geo_mapping_df)

    # Insert into FUEL_DETAILS
    con.execute("""
        INSERT INTO FUEL_DETAILS (FuelCode, FuelType, Sales, Date)
        SELECT FuelCode, FuelType, Sales, Date
        FROM fuel_details_df
    """)


    # Insert into GEO_MAPPING
    con.execute("""
        INSERT INTO GEO_MAPPING
        SELECT * FROM geo_mapping_df
    """)

    # Insert into fuel_data (limit can be adjusted or removed)
    con.execute("""
        INSERT INTO fuel_data (
            servicestationname,
            address,
            suburb,
            postcode,
            brand,
            fuelcode,
            fuel_date, 
            priceupdateddate,
            price
        )
        SELECT servicestationname, address, suburb, postcode, brand, fuelcode, fuel_date, priceupdateddate, price
        FROM fuel_df
    """)

    con.close()
    print("All schemas and data stored in db/fuelcheck.duckdb")


def test_fuel_data_queries():
    pd.set_option('display.max_columns', 20)

    con = duckdb.connect("db/fuelcheck.duckdb")

    # Show all rows from fuel_data (or limited if necessary)
    print("All rows from fuel_data:")
    df_all_data = con.execute("SELECT * FROM fuel_data").fetchdf()
    print(df_all_data)

    # Count total records
    total = con.execute("SELECT COUNT(*) AS total_rows FROM fuel_data").fetchone()[0]
    print(f"\nTotal rows in fuel_data: {total}")

    # Example: Get average price per fuel type
    print("\nAverage price per fuel type:")
    avg_price = con.execute(
        """
        SELECT fuelcode, AVG(price) AS avg_price
        FROM fuel_data
        GROUP BY fuelcode
        ORDER BY avg_price DESC
        LIMIT 10
    """
    ).fetchdf()
    print(avg_price)

    con.close()
