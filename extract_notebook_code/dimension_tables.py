import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine, text
import os

config = dotenv_values()

DB_NAME = config.get('DB_NAME')
DB_USER = config.get('DB_USER')
DB_HOST = config.get('DB_HOST')
DB_PASSWORD = config.get('DB_PASSWORD')
DB_PORT = config.get('DB_PORT')

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

countries_df = pd.read_csv('../data/f1db-countries.csv')
countries_df.head()

countries_df = countries_df[['alpha3Code', 'name', 'demonym', 'id']]
countries_df.head()

countries_df.columns = ['alpha3_code', 'name', 'demonym', 'id']
countries_df.head()

def insert_data(df, table):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()

        if count == 0:
            df.to_sql(table, engine, if_exists='append', index=False)

        result = connection.execute(text(f"SELECT id FROM {table}"))
        generated_ids = [row[0] for row in result]
        
    return generated_ids

countries_df['dbId'] = insert_data(countries_df[['alpha3_code', 'name', 'demonym']], 'countries')
countries_df.head()

circuits_df = pd.read_csv('../data/f1db-circuits.csv')
circuits_df.head()

circuits_df['countryId'] = circuits_df.merge(countries_df[['id', 'dbId']], left_on='countryId', right_on='id', how='left')['dbId']
circuits_df.head()

circuits_df = circuits_df[['name', 'fullName', 'type', 'countryId', 'latitude', 'longitude', 'id']]
circuits_df.head()

circuits_df.columns = ['name', 'full_name', 'type', 'country_id', 'latitude', 'longitude', 'id']
circuits_df.head()

circuits_df['dbId'] = insert_data(circuits_df[['name', 'full_name', 'type', 'country_id', 'latitude', 'longitude']], 'circuits')
circuits_df.head()

constructors_df = pd.read_csv('../data/f1db-constructors.csv')
constructors_df.head()

constructors_df['countryId'] = constructors_df.merge(countries_df[['id', 'dbId']], left_on='countryId', right_on='id', how='left')['dbId']
constructors_df.head()

constructors_df = constructors_df[['name', 'fullName', 'countryId', 'id']]
constructors_df.head()

constructors_df.columns = ['name', 'full_name', 'country_id', 'id']
constructors_df.head()

constructors_df['dbId'] = insert_data(constructors_df[['name', 'full_name', 'country_id']], 'constructors')
constructors_df.head()

drivers_df = pd.read_csv('../data/f1db-drivers.csv')
drivers_df.head()

drivers_df['nationalityCountryId'] = drivers_df.merge(countries_df[['id', 'dbId']], left_on='nationalityCountryId', right_on='id', how='left')['dbId']
drivers_df[['nationalityCountryId']].head()

drivers_df = drivers_df[['name', 'firstName', 'lastName', 'fullName', 'abbreviation', 'permanentNumber', 'gender', 'dateOfBirth', 'nationalityCountryId', 'id']]
drivers_df.head()

drivers_df.columns = ['name', 'first_name', 'last_name', 'full_name', 'abbreviation', 'permanent_number', 'gender', 'date_of_birth', 'nationality_country_id', 'id']
drivers_df.head()

drivers_df['dbId'] = insert_data(drivers_df[['name', 'first_name', 'last_name', 'full_name', 'abbreviation', 'permanent_number', 'gender', 'date_of_birth', 'nationality_country_id']], 'drivers')
drivers_df.head()

grand_prix_df = pd.read_csv('../data/f1db-grands-prix.csv')
grand_prix_df.head()

grand_prix_df['countryId'] = grand_prix_df.merge(countries_df[['id', 'dbId']], left_on='countryId', right_on='id', how='left')['dbId'].astype('Int64')
grand_prix_df.head()

grand_prix_df = grand_prix_df[['name', 'fullName', 'shortName', 'abbreviation', 'countryId', 'id']]
grand_prix_df.head()

grand_prix_df.columns = ['name', 'full_name', 'short_name', 'abbreviation', 'country_id', 'id']
grand_prix_df.head()

grand_prix_df['dbId'] = insert_data(grand_prix_df[['name', 'full_name', 'short_name', 'abbreviation', 'country_id']], 'grand_prix')
grand_prix_df.head()

seasons_df = pd.read_csv('../data/f1db-seasons.csv')
seasons_df.head()

seasons_df['dbId'] = insert_data(seasons_df, 'seasons')
seasons_df.head()

races_df = pd.read_csv('../data/f1db-races.csv')
races_df.head()

races_df['year'] = races_df.merge(seasons_df, on='year', how='left')['dbId'].astype('Int64')
races_df.head()

races_df['grandPrixId'] = races_df.merge(grand_prix_df[['id', 'dbId']], left_on='grandPrixId', right_on='id', how='left')['dbId'].astype('Int64')
races_df.head()

races_df['circuitId'] = races_df.merge(circuits_df[['id', 'dbId']], left_on='circuitId', right_on='id', how='left')['dbId'].astype('Int64')
races_df.head()

races_df = races_df[['id', 'year', 'round', 'date', 'grandPrixId', 'officialName', 'circuitId', 'courseLength', 'laps', 'distance']]
races_df.head()

races_df.columns = ['id', 'season_id', 'round', 'date', 'grand_prix_id', 'official_name', 'circuit_id', 'course_length', 'laps', 'distance']
races_df.head()

insert_data(races_df, 'races')
races_df.head()

columns = ['id', 'dbId']

seasons_df.columns = columns

folder = '../adapted_data/'

dfs_to_save = {
    'countries': countries_df,
    'circuits': circuits_df,
    'constructors': constructors_df,
    'drivers': drivers_df,
    'grand_prix': grand_prix_df,
    'seasons': seasons_df
}

for file_name, df in dfs_to_save.items():
    file_path = os.path.join(folder, f"{file_name}Id.csv")
    df[columns].to_csv(file_path, index=False)

