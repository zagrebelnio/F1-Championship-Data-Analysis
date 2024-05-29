import fastf1
import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine, text

config = dotenv_values()

DB_NAME = config.get('DB_NAME')
DB_USER = config.get('DB_USER')
DB_HOST = config.get('DB_HOST')
DB_PASSWORD = config.get('DB_PASSWORD')
DB_PORT = config.get('DB_PORT')

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def get_races(start_year):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT * FROM races WHERE EXTRACT(YEAR FROM date) >= {start_year} AND date <= NOW()"))
        data = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(data, columns=columns)

races_df = get_races(2018)
races_df.head()

def is_sprint_weekend(event):
    return event.Session3 == 'Sprint' or event.Session4 == 'Sprint'

def fetch_data_from_session(year, round, type):
    session = fastf1.get_session(year, round, type)
    session.load(
        laps = True,
        telemetry = False,
        weather = False,
        messages = False
    )
    drivers_info = []
    for driver in session.drivers:
        driver_info = session.get_driver(driver)
        drivers_info.append({
            'DriverNumber': driver_info['DriverNumber'],
            'FirstName': driver_info['FirstName'],
            'Abbreviation': driver_info['Abbreviation'],
        })

    driver_info_df = pd.DataFrame(drivers_info)

    laps_df = session.laps[['Driver', 'LapTime', 'LapNumber', 'Team', 'DriverNumber']]
    df = laps_df.merge(driver_info_df, on='DriverNumber', how='left')
    return df[['LapTime', 'LapNumber', 'Team', 'FirstName', 'Abbreviation']]

def get_lap_times(races_df):
    sprint_lap_times_df = pd.DataFrame()
    race_lap_times_df = pd.DataFrame()

    for index, row in races_df.iterrows():
        date = row['date']
        round = row['round']
        year = date.year
        race_id = row['id']

        event = fastf1.get_event(year=year, gp=round)
        
        if (is_sprint_weekend(event)):
            sprint_lap_times = fetch_data_from_session(year, round, 'S')
            sprint_lap_times['race_id'] = race_id
            sprint_lap_times_df = pd.concat([sprint_lap_times_df, sprint_lap_times], ignore_index=True)
        
        race_lap_times = fetch_data_from_session(year, round, 'R')
        race_lap_times['race_id'] = race_id
        race_lap_times_df = pd.concat([race_lap_times_df, race_lap_times], ignore_index=True)

    return sprint_lap_times_df, race_lap_times_df            

sprint_lap_times_df, race_lap_times_df = get_lap_times(races_df)

sprint_lap_times_df.head()

race_lap_times_df.head()

race_lap_times_df.info()

sprint_lap_times_df.info()

race_lap_times_df = race_lap_times_df.dropna(subset=['LapTime'])
race_lap_times_df.info()

sprint_lap_times_df = sprint_lap_times_df.dropna(subset=['LapTime'])
sprint_lap_times_df.info()

race_lap_times_df['lap_time_millis'] = race_lap_times_df['LapTime'].dt.total_seconds() * 1000
race_lap_times_df.head()

sprint_lap_times_df['lap_time_millis'] = sprint_lap_times_df['LapTime'].dt.total_seconds() * 1000
sprint_lap_times_df.head()

race_lap_times_df['LapNumber'] = race_lap_times_df['LapNumber'].astype(int)
race_lap_times_df['lap_time_millis'] = race_lap_times_df['lap_time_millis'].astype(int)

race_lap_times_df.head()

sprint_lap_times_df['LapNumber'] = sprint_lap_times_df['LapNumber'].astype(int)
sprint_lap_times_df['lap_time_millis'] = sprint_lap_times_df['lap_time_millis'].astype(int)

sprint_lap_times_df.head()

def timedelta_to_string(td):
    minutes = td.seconds // 60
    seconds = td.seconds % 60
    milliseconds = td.microseconds // 1000
    return '{:02d}:{:02d}.{:03d}'.format(minutes, seconds, milliseconds)

race_lap_times_df['lap_time'] = race_lap_times_df['LapTime'].dt.total_seconds().apply(lambda x: timedelta_to_string(pd.Timedelta(seconds=x)))
race_lap_times_df.head()

sprint_lap_times_df['lap_time'] = sprint_lap_times_df['LapTime'].dt.total_seconds().apply(lambda x: timedelta_to_string(pd.Timedelta(seconds=x)))
sprint_lap_times_df.head()

def get_drivers():
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT * FROM drivers"))
        data = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(data, columns=columns)

drivers_df = get_drivers()
drivers_df.head()

drivers_df['combined_key'] = drivers_df['first_name'] + '_' + drivers_df['abbreviation']
drivers_dict = drivers_df.set_index('combined_key')['id'].to_dict()
race_lap_times_df['combined_key'] = race_lap_times_df['FirstName'] + '_' + race_lap_times_df['Abbreviation']
race_lap_times_df['driver_id'] = pd.NA

race_lap_times_df['driver_id'] = race_lap_times_df.apply(
    lambda row: drivers_dict.get(row['combined_key']) if pd.isnull(row['driver_id']) else row['driver_id'],
    axis=1
)

race_lap_times_df.info()

drivers_df['combined_key'] = drivers_df['first_name'] + '_' + drivers_df['abbreviation']
drivers_dict = drivers_df.set_index('combined_key')['id'].to_dict()
sprint_lap_times_df['combined_key'] = sprint_lap_times_df['FirstName'] + '_' + sprint_lap_times_df['Abbreviation']
sprint_lap_times_df['driver_id'] = pd.NA

sprint_lap_times_df['driver_id'] = sprint_lap_times_df.apply(
    lambda row: drivers_dict.get(row['combined_key']) if pd.isnull(row['driver_id']) else row['driver_id'],
    axis=1
)

sprint_lap_times_df.info()

race_lap_times_df.head()

def get_constructors():
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT * FROM constructors"))
        data = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(data, columns=columns)

constructors_df = get_constructors()
constructors_df.head()

constructors_dict = constructors_df.set_index('full_name')['id'].to_dict()
race_lap_times_df['constructor_id'] = pd.NA

race_lap_times_df['constructor_id'] = race_lap_times_df.apply(
    lambda row: constructors_dict.get(row['Team']) if pd.isnull(row['constructor_id']) else row['constructor_id'],
    axis=1
)

race_lap_times_df.info()

constructors_dict = constructors_df.set_index('name')['id'].to_dict()
null_constructor_id_df = race_lap_times_df[race_lap_times_df['constructor_id'].isnull()]
null_constructor_id_df['constructor_id'] = null_constructor_id_df.apply(
    lambda row: constructors_dict.get(row['Team']) if pd.isnull(row['constructor_id']) else row['constructor_id'],
    axis=1
)
null_constructor_id_df.info()

race_lap_times_df = pd.concat([race_lap_times_df, null_constructor_id_df])
race_lap_times_df.info()

race_lap_times_df = race_lap_times_df.dropna(subset=['constructor_id'])
race_lap_times_df.info()

constructors_dict = constructors_df.set_index('full_name')['id'].to_dict()
sprint_lap_times_df['constructor_id'] = pd.NA

sprint_lap_times_df['constructor_id'] = sprint_lap_times_df.apply(
    lambda row: constructors_dict.get(row['Team']) if pd.isnull(row['constructor_id']) else row['constructor_id'],
    axis=1
)

constructors_dict = constructors_df.set_index('name')['id'].to_dict()
null_constructor_id_df = sprint_lap_times_df[sprint_lap_times_df['constructor_id'].isnull()]
null_constructor_id_df['constructor_id'] = null_constructor_id_df.apply(
    lambda row: constructors_dict.get(row['Team']) if pd.isnull(row['constructor_id']) else row['constructor_id'],
    axis=1
)

sprint_lap_times_df = pd.concat([sprint_lap_times_df, null_constructor_id_df])

sprint_lap_times_df = sprint_lap_times_df.dropna(subset=['constructor_id'])
sprint_lap_times_df.info()

race_lap_times_df = race_lap_times_df[['race_id', 'driver_id', 'constructor_id', 'LapNumber', 'lap_time', 'lap_time_millis']]
sprint_lap_times_df = sprint_lap_times_df[['race_id', 'driver_id', 'constructor_id', 'LapNumber', 'lap_time', 'lap_time_millis']]

race_lap_times_df.rename(columns={'LapNumber': 'lap_number'}, inplace=True)
sprint_lap_times_df.rename(columns={'LapNumber': 'lap_number'}, inplace=True)
race_lap_times_df.head()
sprint_lap_times_df.head()

race_lap_times_df['constructor_id'] = race_lap_times_df['constructor_id'].astype('Int64')
sprint_lap_times_df['constructor_id'] = sprint_lap_times_df['constructor_id'].astype('Int64')
race_lap_times_df.head()
sprint_lap_times_df.head()

def insert_data(df, table):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()

        if count != 0:
            connection.execute(text(f"DELETE FROM {table}"))
        df.to_sql(table, engine, if_exists='append', index=False)

insert_data(race_lap_times_df, 'races_race_lap_times')
insert_data(sprint_lap_times_df, 'races_sprint_race_lap_times')

