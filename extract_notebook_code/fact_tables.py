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

def insert_data(df, table):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()

        if count == 0:
            df.to_sql(table, engine, if_exists='append', index=False)

circuits_id_df = pd.read_csv('../adapted_data/circuitsId.csv')
constructors_id_df = pd.read_csv('../adapted_data/constructorsId.csv')
countries_id_df = pd.read_csv('../adapted_data/countriesId.csv')
drivers_id_df = pd.read_csv('../adapted_data/driversId.csv')
grand_prix_id_df = pd.read_csv('../adapted_data/grand_prixId.csv')
seasons_id_df = pd.read_csv('../adapted_data/seasonsId.csv')

circuits_id_df.head()

races_fp1_results_df = pd.read_csv('../data/f1db-races-free-practice-1-results.csv')
races_fp1_results_df.head()

races_fp1_results_df['driver_id'] = races_fp1_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_fp1_results_df['constructor_id'] = races_fp1_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_fp1_results_df = races_fp1_results_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'time', 'timeMillis', 'gap', 'gapMillis', 'interval', 'intervalMillis', 'laps']]
races_fp1_results_df.head()

races_fp1_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number', 'time', 'time_millis', 'gap', 'gap_millis', 'interval', 'interval_millis', 'laps']

insert_data(races_fp1_results_df, 'races_fp1_results')

races_fp2_results_df = pd.read_csv('../data/f1db-races-free-practice-2-results.csv')
races_fp2_results_df.head()

races_fp2_results_df['driver_id'] = races_fp2_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_fp2_results_df['constructor_id'] = races_fp2_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_fp2_results_df = races_fp2_results_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'time', 'timeMillis', 'gap', 'gapMillis', 'interval', 'intervalMillis', 'laps']]
races_fp2_results_df.head()

races_fp2_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number', 'time', 'time_millis', 'gap', 'gap_millis', 'interval', 'interval_millis', 'laps']

insert_data(races_fp2_results_df, 'races_fp2_results')

races_fp3_results_df = pd.read_csv('../data/f1db-races-free-practice-3-results.csv')
races_fp3_results_df.head()

races_fp3_results_df['driver_id'] = races_fp3_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_fp3_results_df['constructor_id'] = races_fp3_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_fp3_results_df = races_fp3_results_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'time', 'timeMillis', 'gap', 'gapMillis', 'interval', 'intervalMillis', 'laps']]
races_fp3_results_df.head()

races_fp3_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number', 'time', 'time_millis', 'gap', 'gap_millis', 'interval', 'interval_millis', 'laps']

insert_data(races_fp3_results_df, 'races_fp3_results')

races_qualifying_results_df = pd.read_csv('../data/f1db-races-qualifying-results.csv')
races_qualifying_results_df.head()

races_qualifying_results_df['driver_id'] = races_qualifying_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_qualifying_results_df['constructor_id'] = races_qualifying_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_qualifying_results_df = races_qualifying_results_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'q1', 'q1Millis', 'q2', 'q2Millis', 'q3', 'q3Millis', 'gap', 'gapMillis', 'interval', 'intervalMillis', 'laps']]
races_qualifying_results_df.head()

races_qualifying_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number', 'q1', 'q1_millis', 'q2', 'q2_millis', 'q3', 'q3_millis', 'gap', 'gap_millis', 'interval', 'interval_millis', 'laps']

insert_data(races_qualifying_results_df, 'races_qualifying_results')

races_race_results_df = pd.read_csv('../data/f1db-races-race-results.csv')
races_race_results_df.head()
races_race_results_df.info()

races_race_results_df['driver_id'] = races_race_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_race_results_df['constructor_id'] = races_race_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_race_results_df = races_race_results_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'laps', 'time', 'timeMillis',
                                                'timePenalty', 'timePenaltyMillis', 'gap', 'gapMillis', 'gapLaps', 'interval','intervalMillis', 'reasonRetired','points','gridPositionNumber',
                                                'positionsGained','fastestLap','pitStops','driverOfTheDay','grandSlam']]
races_race_results_df.head()

races_race_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number', 'laps', 'time', 'time_millis', 
                                       'time_penalty', 'time_penalty_millis', 'gap', 'gap_millis', 'gap_laps', 'interval', 'interval_millis', 'reason_retired', 'points', 'grid_position_number', 
                                       'positions_gained', 'fastest_lap', 'pit_stops', 'driver_of_the_day', 'grand_slam']


insert_data(races_race_results_df, 'races_race_results')

races_pit_stops_df = pd.read_csv('../data/f1db-races-pit-stops.csv')
races_pit_stops_df.head()
races_pit_stops_df.info()

races_pit_stops_df['driver_id'] = races_pit_stops_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_pit_stops_df['constructor_id'] = races_pit_stops_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_pit_stops_df = races_pit_stops_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber','stop', 'lap', 'time', 'timeMillis']]
races_pit_stops_df.head()

races_pit_stops_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number','stop', 'lap', 'time', 'time_millis']


insert_data(races_pit_stops_df, 'races_pit_stops')

races_sprint_qualifying_results_df = pd.read_csv('../data/f1db-races-sprint-qualifying-results.csv')
races_sprint_qualifying_results_df.head()
#races_sprint_qualifying_results_df.info(100)

races_sprint_qualifying_results_df['driver_id'] = races_sprint_qualifying_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_sprint_qualifying_results_df['constructor_id'] = races_sprint_qualifying_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_sprint_qualifying_results_df = races_sprint_qualifying_results_df[['raceId','driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber',
                                                            'q1', 'q1Millis', 'q2', 'q2Millis', 'q3', 'q3Millis', 'gap', 'gapMillis', 'interval',
                                                             'intervalMillis', 'laps']]
races_sprint_qualifying_results_df.head()
races_sprint_qualifying_results_df.head()

races_sprint_qualifying_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number',
                            'q1', 'q1_millis', 'q2', 'q2_millis', 'q3', 'q3_millis', 'gap', 'gap_millis', 'interval',
                            'interval_millis', 'laps']



insert_data(races_sprint_qualifying_results_df, 'races_sprint_qualifying_results')

races_sprint_race_results_df = pd.read_csv('../data/f1db-races-sprint-race-results.csv')
races_sprint_race_results_df.head()
#races_sprint_race_results_df.info(100)

races_sprint_race_results_df['driver_id'] = races_sprint_race_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_sprint_race_results_df['constructor_id'] = races_sprint_race_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_sprint_race_results_df = races_sprint_race_results_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'laps',
                                                              'time', 'timeMillis', 'timePenalty', 'timePenaltyMillis', 'gap', 'gapMillis', 'gapLaps', 'interval',
                                                                'intervalMillis', 'reasonRetired', 'points', 'gridPositionNumber', 'positionsGained',
                                                                  'fastestLap', 'pitStops']]
races_sprint_race_results_df.head()
races_sprint_race_results_df.head()

races_sprint_race_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number', 'laps',
                                         'time', 'time_millis', 'time_penalty', 'time_penalty_millis', 'gap', 'gap_millis', 'gap_laps', 'interval',
                                           'interval_millis', 'reason_retired', 'points', 'grid_position_number', 'positions_gained', 'fastest_lap', 'pit_stops']



insert_data(races_sprint_race_results_df, 'races_sprint_race_results')

races_constructor_standings_df = pd.read_csv('../data/f1db-races-constructor-standings.csv')
races_constructor_standings_df.head()
#races_constructor_standings_df.info()

races_constructor_standings_df['constructor_id'] = races_constructor_standings_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_constructor_standings_df = races_constructor_standings_df[['raceId', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'points', 'positionsGained']]
races_constructor_standings_df['positionNumber'] = races_constructor_standings_df['positionNumber'].astype('Int64')
races_constructor_standings_df.head()

races_constructor_standings_df.columns = ['race_id', 'constructor_id', 'position_display_order', 'position_number', 'points', 'positions_gained']

insert_data(races_constructor_standings_df, 'races_constructor_standings')

races_dod_results_df = pd.read_csv('../data/f1db-races-driver-of-the-day-results.csv')
races_dod_results_df.head()
#races_dod_results_df.info(100)

races_dod_results_df['driver_id'] = races_dod_results_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
races_dod_results_df['constructor_id'] = races_dod_results_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']

races_dod_results_df = races_dod_results_df[['raceId', 'driver_id', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'percentage']]
races_dod_results_df.head()

races_dod_results_df.columns = ['race_id', 'driver_id', 'constructor_id', 'position_display_order', 'position_number', 'percentage']

insert_data(races_dod_results_df, 'races_dod_results')

races_driver_standings_df = pd.read_csv('../data/f1db-races-driver-standings.csv')
races_driver_standings_df.head()
#races_driver_standings_df.info(100)

races_driver_standings_df['driver_id'] = races_driver_standings_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']

races_driver_standings_df = races_driver_standings_df[['raceId', 'driver_id', 'positionDisplayOrder', 'positionNumber', 'points','positionsGained']]
races_driver_standings_df.head()

races_driver_standings_df.columns = ['race_id', 'driver_id', 'position_display_order', 'position_number', 'points','positions_gained']

insert_data(races_driver_standings_df, 'races_driver_standings')

seasons_constructor_standings_df = pd.read_csv('../data/f1db-seasons-constructor-standings.csv')
seasons_constructor_standings_df.head()


seasons_constructor_standings_df['constructor_id'] = seasons_constructor_standings_df.merge(constructors_id_df, left_on='constructorId', right_on='id', how='left')['dbId']
seasons_constructor_standings_df['year'] = seasons_constructor_standings_df.merge(seasons_id_df, left_on='year', right_on='id', how='left')['dbId']

seasons_constructor_standings_df = seasons_constructor_standings_df[['year', 'constructor_id', 'positionDisplayOrder', 'positionNumber', 'points']]

seasons_constructor_standings_df.columns = ['season_id', 'constructor_id', 'position_display_order', 'position_number', 'points']
seasons_constructor_standings_df.head()

insert_data(seasons_constructor_standings_df, 'seasons_constructor_standings')

seasons_driver_standings_df = pd.read_csv('../data/f1db-seasons-driver-standings.csv')
seasons_driver_standings_df.head()
#seasons_driver_standings_df.info(100)

seasons_driver_standings_df['driver_id'] = seasons_driver_standings_df.merge(drivers_id_df, left_on='driverId', right_on='id', how='left')['dbId']
seasons_driver_standings_df['year'] = seasons_driver_standings_df.merge(seasons_id_df, left_on='year', right_on='id', how='left')['dbId']

seasons_driver_standings_df = seasons_driver_standings_df[['year', 'driver_id', 'positionDisplayOrder', 'positionNumber', 'points']]

seasons_driver_standings_df.head()

seasons_driver_standings_df.columns = ['season_id', 'driver_id', 'position_display_order', 'position_number', 'points']
seasons_driver_standings_df.head()


insert_data(seasons_driver_standings_df, 'seasons_driver_standings')

