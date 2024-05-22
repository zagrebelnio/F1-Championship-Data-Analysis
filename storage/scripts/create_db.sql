-- Table seasons
CREATE TABLE seasons (
    id SERIAL PRIMARY KEY,
    year INTEGER
);

CREATE TABLE countries (
    id SERIAL PRIMARY KEY,
    alpha3_code CHAR(3),
    name VARCHAR,
    demonym VARCHAR
);

-- Table circuits
CREATE TABLE circuits (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    full_name TEXT,
    type VARCHAR,
    country_id INTEGER REFERENCES countries(id),
    latitude FLOAT,
    longitude FLOAT
);

-- Table constructors
CREATE TABLE constructors (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    full_name VARCHAR,
    country_id INTEGER REFERENCES countries(id)
);

-- Table drivers
CREATE TABLE drivers (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    full_name VARCHAR,
    abbrevation CHAR(3),
    permanentNumber INTEGER,
    gender VARCHAR,
    dateOfBirth DATE,
    nationality_country_id INTEGER REFERENCES countries(id)
);

-- Table grand_prix
CREATE TABLE grand_prix (
    id SERIAL PRIMARY KEY,
    name VARCHAR,
    full_name TEXT,
    short_name VARCHAR,
    abbrevation CHAR(3),
    country_id INTEGER REFERENCES countries(id)
);

-- Table races
CREATE TABLE races (
    id SERIAL PRIMARY KEY,
    season_id INTEGER REFERENCES seasons(id),
    round INTEGER,
    date DATE,
    grand_prix_id INTEGER REFERENCES grand_prix(id),
    official_name TEXT,
    circuit_id INTEGER REFERENCES circuits(id),
    course_length FLOAT,
    laps INTEGER,
    distance FLOAT
);

-- Table races_fp1_results
CREATE TABLE races_fp1_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    time VARCHAR,
    time_millis INTEGER,
    gap VARCHAR,
    gap_millis INTEGER,
    interval VARCHAR,
    interval_millis INTEGER,
    laps INTEGER
);

-- Table races_fp2_results
CREATE TABLE races_fp2_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    time VARCHAR,
    time_millis INTEGER,
    gap VARCHAR,
    gap_millis INTEGER,
    interval VARCHAR,
    interval_millis INTEGER,
    laps INTEGER
);

-- Table races_fp3_results
CREATE TABLE races_fp3_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    time VARCHAR,
    time_millis INTEGER,
    gap VARCHAR,
    gap_millis INTEGER,
    interval VARCHAR,
    interval_millis INTEGER,
    laps INTEGER
);

-- Table races_qualifying_results
CREATE TABLE races_qualifying_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    q1 VARCHAR,
    q1_millis INTEGER,
    q2 VARCHAR,
    q2_millis INTEGER,
    q3 VARCHAR,
    q3_millis INTEGER,
    gap VARCHAR,
    gap_millis INTEGER,
    interval VARCHAR,
    interval_millis INTEGER,
    laps INTEGER
);

-- Table races_race_results
CREATE TABLE races_race_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    laps INTEGER,
    time VARCHAR,
    time_millis INTEGER,
    time_penalty VARCHAR,
    time_penalty_millis INTEGER,
    gap VARCHAR,
    gap_millis INTEGER,
    gap_laps INTEGER,
    interval VARCHAR,
    interval_millis INTEGER,
    reason_retired VARCHAR,
    points INTEGER,
    grid_position_number INTEGER,
    positions_gained INTEGER,
    fastest_lap BOOLEAN,
    pit_stops INTEGER,
    driver_of_the_day BOOLEAN,
    grand_slam BOOLEAN
);

-- Table races_pit_stops
CREATE TABLE races_pit_stops (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    stop INTEGER,
    lap INTEGER,
    time VARCHAR,
    time_millis INTEGER
);

-- Table races_sprint_qualifying_results
CREATE TABLE races_sprint_qualifying_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    q1 VARCHAR,
    q1_millis INTEGER,
    q2 VARCHAR,
    q2_millis INTEGER,
    q3 VARCHAR,
    q3_millis INTEGER,
    gap VARCHAR,
    gap_millis INTEGER,
    interval VARCHAR,
    interval_millis INTEGER,
    laps INTEGER
);

-- Table races_sprint_race_results
CREATE TABLE races_sprint_race_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    laps INTEGER,
    time VARCHAR,
    time_millis INTEGER,
    time_penalty VARCHAR,
    time_penalty_millis INTEGER,
    gap VARCHAR,
    gap_millis INTEGER,
    gap_laps INTEGER,
    interval VARCHAR,
    interval_millis INTEGER,
    reason_retired VARCHAR,
    points INTEGER,
    grid_position_number INTEGER,
    positions_gained INTEGER,
    fastest_lap BOOLEAN,
    pit_stops INTEGER
);

-- Table races_constructor_standings
CREATE TABLE races_constructor_standings (
    race_id INTEGER REFERENCES races(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    points INTEGER,
    positions_gained INTEGER
);

-- Table races_dod_results
CREATE TABLE races_dod_results (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    percentage FLOAT
);

-- Table races_driver_standings
CREATE TABLE races_driver_standings (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    position_display_order INTEGER,
    position_number INTEGER,
    points INTEGER,
    positions_gained INTEGER
);



-- Table seasons_constructor_standings
CREATE TABLE seasons_constructor_standings (
    season_id INTEGER REFERENCES seasons(id),
    constructor_id INTEGER REFERENCES constructors(id),
    position_display_order INTEGER,
    position_number INTEGER,
    points INTEGER
);

-- Table seasons_driver_standings
CREATE TABLE seasons_driver_standings (
    season_id INTEGER REFERENCES seasons(id),
    driver_id INTEGER REFERENCES drivers(id),
    position_display_order INTEGER,
    position_number INTEGER,
    points INTEGER
);

-- Table races_sprint_race_lap_times
CREATE TABLE races_sprint_race_lap_times (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    lap_number INTEGER,
    lap_time VARCHAR,
    lap_time_millis INTEGER
);

-- Table races_race_lap_times
CREATE TABLE races_race_lap_times (
    race_id INTEGER REFERENCES races(id),
    driver_id INTEGER REFERENCES drivers(id),
    constructor_id INTEGER REFERENCES constructors(id),
    lap_number INTEGER,
    lap_time VARCHAR,
    lap_time_millis INTEGER
);
