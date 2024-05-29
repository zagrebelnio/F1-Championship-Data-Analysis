from dotenv import dotenv_values
import psycopg2
from sqlalchemy import create_engine, text

config = dotenv_values()

DB_NAME = config.get('DB_NAME')
DB_USER = config.get('DB_USER')
DB_HOST = config.get('DB_HOST')
DB_PASSWORD = config.get('DB_PASSWORD')
DB_PORT = config.get('DB_PORT')

DB_URL_NO_DB = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/"

try:
    con = psycopg2.connect(
        dbname='postgres',
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    con.autocommit = True
    cur = con.cursor()
    cur.execute(f"CREATE DATABASE {DB_NAME}")
    cur.close()
    con.close()
    print(f"Database '{DB_NAME}' created successfully.")
except psycopg2.errors.DuplicateDatabase:
    print(f"Database '{DB_NAME}' already exists.")
except Exception as e:
    print(f"Error creating database '{DB_NAME}': {e}")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL)

with open('create_db.sql', 'r') as f:
    create_sql = f.read()

sql_commands = create_sql.split(';')
sql_commands.pop()

with engine.connect() as connection:
    try:
        transaction = connection.begin()
        for script in sql_commands:
            connection.execute(text(script))
        transaction.commit()
        print("SQL script executed successfully.")
    except Exception as e:
        transaction.rollback()
        print(f"Error executing SQL script: {e}")

