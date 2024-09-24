import psycopg2
import os

db_pass = os.getenv("DB_PASSWORD")

try:
    initial_conn = psycopg2.connect(dbname="postgres", user="postgres",
                                    host="localhost", port="5432", password=db_pass)
    initial_conn.autocommit = True
    print("connection successful")
except Exception as error:
    print(f"connection failed :{error}")



def create_db(conn, db_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cur.fetchone()
        # print(exists)
        if not exists:
            cur.execute(f"CREATE DATABASE {db_name}")
            print(f"Database {db_name} created successfully")
        else:
            print(f"Database {db_name} already exists")

create_db(initial_conn, "weather_record")


try:
    conn = psycopg2.connect(dbname="weather_record", user="postgres",
                            host="localhost", port="5432", password=db_pass)
    conn.autocommit = True
    print("connection successful to weather_record")
except Exception as error:
    print(f"connection failed :{error}")


def create_schema(schema_name):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print("schema created")

create_schema('weather_schema')


def create_table(table_name):
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS weather_schema.{table_name} (
                    Serial_No SERIAL PRIMARY KEY,
                    City VARCHAR(20) ,
                    Description TEXT,
                    Temperature_in_C NUMERIC,
                    Minimum_Temp_in_C NUMERIC,
                    Maximum_Temp_in_C NUMERIC,
                    Pressure INTEGER,
                    Humidity INTEGER,
                    Wind_Speed FLOAT,
                    Time_of_Record_local_time TIMESTAMP ,
                    Sunrise_local_time TIMESTAMP,
                    Sunset_local_time TIMESTAMP
                    );""")
        print(f'Table {table_name} created')

create_table('weather_data')


def load_data_to_table(conn,schema_name,table_name,data):
    try:    
        columns = [col for col in data[0].keys()]
        values = [tuple(row[col] for col in columns) for row in data]
        query = f"""INSERT INTO {schema_name}.{table_name} ({', '.join(columns)})
                            VALUES ({', '.join(['%s']*len(columns))})"""
        with conn.cursor() as cur:
            cur.executemany(query,values)
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"Data insert failed : {e}")
    