
import os
import psycopg2
import requests as r
from datetime import datetime
from db_utils import load_data_to_table
from dotenv import load_dotenv

load_dotenv()  # Automatically load variables from .env into environment variables

api_key = os.getenv("API_KEY")
db_pass = os.getenv("DB_PASSWORD")


def extract():
    # with open("credential.txt", "r") as f:
    #     api_key = f.read().strip()
    # print(api_key)
    data = {}
    cities = ['chennai', 'bengaluru', 'madurai', 'coimbatore']

    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

        res = r.get(url)
        res = res.json()
        data[city] = res
    return data
    # print(data)


def kelvin_to_celsius(temp):
    celsius = (temp - 273.15)
    return round(celsius, 2)


def transform(data):
    transformed_data_list = []
    for place, datas in data.items():
        city = data[place]['name']
        weather_description = data[place]['weather'][0]['description']
        temperature_celsius = kelvin_to_celsius(data[place]['main']['temp'])
        min_temp_celsius = kelvin_to_celsius(data[place]['main']['temp_min'])
        max_temp_celsius = kelvin_to_celsius(data[place]['main']['temp_max'])
        pressure = data[place]['main']['pressure']
        humidity = data[place]['main']['humidity']
        wind_speed = data[place]['wind']['speed']
        time_record = datetime.fromtimestamp(
            data[place]['dt']+data[place]['timezone'])
        sunrise_time = datetime.fromtimestamp(
            data[place]['sys']['sunrise']+data[place]['timezone'])
        sunset_time = datetime.fromtimestamp(
            data[place]['sys']['sunset']+data[place]['timezone'])

        transformed_data = {"City": city,
                            "Description": weather_description,
                            "Temperature_in_C": temperature_celsius,
                            "Minimum_Temp_in_C": min_temp_celsius,
                            "Maximum_Temp_in_C": max_temp_celsius,
                            "Pressure": pressure,
                            "Humidity": humidity,
                            "Wind_Speed": wind_speed,
                            "Time_of_Record_local_time": time_record,
                            "Sunrise_local_time": sunrise_time,
                            "Sunset_local_time": sunset_time
                            }
        transformed_data_list.append(transformed_data)
    return transformed_data_list

# print(transformed_data)
# df = pd.DataFrame(transformed_data_list))
# print(df)


def load(data):
    conn = psycopg2.connect(dbname="weather_record", user="postgres",
                            host="localhost", port="5432", password=db_pass)
    load_data_to_table(conn, 'weather_schema', 'weather_data', data)
    conn.close()
    print("Data inserted")


if __name__== "__main__":
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)
