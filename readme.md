# WeatherETL

## Project Description

**WeatherETL** is a Python-based ETL pipeline that does extraction, transformation, and loading of weather data from the OpenWeatherMap API into a PostgreSQL database. The project collects real-time weather information for selected cities and transforms it into a structured format, storing it in a database for further analysis.

### Features
- Extracts real-time weather data from OpenWeatherMap API.
- Transforms the data into a structured format, including temperature (in Celsius), humidity, wind speed, and other weather-related metrics.
- Loads the transformed data into a PostgreSQL database.
- Designed to handle multiple cities, updating the database with new weather data on each execution.

## Requirements

* Make sure the following libraries are installed:

```bash 
pip install psycopg2 requests python-dotenv
```

## Setup Instructions
1. Clone this repository:
    ```bash
    git clone https://github.com/branesh2k/weather-etl-pipeline.git
    ```
2. Create a .env file in the root directory to store your API key and database password. Example `.env`:
    ```pyhton
    API_KEY= your_api_key
    DB_PASSWORD= your_database_password
    ```

3. Run the ETL pipeline:
    ```bash
    python main.py
    ```

## Project Structure
* `main.py`: Contains the ETL logic to extract data from the API, transform it, and load it into the database.

* `db_utils.py`: Contains helper functions for database management (creating databases, schemas, tables, and loading data).

* `.env`: Stores database credentials and API key (not included in the repository for security).

## Usage
* The script automatically collects weather data from the OpenWeatherMap API for the cities that specified in the code. It then processes the data and stores it in the PostgreSQL database, allowing you to query the data for analysis.

* You can modify the cities in the `main.py` file, under the `cities` list.