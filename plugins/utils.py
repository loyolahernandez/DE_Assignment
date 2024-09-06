import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import snowflake.connector
import os


# get dict of stations names and timezones
def fetch_weather_data(station_id):
    # get stations names and timezones
    API_BASE_URL = "https://api.weather.gov/stations"
    response = requests.get(f"{API_BASE_URL}")
    stations = response.json().get('features', [])
    name_map = {station['properties']['stationIdentifier']: station['properties']['name'] for station in stations}
    timezone_map = {station['properties']['stationIdentifier']: station['properties']['timeZone'] for station in stations}

    # get observations of selected station
    url = f"{API_BASE_URL}/{station_id}/observations"
    response = requests.get(url)
    observations = response.json().get('features', [])

    return name_map, timezone_map, observations


# Transform and create data points
def transform_observations(station_id, observations, name_map, timezone_map):
    transformed_data = []
    name = name_map.get(station_id, "Unknown")  # Get station's timezone
    timezone = timezone_map.get(station_id, "Unknown")  # Get station's timezone

    for obs in observations:
        # Verificar y extraer cada atributo asegurándose de que no sea None antes de aplicar round
        temp = obs['properties'].get('temperature')
        temperature = round(temp['value'], 2) if temp and temp.get('value') is not None else None
        
        wind_speed_data = obs['properties'].get('windSpeed')
        wind_speed = round(wind_speed_data['value'], 2) if wind_speed_data and wind_speed_data.get('value') is not None else None
        
        humidity_data = obs['properties'].get('relativeHumidity')
        humidity = round(humidity_data['value'], 2) if humidity_data and humidity_data.get('value') is not None else None
        
        data = {
            "station_id": station_id,
            "station_name": name,
            "station_timezone": timezone,
            "latitude": obs['geometry']['coordinates'][1],
            "longitude": obs['geometry']['coordinates'][0],
            "timestamp": obs['properties']['timestamp'],
            "temperature": temperature,
            "wind_speed": wind_speed,
            "humidity": humidity
        }
        transformed_data.append(data)
    return transformed_data



def load_data_to_snowflake(data):
    """
    Función para cargar datos transformados a Snowflake.
    """
    # Obtener la contraseña desde la variable de entorno
    password = os.getenv('SNOWFLAKE_PASSWORD')
    
    # Conectar a Snowflake
    conn = snowflake.connector.connect(
        user='ignacioloyolahernandez',
        password=password,
        account='cu03892.sa-east-1.aws',
        warehouse='weather_wh',
        database='weather_db',
        schema='public',
        role='accountadmin'
    )

    cur = conn.cursor()

    # Crear la tabla si no existe
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_obs (
        id INT AUTOINCREMENT,
        observation_time TIMESTAMP,
        temperature FLOAT,
        humidity FLOAT,
        wind_speed FLOAT
    );
    """)

    # Insertar los datos transformados en la tabla
    insert_query = """
    INSERT INTO weather_obs (observation_time, temperature, humidity, wind_speed)
    VALUES (%s, %s, %s, %s);
    """
    for record in data:
        cur.execute(insert_query, (
            record['timestamp'],
            record['temperature'],
            record['humidity'],
            record['wind_speed']
        ))

    cur.close()
    conn.close()
