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
def transform_observations(station_id, name_map, timezone_map, observations):
    transformed_data = []
    name = name_map.get(station_id, "Unknown")  # Get station's timezone
    timezone = timezone_map.get(station_id, "Unknown")  # Get station's timezone

    for obs in observations:
        # Specific data transformations (if not None)
        temp = obs['properties'].get('temperature')
        temperature = round(temp['value'], 2) if temp and temp.get('value') is not None else None
        
        wind_speed_data = obs['properties'].get('windSpeed')
        wind_speed = round(wind_speed_data['value'], 2) if wind_speed_data and wind_speed_data.get('value') is not None else None
        
        humidity_data = obs['properties'].get('relativeHumidity')
        humidity = round(humidity_data['value'], 2) if humidity_data and humidity_data.get('value') is not None else None
        
        # Final data to be loaded
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



# Load data to Snowflake db
def load_data_to_snowflake(data):
    """
    Función para cargar un DataFrame a la tabla en Snowflake.
    """
    # Get snowflake password
    password = os.getenv('SNOWFLAKE_PASSWORD')
    
    # Snowflake connection
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

    # Insertar o actualizar datos usando el comando MERGE
    merge_query = """
    MERGE INTO WEATHER_OBS AS target
    USING (SELECT %s AS station_id,
                  %s AS station_name,
                  %s AS station_timezone,
                  %s AS latitude,
                  %s AS longitude,
                  %s AS timestamp,
                  %s AS temperature,
                  %s AS wind_speed,
                  %s AS humidity
           ) AS source
    ON target.station_id = source.station_id AND target.timestamp = source.timestamp
    WHEN MATCHED THEN 
        UPDATE SET 
            target.temperature = source.temperature,
            target.wind_speed = source.wind_speed,
            target.humidity = source.humidity
    WHEN NOT MATCHED THEN 
        INSERT (station_id, station_name, station_timezone, latitude, longitude, timestamp, temperature, wind_speed, humidity)
        VALUES (source.station_id, source.station_name, source.station_timezone, source.latitude, source.longitude, source.timestamp, source.temperature, source.wind_speed, source.humidity);
    """

    # Iterar sobre el DataFrame o lista de diccionarios
    for record in data:
        cur.execute(merge_query, (
            record['station_id'],
            record['station_name'],
            record['station_timezone'],
            record['latitude'],
            record['longitude'],
            record['timestamp'],
            record['temperature'],
            record['wind_speed'],
            record['humidity']
        ))

    cur.close()
    conn.close()