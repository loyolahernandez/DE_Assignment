import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
import psycopg2
import time

# Fetch weather data from the API with 3 retry attempts
def fetch_weather_data(station_id):
    """
    Fetch the weather data for a given station from the National Weather Service API.
    
    Args:
        station_id (str): The ID of the weather station to fetch data for.
        
    Returns:
        tuple: A tuple containing:
            - name_map (dict): A dictionary mapping station IDs to station names.
            - timezone_map (dict): A dictionary mapping station IDs to their time zones.
            - observations (list): A list of observations for the specified station.
    """

    API_BASE_URL = "https://api.weather.gov/stations"

    # Try API connection 3 times max
    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
        try:
            # Fetch stations names and timezones
            response = requests.get(f"{API_BASE_URL}")
            stations = response.json().get('features', [])
            name_map = {station['properties']['stationIdentifier']: station['properties']['name'] for station in stations}
            timezone_map = {station['properties']['stationIdentifier']: station['properties']['timeZone'] for station in stations}

            # Fetch observations of selected station
            url = f"{API_BASE_URL}/{station_id}/observations"
            response = requests.get(url)
            observations = response.json().get('features', [])

            return name_map, timezone_map, observations

        except requests.RequestException as e:
            attempts += 1
            print(f"API connection attempt {attempts} failed: {e}")
            if attempts < max_attempts:
                print(f"Retrying API connection in 5 seconds...")
                time.sleep(5)
            else:
                print("Failed to fetch data from the API after 3 attempts.")
                return {}, {}, []



# Transform and create data points for insertion into the database
def transform_observations(station_id, name_map, timezone_map, observations):
    """
    Transform the raw weather observations into a structured format for database insertion.
    
    Args:
        station_id (str): The ID of the weather station.
        name_map (dict): A dictionary mapping station IDs to station names.
        timezone_map (dict): A dictionary mapping station IDs to their time zones.
        observations (list): A list of weather observations to transform.
        
    Returns:
        list: A list of transformed data dictionaries, ready for insertion into the database.
    """

    transformed_data = []
    name = name_map.get(station_id, "Unknown")  # Get station's timezone
    timezone = timezone_map.get(station_id, "Unknown")  # Get station's timezone

    for obs in observations:
        try:
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

        except KeyError as e:
            print(f"Error transforming observation: missing key {e}")

    return transformed_data



# Connect to the PostgreSQL database with 3 retries
def connect_to_postgres():
    """
    Establish a connection to the PostgreSQL database with up to 3 retry attempts.
    
    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database, or None if the connection fails.
    """
    attempts = 0
    max_attempts = 3
    while attempts < max_attempts:
        try:
            conn = psycopg2.connect(
                host="db",
                port="5432",
                database="weather_db",
                user="myuser",
                password="mypassword"
            )
            print("Successfully connected to PostgreSQL")
            return conn

        except psycopg2.DatabaseError as e:
            attempts += 1
            print(f"Attempt {attempts} failed: {e}")
            if attempts < max_attempts:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print("Failed to connect to PostgreSQL after 3 attempts.")
                return None



# Load data into the PostgreSQL database
def load_data_to_postgres(data):
    """
    Insert the transformed weather data into the PostgreSQL database.
    
    Args:
        data (list): A list of transformed weather data dictionaries to insert.
    """

    conn = connect_to_postgres()
    if conn is None:
            return

    try:
        cur = conn.cursor()

        # Create the table if it does not exist
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_obs (
            station_id VARCHAR(30),
            station_name VARCHAR(30),
            station_timezone VARCHAR(30),
            latitude INTEGER,
            longitude INTEGER,
            timestamp VARCHAR(30),
            temperature INTEGER,
            wind_speed INTEGER,
            humidity INTEGER
        );
        """)

        # Insert the data into the table
        insert_query = """
        INSERT INTO weather_obs (
            station_id, station_name, station_timezone, latitude, longitude, timestamp, temperature, wind_speed, humidity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        for record in data:
            cur.execute(insert_query, (
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

        conn.commit()

    except psycopg2.DatabaseError as e:
        print(f"Error loading data to PostgreSQL: {e}")
    finally:
        cur.close()
        conn.close()