"""
Unit tests for the utils.py module.

This test suite verifies the following functionalities in utils.py:
- fetch_weather_data: Tests for successful and failed API calls to fetch weather data from the National Weather Service.
- transform_observations: Ensures that raw weather observations are correctly transformed into the appropriate format for database insertion.
- connect_to_postgres: Tests the connection to a PostgreSQL database, covering both success and failure scenarios.
- load_data_to_postgres: Verifies that transformed weather data is correctly inserted into the PostgreSQL database.
- execute_query: Ensures that SQL queries are correctly read from files and executed against the PostgreSQL database.

The tests utilize unittest.mock to simulate external dependencies such as requests for API calls and psycopg2 for database connections.
"""


import pytest
from unittest.mock import patch, mock_open
import requests
import psycopg2
from src.utils import (
    fetch_weather_data, 
    transform_observations, 
    connect_to_postgres, 
    load_data_to_postgres, 
    execute_query
)

# Test fetch_weather_data - success case
@patch('src.utils.requests.get')
def test_fetch_weather_data_success(mock_get):
    # Mock API response for stations and observations
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.side_effect = [
        {
            "features": [
                {
                    "properties": {
                        "stationIdentifier": "STATION_1",
                        "name": "Station One",
                        "timeZone": "UTC"
                    }
                }
            ]
        },
        {
            "features": [
                {
                    "properties": {
                        "temperature": {"value": 22.5},
                        "windSpeed": {"value": 5.6},
                        "relativeHumidity": {"value": 80},
                        "timestamp": "2024-09-01T00:00:00Z"
                    },
                    "geometry": {
                        "coordinates": [-122.0, 37.0]
                    }
                }
            ]
        }
    ]

    station_id = "STATION_1"
    name_map, timezone_map, observations = fetch_weather_data(station_id)

    assert "STATION_1" in name_map
    assert name_map["STATION_1"] == "Station One"
    assert timezone_map["STATION_1"] == "UTC"
    assert len(observations) > 0
    assert observations[0]["properties"]["temperature"]["value"] == 22.5

# Test fetch_weather_data - failure case
@patch('src.utils.requests.get')
def test_fetch_weather_data_fail(mock_get):
    # Mock API request failure
    mock_get.side_effect = requests.RequestException

    station_id = "STATION_1"
    name_map, timezone_map, observations = fetch_weather_data(station_id)

    assert name_map == {}
    assert timezone_map == {}
    assert observations == []

# Test transform_observations
def test_transform_observations():
    # Mock input data for transformation
    station_id = "STATION_1"
    name_map = {"STATION_1": "Station One"}
    timezone_map = {"STATION_1": "UTC"}
    observations = [
        {
            "properties": {
                "temperature": {"value": 22.5},
                "windSpeed": {"value": 5.6},
                "relativeHumidity": {"value": 80},
                "timestamp": "2024-09-01T00:00:00Z"
            },
            "geometry": {
                "coordinates": [-122.0, 37.0]
            }
        }
    ]

    result = transform_observations(station_id, name_map, timezone_map, observations)
    assert len(result) == 1
    assert result[0]["station_name"] == "Station One"
    assert result[0]["station_timezone"] == "UTC"
    assert result[0]["temperature"] == 22.5
    assert result[0]["wind_speed"] == 5.6
    assert result[0]["humidity"] == 80

# Test connect_to_postgres - success case
@patch('src.utils.psycopg2.connect')
def test_connect_to_postgres_success(mock_connect):
    # Mock successful PostgreSQL connection
    mock_connect.return_value = True  
    conn = connect_to_postgres()
    assert conn is not None
    assert mock_connect.called

# Test connect_to_postgres - failure case
@patch('src.utils.psycopg2.connect')
def test_connect_to_postgres_fail(mock_connect):
    # Mock failure in PostgreSQL connection
    mock_connect.side_effect = psycopg2.DatabaseError  
    conn = connect_to_postgres()
    assert conn is None

# Test execute_query
@patch('src.utils.connect_to_postgres')  # Make sure to adjust this path if needed
@patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM weather_obs;")
def test_execute_query(mock_open, mock_connect):
    # Mock database connection and cursor
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    # Simulate successful execution of the query
    mock_cursor.execute.return_value = True
    
    # Call the function to execute the query
    result = execute_query("path/to/query.sql")
    
    # Verify that the execute method was called
    mock_cursor.execute.assert_called_once_with("SELECT * FROM weather_obs;")

# Test load_data_to_postgres
@patch('src.utils.connect_to_postgres')
def test_load_data_to_postgres(mock_connect):
    # Mock database connection and cursor
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    
    # Mock input data for database insertion
    data = [
        {
            "station_id": "STATION_1",
            "station_name": "Station One",
            "station_timezone": "UTC",
            "latitude": 37.0,
            "longitude": -122.0,
            "timestamp": "2024-09-01T00:00:00Z",
            "temperature": 22.5,
            "wind_speed": 5.6,
            "humidity": 80
        }
    ]
    
    # Call the function to load data into the database
    load_data_to_postgres(data)
    
    # Verify that the execute method was called
    assert mock_cursor.execute.called
