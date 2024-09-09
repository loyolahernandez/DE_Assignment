import pytest
from unittest.mock import patch, mock_open
import requests  # Importar requests para poder mockearlo correctamente
import psycopg2  # Importar psycopg2 para mockear la conexión a PostgreSQL
from src.utils import (
    fetch_weather_data, 
    transform_observations, 
    connect_to_postgres, 
    load_data_to_postgres, 
    execute_query
)

# Probar fetch_weather_data
@patch('src.utils.requests.get')
def test_fetch_weather_data_success(mock_get):
    # Simular la respuesta de las estaciones
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

@patch('src.utils.requests.get')
def test_fetch_weather_data_fail(mock_get):
    # Simular un fallo en la conexión a la API
    mock_get.side_effect = requests.RequestException

    station_id = "STATION_1"
    name_map, timezone_map, observations = fetch_weather_data(station_id)

    assert name_map == {}
    assert timezone_map == {}
    assert observations == []


# Probar transform_observations
def test_transform_observations():
    # Datos simulados de entrada
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


# Probar connect_to_postgres
@patch('src.utils.psycopg2.connect')
def test_connect_to_postgres_success(mock_connect):
    mock_connect.return_value = True  # Simula una conexión exitosa
    conn = connect_to_postgres()
    assert conn is not None
    assert mock_connect.called

@patch('src.utils.psycopg2.connect')
def test_connect_to_postgres_fail(mock_connect):
    mock_connect.side_effect = psycopg2.DatabaseError  # Simula un fallo en la conexión
    conn = connect_to_postgres()
    assert conn is None


# Probar load_data_to_postgres
@patch('src.utils.connect_to_postgres')
@patch('src.utils.psycopg2.connect')
def test_load_data_to_postgres(mock_connect, mock_conn):
    # Simulamos la conexión y la inserción de datos
    mock_conn.cursor.return_value = mock_connect
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
    load_data_to_postgres(data)
    assert mock_conn.cursor().execute.called


# Probar execute_query
@patch('src.utils.connect_to_postgres')
@patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM weather_obs;")
def test_execute_query(mock_open, mock_connect):
    mock_connect.cursor.return_value.execute.return_value = True
    result = execute_query("path/to/query.sql")
    assert mock_connect.cursor().execute.called
