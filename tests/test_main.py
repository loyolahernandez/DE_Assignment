"""
Unit tests for the main.py module.

This test suite verifies the following functionalities in main.py:
- The main function: Ensures that all key steps (Extract, Transform, Load, and Query Execution) are executed correctly.
- Dependencies on fetch_weather_data, transform_observations, connect_to_postgres, load_data_to_postgres, and execute_query are mocked to isolate the function's behavior.
- The tests simulate successful extraction of weather data, database connection, data loading, and query execution.
"""

import pytest
from unittest.mock import patch
from main import main

# Test the main function
@patch('main.execute_query')  # Mock execute_query to simulate SQL execution
@patch('main.load_data_to_postgres')  # Mock load_data_to_postgres to simulate data insertion
@patch('main.connect_to_postgres')  # Mock connect_to_postgres to avoid real database connections
@patch('main.transform_observations')  # Mock transform_observations to simulate data transformation
@patch('main.fetch_weather_data')  # Mock fetch_weather_data to simulate API calls
def test_main(mock_fetch, mock_transform, mock_connect, mock_load, mock_execute):
    # Mock the return values for each function
    mock_fetch.return_value = (
        {"0128W": "Station Name"},  # name_map
        {"0128W": "UTC"},  # timezone_map
        [{"properties": {"temperature": {"value": 22.5}}}]  # observations
    )
    mock_transform.return_value = [
        {
            "station_id": "0128W",
            "station_name": "Station Name",
            "station_timezone": "UTC",
            "latitude": 37.0,
            "longitude": -122.0,
            "timestamp": "2024-09-01T00:00:00Z",
            "temperature": 22.5,
            "wind_speed": 5.6,
            "humidity": 80
        }
    ]
    mock_execute.side_effect = [
        [(22.5,)],  # temperature query result
        [(12.3,)]   # wind speed query result
    ]

    # Call the main function
    main()

    # Ensure that all functions are called
    mock_fetch.assert_called_once_with('0128W')
    mock_transform.assert_called_once()
    mock_connect.assert_called_once()
    mock_load.assert_called_once_with(mock_transform.return_value)
    mock_execute.assert_any_call('queries/temperature.sql')
    mock_execute.assert_any_call('queries/wind.sql')


    assert mock_execute.call_count == 2  # Should call execute_query twice
