"""
Main module for the weather data pipeline.

This script performs the following tasks:
1. Fetches weather data from the National Weather Service API for a specific station.
2. Transforms the raw observations into a structured format.
3. Connects to a PostgreSQL database.
4. Loads the transformed data into the database.
5. Executes queries to calculate the average temperature and wind speed change over the last week.
"""

from src.utils import fetch_weather_data, transform_observations, connect_to_postgres, load_data_to_postgres, execute_query

def main():
    # Extract
    station_id = '0128W'
    name_map, timezone_map, observations = fetch_weather_data(station_id)

    # Transform
    transformed_data = transform_observations(station_id, name_map, timezone_map, observations)

    # Connect to db
    connect_to_postgres()
    
    # Load
    load_data_to_postgres(transformed_data)

    # Execute queries after loading data
    temp_result = execute_query('queries/temperature.sql')
    wind_result = execute_query('queries/wind.sql')

    # Print the results of the queries
    print(f"Average temperature for last week: {temp_result}")
    print(f"Max wind speed change in last 7 days: {wind_result}")

if __name__ == "__main__":
    main()