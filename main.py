from plugins.utils import fetch_weather_data, transform_observations, connect_to_postgres, load_data_to_postgres

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


if __name__ == "__main__":
    main()