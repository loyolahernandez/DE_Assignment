from plugins.utils import fetch_weather_data, transform_observations, load_data_to_snowflake

def main():
    # Extract
    station_id = '0128W'
    name_map, timezone_map, observations = fetch_weather_data(station_id)

    # Transform
    transformed_data = transform_observations(station_id, name_map, timezone_map, observations)

    # Load
    load_data_to_snowflake(transformed_data)


if __name__ == "__main__":
    main()