
# Weather Data Pipeline

## Overview

This repository contains a weather data pipeline designed to fetch, store, and analyze weather observations from the National Weather Service API. The pipeline is built using Python and PostgreSQL, following best practices for data ingestion, transformation, and analysis.

The pipeline fetches weather data for a selected station, transforms the data, loads it into a PostgreSQL database, and performs SQL queries to calculate key metrics such as the average temperature and maximum wind speed change for the past week.

## Features

- **Weather Data Extraction**: Fetches weather data from the National Weather Service API.
- **Data Transformation**: Transforms the raw API data into a structured format suitable for database storage.
- **PostgreSQL Integration**: Stores the transformed data in a PostgreSQL database.
- **Data Analysis**: Executes SQL queries to compute the average temperature and wind speed changes over the past week.
- **No Duplicate Records**: The pipeline ensures that re-running it will not result in duplicate records in the database.

## Requirements

To run this project, the following dependencies are required:

```txt
requests
pandas
SQLAlchemy
psycopg2
pytest
```

You can install the required Python libraries using:

```bash
pip install -r requirements.txt
```

## Services

The project includes two services: 
1. **PostgreSQL Database**: Stores weather data.
2. **Weather Data Pipeline**: Python app that extracts, transforms, loads, and queries the data.

The services are configured via `docker-compose.yml` and can be started together.

### Docker Setup

- **PostgreSQL**: A Dockerized PostgreSQL service to store the weather data.
- **Python App**: A Dockerized Python app to run the pipeline.

## Setup Instructions

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/loyolahernandez/DE_Assignment.git
   ```

2. **Run Services with Docker**:
   
   Ensure Docker is installed, then run the following to start both the PostgreSQL and the app containers:

   ```bash
   docker-compose up --build -d
   ```

   This will set up the PostgreSQL database and run the Python script to fetch, transform, and load weather data into the database.

3. **Running the Pipeline Manually**:

   If Docker is not preferred, you can run the Python script directly:

   ```bash
   python main.py
   ```

4. **Accessing the Database**:

   The PostgreSQL database will be accessible at `localhost:5432`. You can connect to it using any PostgreSQL client or via the command line once the containers are running:

   ```bash
   docker exec -it postgres_db psql -U myuser -d weather_db
   ```

## Project Structure

```bash
├── DE Take Home Assignment.pdf   # Original assignment instructions
├── Dockerfile                    # Dockerfile for the Python app
├── docker-compose.yml            # Docker Compose file to orchestrate services
├── main.py                       # Main script to run the ETL pipeline
├── src                           # Directory for utility functions
│   ├── __init__.py
│   └── utils.py                  # Utility functions for fetching, transforming, and loading data
├── queries                       # SQL queries for analysis
│   ├── temperature.sql           # SQL to compute average temperature
│   └── wind.sql                  # SQL to compute wind speed change
├── requirements.txt              # List of Python dependencies
├── tests                         # Directory for test files
│   ├── __init__.py
│   ├── test_main.py              # Unit tests for main.py
│   └── test_utils.py             # Unit tests for utils.py
└── weatherapi.ipynb              # Jupyter notebook for API exploration
```

## How the Pipeline Works

1. **Extract**: Fetches weather data from the `/stations` and `/observations` endpoints of the National Weather Service API.
   
2. **Transform**: Transforms the fetched data into the following format:
   - `Station ID`
   - `Station Name`
   - `Station Timezone`
   - `Latitude/Longitude`
   - `Observation Timestamp`
   - `Temperature`
   - `Wind Speed`
   - `Humidity`

3. **Load**: Loads the transformed data into a PostgreSQL database, ensuring that no duplicate records are inserted.

4. **Query**: Runs SQL queries to calculate:
   - The average observed temperature over the last week.
   - The maximum wind speed change between consecutive observations in the last 7 days.

## Testing

Unit tests are provided to validate the functionality of the main pipeline and its utility functions. Tests are written using `pytest` and can be run as follows:

```bash
pytest
```

## SQL Queries

Two SQL queries are provided in the `queries` directory:
- `temperature.sql`: Calculates the average temperature observed during the last 7 days.
- `wind.sql`: Computes the maximum wind speed change between consecutive observations over the past week.

## Assumptions

- The pipeline assumes that the API responses are well-formed and contain valid data.
- Data is fetched for the station ID `0128W` as a sample, but this can be changed within `main.py`.
- The database schema is created dynamically during the ETL process if it doesn't already exist.
