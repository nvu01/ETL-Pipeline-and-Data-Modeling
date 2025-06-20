# Sparkify - Data Modeling with Postgres

This project involves the creation of an ETL pipeline and data modeling for a song play analysis. 
The goal is to design and implement a star schema in a PostgreSQL database, ingest data from song and log datasets (in JSON format), and process them using Python and SQL. 
This project focuses on creating fact and dimension tables that will be optimized for queries on song play data.

## Project Structure

### Data Sources

- Song Data:
  - A subset of real data from the Million Song Dataset.
  - The data is partitioned by the first three letters of the track ID.
  - Each file contains metadata about a song and its artist in JSON format. Example: song_data/A/B/C/TRABCEI128F424C983.json

- Log Data:
  - User event data in JSON format.
  - Partitioned by year and month. Example: log_data/2018/11/2018-11-12-events.json

### Tables:
- Fact Table:
    - `songplays`: Records of song plays from the log data, specifically when the "NextSong" page is visited.
    - Fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- Dimension Tables:
    - `users`: Information about users.
      - Fields: user_id, first_name, last_name, gender, level
    - `songs`: Metadata about songs.
      - Fields: song_id, title, artist_id, year, duration
    - `artists`: Metadata about artists. 
      - Fields: artist_id, name, location, latitude, longitude
    - `time`: Breakdown of timestamps in the songplays. 
      - Fields: start_time, hour, day, week, month, year, weekday

## Project Files

1. `create_tables.py`: Contains SQL DROP and CREATE statements to reset the database tables before each ETL run.
2. `etl.py`: automates the ETL process by reading and processing the song and log data files. 
3. `etl.ipynb`: A step-by-step guide for processing individual files. 
4. `sql_queries.py`: Contains the SQL queries for interacting with the database. 
5. `test.ipynb`: Verifies that the data was loaded correctly by displaying the first few rows of the tables. It also includes sanity tests to check for common issues in the database schema, data loading process, and query results.


## Steps to Run the Project

1. Set Up Database: Run the following to create the PostgreSQL tables:
    
   - In terminal: ```python create_tables.py```
   - In Jupyter Notebooks: ```!python create_tables.p```

2. Run ETL pipeline: 

    Follow the ETL process in `etl.ipynb` for step-by-step instructions on processing the song and log files.

    Alternatively, you can run the full ETL pipeline using `etl.py`.
   - In terminal: ```python etl.py```
   - In Jupyter Notebooks: ```!python etl.py```

3. Verify your data:
Run `test.ipynb` to check that the data has been correctly loaded into the database.


