## Sparkify - Data Modeling with Postgres

### Purpose

The goal is to create a database named "sparkifydb" for Sparkify to analyze song play data. The data is stored in a star schema with a central fact table (songplays) and related dimension tables (users, songs, artists, and time). This database helps Sparkify run analytical queries to gain insights into user behavior and music trends.

### How to Run the Python Scripts

1. Create tables:  
Run the following to create the necessary tables:  
``!python create_tables.py``
2. Run ETL pipeline:
Run the following to load the data into the database:  
``!python etl.py``

### Files in the Repository

1. create_tables.py: Creates the necessary database tables. 
2. etl.py: Extracts, transforms, and loads data into the database. 
3. etl.ipynb: Step-by-step guide for processing individual files. 
4. sql_queries.py: Contains the SQL queries for interacting with the database. 
5. test.ipynb: Verifies that the data was loaded correctly by displaying the first few rows of the tables. It also includes sanity tests to check for common issues in the database schema, data loading process, and query results.

### Data Schema Design

The database uses a star schema with one fact table (songplays) and five dimension tables:

songplays: Stores information about song plays.
users: Contains user data.
songs: Contains song metadata.
artists: Contains artist metadata.
time: Breaks down timestamps into different time components.

This design optimizes query performance by separating descriptive data (dimensions) from transactional data (fact table).

### ETL Pipeline

The ETL pipeline extracts data from song and log JSON files, transforms it (e.g., converting timestamps, filtering data), and loads it into the appropriate tables. This allows Sparkify to analyze song plays, user demographics, and music trends efficiently.
