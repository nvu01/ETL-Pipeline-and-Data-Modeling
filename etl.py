import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a single song file and loads relevant data into the songs and artists tables.
    
    1. Load the content of the song JSON file from filepath to a dataframe.
    
    2. Insert record into songs table:
    - Use df.values to select values from the dataframe.
    - Use .tolist to convert the array to a list and set it to song_data.
    - Implement the song_table_insert query in sql_queries.py.
    
    3. Insert record into artists table:
    - Use df.values to select values from the dataframe.
    - Use .tolist to convert the array to a list and set it to artist_data.
    - Implement the artist_table_insert query in sql_queries.py.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes a single log file and loads relevant data into the time, users, and songplays tables.
    
    1. Load the content of the log JSON file from filepath to a dataframe.
    
    2. Filter the dataframe by 'NextSong' under 'page' column.
    
    3. Convert timestamp column to datetime.
    
    4. Insert record into time table:
    - Extract timestamp, hour, day, week of year, month, year, and weekday from the ts column and set to time_data.
    - Specify labels for these columns and set to column_labels.
    - Combine time_data and column_labels and set to time_df dataframe.
    - Implement the time_table_insert query in sql_queries.py.
    
    5. Insert record into users table:
    - Extract data for users table and set to user_df.
    - Implement the user_table_insert query in sql_queries.py.
    
    6. Insert record into songplays table:
    - Retrieve songid and artistid from songs and artist table based on song, artist and length in df
    - Add songid, artistid and columns from df to songplay_data
    - Implement the songplay_table_insert query in sql_queries.py.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterates through all files in a given directory and applies a specified processing function to each file.
    
    1. Finds all JSON files in the specified directory (filepath).
    2. Iterates through each file, applying the specified processing function (process_song_file or process_log_file) to load the data into the database.
    3. Commits the transaction after processing each file.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main entry point of the ETL pipeline script.
    
    1. Establishes connection to the PostgreSQL database using psycopg2.
    2. Calls 'process_data' to process all files in the 'song_data' and 'log_data' directories.
    3. Closes the database connection after all data has been processed.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()