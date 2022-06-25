import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stg_events (
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender TEXT,
        itemInSession INT,
        lastName TEXT,
        length FLOAT,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration BIGINT,
        sessionId INT,
        song TEXT,
        status INT,
        ts TIMESTAMP,
        userAgent TEXT,
        userId INT);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stg_songs (
        num_songs INT, 
        artist_id TEXT,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT,
        artist_name TEXT,
        song_id TEXT PRIMARY KEY,
        title TEXT,
        duration FLOAT, 
        year INT);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
        start_time TIMESTAMP NOT NULL, 
        user_id INT NOT NULL, 
        level TEXT, 
        song_id TEXT, 
        artist_id TEXT, 
        session_id TEXT, 
        location TEXT, 
        user_agent TEXT);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY, 
        first_name TEXT, 
        last_name TEXT, 
        gender TEXT, 
        level TEXT);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT PRIMARY KEY, 
        title TEXT NOT NULL, 
        artist_id TEXT, 
        year INT, 
        duration FLOAT NOT NULL);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT PRIMARY KEY, 
        name TEXT NOT NULL, 
        location TEXT, 
        latitude FLOAT, 
        longitude FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT,  
        month INT,
        year INT,
        weekday INT);
""")

# STAGING TABLES

staging_events_copy = ("""COPY stg_events
                          FROM '{logdata}'
                          IAM_ROLE '{iamrole}'
                          REGION 'us-west-2'
                          FORMAT AS JSON '{log_jsonpath}'
                          TIMEFORMAT AS 'epochmillisecs';
""").format(logdata=config['S3']['LOG_DATA'], iamrole = config['IAM_ROLE']['ARN'], log_jsonpath=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY stg_songs
                    FROM '{songdata}'
                    IAM_ROLE '{iamrole}'
                    REGION 'us-west-2'
                    FORMAT AS JSON 'auto';
""").format(songdata=config['S3']['SONG_DATA'], iamrole = config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        events.ts AS start_time, 
        events.userId AS user_id, 
        events.level, 
        songs.song_id, 
        songs.artist_id, 
        events.sessionId AS session_id, 
        events.location, 
        events.userAgent AS user_agent
    FROM
        stg_events AS events
    LEFT JOIN
        stg_songs AS songs
    ON events.song = songs.title AND events.artist = songs.artist_name
    WHERE
        events.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    )
    WITH most_recent_user_data AS
    (SELECT
        userId AS user_id,
        firstName AS first_name, 
        lastName AS last_name, 
        gender,
        level,
        ROW_NUMBER() OVER ( PARTITION BY userId ORDER BY ts DESC) AS rn 
    FROM
        stg_events AS events
    WHERE
        userId IS NOT NULL)
    SELECT user_id,
           first_name,
           last_name,
           gender,
           level
    FROM most_recent_user_data
    WHERE rn = 1
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    )
    SELECT
        DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM stg_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    SELECT
        DISTINCT(artist_id) AS artist_id,
        artist_name AS name,
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS longitude
    FROM stg_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT
        DISTINCT(start_time) AS start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week,
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(WEEKDAY FROM start_time) AS weekday
    FROM songplays      
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]