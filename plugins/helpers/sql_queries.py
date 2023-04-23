class SqlQueries:

    staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR,
            itemInSession INTEGER,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            sessionId int,
            song VARCHAR,
            status INTEGER,
            ts BIGINT,
            userAgent VARCHAR,
            userId INTEGER
        );
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs INTEGER,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration FLOAT,
            year INTEGER
        );
    """)    

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id CHAR(32) PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id INT NOT NULL,
            level VARCHAR(10),
            song_id VARCHAR(18),
            artist_id VARCHAR(18),
            session_id VARCHAR(32),
            location VARCHAR(100),
            user_agent VARCHAR(200),
            FOREIGN KEY (start_time) REFERENCES time(start_time),
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (song_id) REFERENCES songs(song_id),
            FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
        )
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender CHAR(1),
            level VARCHAR(10)
        )
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR(18) PRIMARY KEY,
            title VARCHAR,
            artist_id VARCHAR(18),
            year INT,
            duration FLOAT,
            FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
        )
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR(18) PRIMARY KEY,
            name VARCHAR,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        )
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        )
    """)

    # INSERT RECORDS

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionId || events.start_time) songplay_id,
                events.start_time, 
                events.userId, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionId, 
                events.location, 
                events.userAgent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
