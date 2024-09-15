import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
    event_id INT IDENTITY(0,1),
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender  VARCHAR(1),
    item_in_session	INTEGER,
    user_last_name VARCHAR(255),
    song_length	DOUBLE PRECISION, 
    level VARCHAR(50),
    location VARCHAR(255),	
    method VARCHAR(25),
    page VARCHAR(35),	
    registration VARCHAR(50),	
    session_id	BIGINT,
    song_title VARCHAR(255),
    status INTEGER, 
    ts VARCHAR(50),
    user_agent TEXT,	
    user_id VARCHAR(100),
    PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""
create table if not exists staging_songs (
    song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id))
""")

songplay_table_create = ("""
create table if not exists songplays(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP,
    user_id VARCHAR(50),
    level VARCHAR(50),
    song_id VARCHAR(100),
    artist_id VARCHAR(100),
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT,
    PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
create table if not exists users(
    user_id VARCHAR,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(50),
    PRIMARY KEY (user_id))
""")

song_table_create = ("""
create table if not exists songs(
    song_id VARCHAR(100),
    title VARCHAR(255),
    artist_id VARCHAR(100) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")

artist_table_create = ("""
create table if not exists artists(
    artist_id VARCHAR(100),
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
""")

time_table_create = ("""
create table if not exists time(
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}' 
FORMAT AS JSON {} REGION 'us-east-1';
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}' 
FORMAT AS JSON 'auto' REGION 'us-east-1';
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'])

# FINAL TABLES 

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select distinct timestamp 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time, 
        e.user_id, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.session_id, 
        e.location, 
        e.user_agent
    from staging_events e, staging_songs  s
    where e.song_title  = s.title 
    and e.artist_name  = s.artist_name 
    and e.song_length  = s.duration 
    and e.page = 'NextSong'
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
    select distinct (user_id),
        user_first_name, 
        user_last_name, 
        user_gender, 
        level
    from staging_events
    where user_id is not null
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) 
    select distinct (song_id),
        title,
        artist_id,
        year,
        duration
    from staging_songs
    where song_id is not null
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
    select distinct (artist_id),
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from staging_songs
    where artist_id is not null
""")

time_table_insert = ("""
insert into time 
        with temp_time as (select timestamp 'epoch' + (ts/1000 * INTERVAL '1 second') as ts from staging_events)
        select distinct
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        from temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
