class SqlQueries:
    # CREATE TABLES

    staging_events_table_create = ("""
    create table if not exists staging_events (
    artist varchar,
    auth varchar,
    firstname varchar,
    gender  varchar,
    itemInSession  varchar,
    lastName  varchar,
    length  decimal,
    level  varchar,
    location varchar,
    method varchar,
    page varchar,
    registration  varchar,
    sessionId integer,
    song varchar,
    status  int,
    ts  bigint,
    userAgent  varchar,
    userId int
    )
    """)

    staging_songs_table_create = ("""
    drop table if exists staging_songs;
    create table  if not exists  staging_songs (
    num_songs integer,
    artist_id varchar,
    artist_latitude decimal(19,5),
    artist_longitude decimal(19,5),
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration decimal(19,5),
    year integer
    )
    """)

    songplay_table_create = ("""
        create table if not exists songplays
        (id varchar primary key not null,
        start_time timestamp not null,
        user_id int not null,
        level varchar,
        song_id varchar,
        artist_id varchar, 
        session_id int, 
        location varchar,
        user_agent varchar)
    """)

    user_table_create = ("""
        create table if not exists users
        (id int primary key not null,first_name varchar,last_name varchar,gender varchar,level varchar);
        """)

    song_table_create = ("""
    drop table if exists songs;
    create table songs (id varchar primary key,title varchar, artist_id varchar, year int, duration numeric);
    """)

    artist_table_create = ("""create table if not exists artists (id varchar primary key,name varchar,location varchar, latitude decimal,longitude decimal)

    """)

    time_table_create = ("""
        create table if not exists time
        (start_time timestamp primary key not null, hour int, day int, week_of_year int, month int, year int, weekday int)
    """)

    songplay_table_insert = ("""
        insert into songplays
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            INNER JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
       insert into users
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        and userid is not null
    """)

    song_table_insert = ("""
        insert into songs
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        insert into artists
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        insert into time
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
        where start_time is not null;
    """)

    quality_check_1 = ("""
        SELECT count(1) from songplays where user_id is null;
    """)
    create_verify = (
        '''
            SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_schema = 'dev'
   AND    table_name   in ( 'songplays','users','time','artists','songs'
   );
        '''
    )