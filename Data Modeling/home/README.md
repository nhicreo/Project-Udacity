# Project: Data Modeling with Cassandra

# Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.
# Datasets
For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
+ event_data/2018-11-08-events.csv
+ event_data/2018-11-09-events.csv

+ Session information: timestamp, user activity (song plays, etc.), session duration
+ User information: user ID, first name, last name, gender, location
+ Song information: song title, artist, length

## 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 

query = "create table if not exists artist_song_length_song"
query = query + "(sessionId int, itemInSession int, artist text, song_length float, song text, PRIMARY KEY (sessionId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)


## 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

query = "select artist, song, song_length from artist_song_length_song where sessionId = 338 and itemInSession = 4"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist, row.song, row.song_length)   
## 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = "create table if not exists firstname_lastname"
query = query + "(song text, userId int,firstname text, lastname text, PRIMARY KEY (song, userId))"
try:
    session.execute(query)
except Exception as e:
    print(e)
# Build ETL Pipeline
Test by running SELECT statements after running the queries on database