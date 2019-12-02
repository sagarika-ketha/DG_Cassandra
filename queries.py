# CREATE TABLE QUERIES

# After analysing the data, it is clear that every entry can be uniquely identified by a combination of userId, sessionId and itemInSession as itemInSession is an incremental value of number of songs a user plays in a session

# Query 1 - Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
# The data is being queried by sessionId and itemInSession

query1_create_table = """
CREATE TABLE IF NOT EXISTS query1 (
artist text,
first_name text,
gender varchar,
itemInSession int,
last_name text,
length decimal,
level text,
location text,
sessionId int,
song text,
userId int,
primary key(sessionId, itemInSession, userId)
)
"""

# Query 2 - Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
# The data is being queried by userId and SessionId

query2_create_table = """
CREATE TABLE IF NOT EXISTS query2 (
artist text,
first_name text,
gender varchar,
itemInSession int,
last_name text,
length decimal,
level text,
location text,
sessionId int,
song text,
userId int,
primary key(userId, sessionId, itemInSession)
)
"""

# Query 3 - Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# The data is being queried by song name

query3_create_table = """
CREATE TABLE IF NOT EXISTS query3 (
artist text,
first_name text,
gender varchar,
itemInSession int,
last_name text,
length decimal,
level text,
location text,
sessionId int,
song text,
userId int,
primary key(song, userId, sessionId, itemInSession)
)
"""

# DROP QUERIES

drop_query1 = "DROP TABLE IF EXISTS query1"
drop_query2 = "DROP TABLE IF EXISTS query2"
drop_query3 = "DROP TABLE IF EXISTS query3"

# INSERT QUERIES

insert_query1 = "INSERT INTO query1 (artist, itemInSession, length, sessionId, song, userId) values (%s, %s, %s, %s, %s, %s)"
insert_query2 = "INSERT INTO query2 (artist, first_name, itemInSession, last_name, sessionId, song, userId) values (%s, %s, %s, %s, %s, %s, %s)"
insert_query3 = "INSERT INTO query3 (first_name, itemInSession, last_name, sessionId, song, userId) values (%s, %s, %s, %s, %s, %s)"


# SELECT QUERIES

select_query1 = "SELECT ARTIST, SONG, LENGTH from query1 where sessionId = %s and itemInSession = %s"
select_query2 = "SELECT artist, song, first_name, last_name from query2 where sessionid = %s and userid = %s"
select_query3 = "SELECT first_name, last_name from query3 where song = '%s'"


# LIST
create_table_queries = [query1_create_table, query2_create_table, query3_create_table]
drop_queries = [drop_query1, drop_query2, drop_query3]