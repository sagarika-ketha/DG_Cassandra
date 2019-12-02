import pandas as pd
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv
from queries import *

def extract_data_to_csv(folder_name, csv_file_name):
    """
    Summary: Get the list of files in a specified folder and save the data to a csv file.
    Parameters:
        folder_name (String): Name of the folder.
        conn (Object): Database connection object.
        filepath (string): Path of the file.
        func (function): Function to be applied on the data.
    """

    # Go to event data folder and fetch all files paths
    filepath = os.getcwd() + '/'+ folder_name
    all_file_path_list = []
    for root, dirs, files in os.walk(filepath):
        all_file_path_list.extend(glob.glob(os.path.join(root,'*')))

    # Collect the data and store in a list
    full_data_rows_list = []
    for f in all_file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    # Inserting the data into event_data.csv
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open(csv_file_name, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')

        # Header
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

def insert_data(session, csv_file):
    """
    Summary: To insert data from csv file into three tables.
    Parameters:
    session (Object): Cassandra Session object.
    csv_file (String): Name of the csv file.
    """
    with open(csv_file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        # Skip header
        next(csvreader)

        for line in csvreader:
            # Query 1 insert
            session.execute(insert_query1, (line[0],  int(line[3]), float(line[5]), int(line[8]), line[9], int(line[10])))

            # Query 2 insert
            session.execute(insert_query2, (line[0],  line[1],  int(line[3]),  line[4], int(line[8]), line[9], int(line[10])))

            # Query 3 insert
            session.execute(insert_query3, (line[1], int(line[3]),  line[4],int(line[8]), line[9], int(line[10])))

#####Test queries#####

def execute_query_1(session, sessionId, itemInSession):
    op = session.execute(select_query1 % (sessionId, itemInSession))
    print("\nOutput of query 1")
    for row in op:
        print("{}, {}, {}".format(row.artist, row.song, row.length))

def execute_query_2(session, sessionId, userId):
    print("\nOutput of query 2")
    op = session.execute(select_query2 % (sessionId, userId))
    for row in op:
        print("{}, {}, {}, {}".format(row.artist, row.song, row.first_name, row.last_name))

def execute_query_3(session, song):
    print("\nOutput of query 3")
    op = session.execute(select_query3 % (song))
    for row in op:
        print("{}, {}".format(row.first_name, row.last_name))

#######################

def main():
    # Creating a connection
    cluster = Cluster()
    session = cluster.connect()

    # Use the keyspace
    session.set_keyspace('event_data')

    # Insert the data
    insert_data(session, 'event_data_new.csv')
    print("Data inserted")

    #### Execute Queries and display outputs####

    # Query 1
    execute_query_1(session, 338, 4)

    # Query 2
    execute_query_2(session, 182, 10)

    # Query 3
    execute_query_3(session, 'All Hands Against His Own')

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()