import pandas as pd
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv
from queries import *


#To drop the tables
def drop_tables(session):
    for query in drop_queries:
        session.execute(query)

#To create tables
def create_tables(session):
    for query in create_table_queries:
        session.execute(query)

def main():
    #Creating a connection
    cluster = Cluster()
    session = cluster.connect()

    #Creating a keyspace
    session.execute("CREATE KEYSPACE IF NOT EXISTS EVENT_DATA WITH \
    REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.set_keyspace('event_data')

    #Drop tables
    drop_tables(session)

    #Create tables
    create_tables(session)

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()

csv_file_name = 'event_data_new.csv'
folder_name = 'event_data'