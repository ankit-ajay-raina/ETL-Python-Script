# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 19:13:45 2020

@author: nkit0

Description: Yello Taxi Data ETL and Rolling Average Trip Time
"""

"""
    Importing required packages
"""
import sqlite3
from datetime import datetime

"""
 SQLite Database functions
"""

def create_database_tables(database_name):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    
    try:
        # Creating table for storing info about processed files
        c.execute('''CREATE TABLE IF NOT EXISTS FilesProcessed
                    (
                        ID INTEGER PRIMARY KEY AUTOINCREMENT,
                        FileName TEXT NOT NULL
                    )
                    ''')

        # Creating table for storing extracted data
        c.execute('''CREATE TABLE IF NOT EXISTS YellowTaxiTrip
                    (
                        ID INTEGER PRIMARY KEY AUTOINCREMENT,
                        PickupDateTime TEXT NOT NULL, 
                        DropDateTime TEXT NOT NULL,
                        TripTimeMinutes INTEGER NOT NULL
                    )
                    ''')

         # Commiting the changes
        conn.commit()
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst) 
    finally:
        conn.close()
        
        
def get_processed_files(database_name):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    
    result = []
    
    # Fetching files which have already been processed
    try:
        c.execute('SELECT FileName FROM FilesProcessed')
        result = c.fetchall()
        result = [e for l in result for e in l]
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst) 
    finally:
        conn.close()
    
    return result


def insert_data_in_database(database_name, insert_query, data):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    
    try:
        c.executemany(insert_query, data)
        conn.commit()
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst) 
    finally:
        conn.close()
        
        
"""
    Function to get files in a directory
"""
def get_directory_file_names(directory_path):
    from os import listdir
    from os.path import isfile, join
    
    file_names = [f for f in listdir(directory_path) if isfile(join(directory_path, f))]
    return file_names

"""
    Function to read data from a file
"""
def read_data_from_file(file_path):
    
    data = []
    
    with open(file_path, mode='r') as file:
        # Skip the header
        next(file)
        
        for line in file:
            line_split = line.split(',')
            
            try:
                pick_up_time = datetime.strptime(line_split[1],'%Y-%m-%d %H:%M:%S')
            except ValueError:
                pick_up_time = datetime.strptime(line_split[1],'%m/%d/%Y %H:%M')
            
            try:
                drop_time = datetime.strptime(line_split[2],'%Y-%m-%d %H:%M:%S')
            except ValueError:
                drop_time = datetime.strptime(line_split[2],'%m/%d/%Y %H:%M')

            
            # Calculating Trip Time in minutes
            trip_time_delta = (drop_time - pick_up_time)
            trip_time_minutes = trip_time_delta.seconds/60
            data.append((pick_up_time, drop_time, trip_time_minutes)) # Fetching Pick Up and Drop times
    
    return data

"""
    ETL
"""

"""
    Function to extract data from Yellow Taxi files
"""
def extract_transform_data(directory_path, database_name):
    trip_data = []
    
    # Getting names of files in the given directory
    file_names = get_directory_file_names(directory_path)
    processed_files = get_processed_files(database_name)
    
    if processed_files != None:
        file_names = list(set(file_names) - set(processed_files))
        
        new_files_processed = [(file_name, ) for file_name in file_names]
        
    for file_name in file_names:
        file_path = directory_path + "/" + file_name
        file_data = read_data_from_file(file_path)
        trip_data.extend(file_data)
    
    return new_files_processed, trip_data


"""
    Function to load data into SQLite
"""
def load_data(database_name, processed_file_data, trip_data):
    # Inserting Info about processed files
    insert_query = f'INSERT INTO FilesProcessed (FileName) VALUES (?)'
    insert_data_in_database(database_name, insert_query, processed_file_data)
    
    # Inserting Info about Trips
    insert_query = f'INSERT INTO YellowTaxiTrip (PickupDateTime, DropDateTime, TripTimeMinutes) VALUES (?,?,?)'
    insert_data_in_database(database_name, insert_query, trip_data)


"""
   Function to calculate n-day rolling average trip time 
"""
def get_rolling_average_trip_time(database_name, rolling_average_days):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    
    # Fetching files which have already been processed
    try:
        c.execute(f"SELECT avg(TripTimeMinutes) FROM YellowTaxiTrip WHERE DropDateTime >= date('now', '-{rolling_average_days} day') AND DropDateTime <= date('now')")
        result,  = c.fetchone()
        
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst) 
    finally:
        conn.close()
    
    return result

"""
    Main function
"""
if __name__ == "__main__":
    database_name = 'yellow_taxi_trip.db'
    directory_path = 'Data'
    create_database_tables(database_name)
    # Extracting and transforming data
    file_names, trip_data = extract_transform_data(directory_path, database_name)
    # Loading Data
    load_data(database_name, file_names, trip_data)
    # 45-day rolling average trip time
    rolling_average_days = 45
    avg_trip_time = get_rolling_average_trip_time(database_name, rolling_average_days)
    
    if avg_trip_time == None:
        avg_trip_time = 0
    
    print("45 days rolling average trip time is {:.2f} minutes.".format(avg_trip_time))