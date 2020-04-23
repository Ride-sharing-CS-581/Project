import mysql.connector

import pandas as pd
from Project.source_code.output_graph import *

try:

    F5 = 0
    F10 = 0
    T5 = 0
    T10 = 0
    connection = mysql.connector.connect(user='root', password='root',
                                         host='localhost',
                                         database='ride_sharing')

    cursor = connection.cursor()

    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into ride_sharing table")

    mysql_select = """  select pool_window,rideLabel,avg(dist_saved)/avg(initial_trips_distance) * 100 as saved from 
pool_details group by rideLabel, pool_window
 """

    df = pd.read_sql(mysql_select, con=connection)
    print(df)
    cursor.close()

    for i in df:
        print(i)

    for i in df.index:

        if (df['pool_window'][i] == 5):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F5 = df['saved'][i]
                print("F5", F5)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T5 = df['saved'][i]
                print("T5", T5)
        if (df['pool_window'][i] == 10):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F10 = df['saved'][i]
                print("F10", F10)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T10 = df['saved'][i]
                print("T10", T10)
    graph1(F5, F10, T5, T10)



except mysql.connector.Error as error:
    print("Failed to insert record into Laptop table {}".format(error))

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")
