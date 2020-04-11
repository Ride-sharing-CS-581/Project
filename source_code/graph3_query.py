import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import pandas as pd
from ride_sharing.source_code.output_graphs.output_graph import *

try:

    F5=0
    F10=0
    T5=0
    T10=0
    connection = mysql.connector.connect(user='root', password='rootroot',
                              host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com',
                              database='ride_sharing')


    mysql_select="""SELECT pool_window, rideLabel, AVG(time_taken)
                    FROM pool_details
                    group by pool_window, rideLabel ;"""

    df = pd.read_sql(mysql_select,con=connection)
    print(df)

    for i in df.index:
        if (df['pool_window'][i] == 5):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F5=df['AVG(time_taken)'][i]
                print("F5",F5)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T5 = df['AVG(time_taken)'][i]
                print("T5", T5)
        if (df['pool_window'][i] == 5):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F10=df['AVG(time_taken)'][i]
                print("F10",F10)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T10 = df['AVG(time_taken)'][i]
                print("T10", T10)
    graph3(F5, F10, T5, T10)

except mysql.connector.Error as error:
    print("Failed to insert record into ride_sharing table {}".format(error))

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")

