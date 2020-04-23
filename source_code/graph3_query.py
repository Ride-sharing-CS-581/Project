import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import pandas as pd
from Project.source_code.output_graph import *

try:

    F5=0
    F10=0
    T5=0
    T10=0
    connection = connection = mysql.connector.connect(user='root', password='root',
                                                      host='localhost',
                                                      database='ride_sharing')


    mysql_select="""select pool_window, rideLabel, avg(time_taken)
                    from pool_details
                    group by pool_window, rideLabel"""

    df = pd.read_sql(mysql_select,con=connection)
    print(df)

    for i in df.index:
        #print(df['AVG(time_taken)'][i])
        if (df['pool_window'][i] == 5):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F5=df['avg(time_taken)'][i]
                print("F5",F5)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T5 = df['avg(time_taken)'][i]
                print("T5", T5)
        if (df['pool_window'][i] == 10):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F10=df['avg(time_taken)'][i]
                print("F10",F10)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T10 = df['avg(time_taken)'][i]
                print("T10", T10)
    graph3(F5, F10, T5, T10)

except mysql.connector.Error as error:
    print("Failed to insert record into ride_sharing table {}".format(error))

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")