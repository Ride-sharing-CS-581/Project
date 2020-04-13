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
    connection = mysql.connector.connect(user='root', password='root',
                                         host='localhost',
                                         database='ride_sharing')
    mySql_insert_query = """insert into graph_plot2 
                            select t.pool_id, t.rideLabel, count(t.isRideShared), p.count_of_rides, p.pool_window
                            from trip_details as t ,
                            pool_details as p
                            where t.pool_id=p.pool_id
                            group by  t.pool_id,t.rideLabel
                            order by t.pool_id  """

    cursor = connection.cursor()
    cursor.execute(mySql_insert_query)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into ride_sharing table")

    mysql_select = """select g.pool_window,g.rideLabel, avg ((( g.actual_rides-g.shared_rides) / g.actual_rides ) * 100)
                        from graph_plot2 as g
                        group by pool_window, rideLabel """

    df = pd.read_sql(mysql_select, con=connection)
    print(df)

    for i in df.index:
        if (df['pool_window'][i] == '5'):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F5 = df['avg ((( g.actual_rides-g.shared_rides) / g.actual_rides ) * 100)'][i]
                print("F5", F5)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T5 = df['avg ((( g.actual_rides-g.shared_rides) / g.actual_rides ) * 100)'][i]

                print("T5", T5)
        if (df['pool_window'][i] == '10'):
            if (df['rideLabel'][i] == "From LaGuardia"):
                F10 = df['avg ((( g.actual_rides-g.shared_rides) / g.actual_rides ) * 100)'][i]
                print("F10", F10)
            if (df['rideLabel'][i] == "To LaGuardia"):
                T10 = df['avg ((( g.actual_rides-g.shared_rides) / g.actual_rides ) * 100)'][i]
                print("T10", T10)
        print( df['avg ((( g.actual_rides-g.shared_rides) / g.actual_rides ) * 100)'][i])
    graph2(F5, F10, T5, T10)
    cursor.close()

except mysql.connector.Error as error:
    print("Failed to insert record into Laptop table {}".format(error))

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")
