import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
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
    mySql_insert_query = """insert into graph_plot1
                            select t.pool_id, t.rideLabel, sum(r.dist_airport),p.dist_saved, p.pool_window
                            from taxitrips as r,
                            trip_details_v2 as t,
                            pool_details_v2 as p
                            where  t.rideT_id=r.RideID and t.pool_id= p.pool_id
                            group by  t.pool_id
                            order by t.pool_id  """

    cursor = connection.cursor()
    cursor.execute(mySql_insert_query)
    connection.commit()
    print(cursor.rowcount, "Record inserted successfully into ride_sharing table")

    mysql_select = """select pool_window, ride_label,avg ((g.shared_dist / g.actual_dist ) * 100)
                    from graph_plot1 as g
                    group by pool_window, ride_label """

    df = pd.read_sql(mysql_select, con=connection)
    print(df)
    cursor.close()

    for i in df:
        print(i)

    for i in df.index:
        if (df['pool_window'][i] == '5'):
            if (df['ride_label'][i] == "From LaGuardia"):
                F5 = df['avg ((g.shared_dist / g.actual_dist ) * 100)'][i]
                print("F5", F5)
            if (df['ride_label'][i] == "To LaGuardia"):
                T5 = df['avg ((g.shared_dist / g.actual_dist ) * 100)'][i]
                print("T5", T5)
        if (df['pool_window'][i] == '10'):
            if (df['ride_label'][i] == "From LaGuardia"):
                F10 = df['avg ((g.shared_dist / g.actual_dist ) * 100)'][i]
                print("F10", F10)
            if (df['ride_label'][i] == "To LaGuardia"):
                T10 = df['avg ((g.shared_dist / g.actual_dist ) * 100)'][i]
                print("T10", T10)
    graph1(F5, F10, T5, T10)



except mysql.connector.Error as error:
    print("Failed to insert record into Laptop table {}".format(error))

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")
