import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import pandas as pd

try:
    connection = mysql.connector.connect(user='root', password='rootroot',
                                         host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com',
                                         database='ride_sharing')
    mySql_insert_query = """insert into graph_plot2 
                            select t.pool_id, t.rideLabel, count(t.isRideShared), p.count_of_rides, p.pool_window
                            from trip_details as t ,
                            pool_details as p
                            where t.pool_id=p.pool_id
                            group by  t.pool_id
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
    cursor.close()

except mysql.connector.Error as error:
    print("Failed to insert record into Laptop table {}".format(error))

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")
