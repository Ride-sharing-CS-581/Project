import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import pandas as pd

try:
    connection = mysql.connector.connect(user='root', password='rootroot',
                              host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com',
                              database='ride_sharing')


    mysql_select="""SELECT pool_window, rideLabel, AVG(time_taken)
                    FROM pool_details
                    group by pool_window, rideLabel ;"""

    df = pd.read_sql(mysql_select,con=connection)
    print(df)

except mysql.connector.Error as error:
    print("Failed to insert record into Laptop table {}".format(error))

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")
