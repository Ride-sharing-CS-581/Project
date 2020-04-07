from mysql.connector import connect
import sys

connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing', user='root', password='rootroot',
                     auth_plugin='mysql_native_password')
print('Attempting to connect to the database...')
if connection.is_connected():
    print("Connection to the database established successfully")
else:
    print("Connection not succesful. Terminating program.")
    sys.exit()


# Function to check if a record exists in the database
def getRecords(query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except Exception as e:
        print("Error while connecting to MySQL", e)
    finally:
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


# Function to insert a record in to the database table
def insertRecord(query):
    try:
        connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing', user='root', password='rootroot',
                             auth_plugin='mysql_native_password')
        cursor = connection.cursor(prepared=True)
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Error while connecting to MySQL", e)
    finally:
        cursor.close()
        connection.close()
        print("MySQL connection is closed")