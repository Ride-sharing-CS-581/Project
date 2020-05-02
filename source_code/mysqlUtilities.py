from math import atan2, radians, degrees, sin, cos
from Project.source_code.datapreprocessing import calculateDistance

from mysql.connector import connect
import sys
import requests

connection = connect(host='localhost', database='ride_sharing', user='root',
                     password='root',
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
        connection = connect(host='localhost', database='ride_sharing',
                             user='root', password='root', auth_plugin='mysql_native_password')
        cursor = connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return records, cursor.column_names
    except Exception as e:
        print("Error while connecting to MySQL", e)
    finally:
        cursor.close()
        connection.close()
        # print("MySQL connection is closed")


# Function to insert a record in to the database table
def insertRecord(query):
    try:
        connection = connect(host='localhost', database='ride_sharing',
                             user='root', password='root', auth_plugin='mysql_native_password')
        cursor = connection.cursor(prepared=True)
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Error while connecting to MySQL", e)
    finally:
        cursor.close()
        connection.close()
        # print("MySQL connection is closed")


# Returns nearest intersection near to the destination considering 0.18 mile radius
def getNearestIntersections(destLat: str, destLong: str):
    query = "SELECT latitude, longitude,intersections.distance, " \
            "(3959 * acos(cos(radians(" + \
            str(destLat) + "))*cos(radians(latitude))*" + \
            "cos(radians(longitude) - radians(" + str(destLong) + ")) + sin(radians(" + \
            str(
                destLat) + ")) * sin(radians(latitude)))) AS distance FROM intersections HAVING " \
                           "distance < 1 ORDER " \
                           "BY intersections.distance LIMIT 0,1; "
    # print(query)
    try:
        connection = connect(host='localhost', database='ride_sharing',
                             user='root', password='root', auth_plugin='mysql_native_password')
        cursor = connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except Exception as e:
        print("Error while connecting to MySQL", e)

    finally:
        cursor.close()
        connection.close()
        # print("MySQL connection is closed")


# # function to compute bearing angle in the direction of source and destination and compute an intersection point
# # within 0.18 miles of the source
# def findNewIntersectionPoint(source_lat, source_long, destination_lat, destination_long):
#     from math import radians, cos, asin, sin, atan2, degrees
#     print("Finding new intersections between source  {} {} and  destination {} {}".format(source_lat, source_long,
#                                                                                           destination_lat,
#                                                                                           destination_long))
#     lat2 = radians(source_lat)
#     lat1 = radians(destination_lat)
#     lon1 = radians(destination_long)
#     lon2 = radians(source_long)
#     bearing = atan2(sin(lon2 - lon1) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lon2 - lon1))
#     bearing = degrees(bearing)
#     bearing = (bearing + 360) % 360
#     print(bearing)
#     R = 3959
#     latB = asin(sin(lat1) * cos(1 / R) + cos(lat1) * sin(1 / R) * cos(radians(bearing)))
#     lonB = lon1 + atan2(sin(radians(bearing)) * sin(1 / R) * cos(lat1), cos(1 / R) - sin(lat1) * sin(latB))
#     return degrees(latB), degrees(lonB)


# function to compute bearing angle in the direction of source and destination and compute an intersection point
# within 0.18 miles of the source
def findNewIntersectionPoint(source_lat, source_long, destination_lat, destination_long):
    lat2 = radians(source_lat)
    lat1 = radians(destination_lat)
    lon1 = radians(destination_long)
    lon2 = radians(source_long)
    bearing = atan2(sin(lon2 - lon1) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lon2 - lon1))
    bearing = degrees(bearing)
    bearing = (bearing + 360) % 360

    nearest_api = "http://localhost:5000/nearest/v1/driving/" + str(destination_long) + "," + str(destination_lat)
    +"?number=1" + "&bearings=" + bearing + "," + bearing

    r = requests.get(url=nearest_api)
    if r.status_code == 200:
        # extracting data in json format
        data = r.json()
        # print('Data ' + str(data))
        location = data['waypoints'][0]['location']
        return location[1], location[0]
    else:
        return -1, 1


# Checks if there exists an intersection in the radius else it would consider the destinations as the new intersection
# and returns the destination itself as the new intersection.
# precisionLong is just placed as a place holder for now.
def getMinDistanceIntersection(sourceLat: str, sourceLong: str, destLat: str, destLong: str, origin,
                               distance_from_laguardia):
    if origin == "To Laguardia":
        # source is the pickup points and destination is laguardia airport
        instance = sourceLat, sourceLong
        sourceLat, sourceLong = destLat, destLong
        # for to laguardia case, destination is Laguardia airport. Swap the destination with source at different origins
        destLat, destLong = instance
        # After swap, source is to Laguardia and dest is pickup points
    result = getNearestIntersections(destLat, destLong)

    if result is None or len(result) == 0:
        # print("Inserting current points as intersection")

        distance = distance_from_laguardia
        if distance > 0:
            query = "insert into intersections(latitude, longitude, distance) values (" + destLat + "," + destLong + "," + \
                    str(distance) + ")"
            insertRecord(query)
        return destLat, destLong, distance
    else:
        return result[0][0], result[0][1], result[0][2]
