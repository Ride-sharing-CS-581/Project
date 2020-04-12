from math import atan2

from mysql.connector import connect
import sys
import requests

connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing', user='root',
                     password='rootroot',
                     auth_plugin='mysql_native_password')

print('Attempting to connect to the database...')
if connection.is_connected():
    print("Connection to the database established successfully")
else:
    print("Connection not succesful. Terminating program.")
    sys.exit()

# API KEY
API_KEY = "Asui_QOxZdbG4g0U9i_XayOUyZAJrCyI6PXqD_RCdi-wKDRnT-y73DOZgBmymjJY"

# BING MAPS API
url = 'https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix?' \
      '$$$$' \
      + '&key=' + API_KEY + '&distanceUnit=mi'


# Function to check if a record exists in the database
def getRecords(query):
    try:
        connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing',
                             user='root', password='rootroot', auth_plugin='mysql_native_password')
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
        connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing',
                             user='root', password='rootroot', auth_plugin='mysql_native_password')
        cursor = connection.cursor(prepared=True)
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Error while connecting to MySQL", e)
    finally:
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


# Function to calculate the distance for a given set of latitude and longitude values for source
# and destination and returns the distance, rounded to 5 decimal places. Throws exception otherwise.
def calculateDistance(source_latitude1: str, source_longitude1: str,
                      destination_latitude1: str, destination_longitude1: str) -> float:
    try:
        # Check if the parameters are empty
        if source_latitude1 == "" or source_longitude1 == "" or destination_latitude1 == "" or destination_longitude1 == "":
            raise Exception("At least 1 argument is empty")
        params = "origins=" + source_latitude1 + "," + source_longitude1 + \
                 "&destinations=" + destination_latitude1 + "," + destination_longitude1 + "&travelMode=driving"
        finalURL = url.replace("$$$$", params)
        response = requests.get(url=finalURL)
        if response.status_code == 200:
            # extracting data in json format
            data = response.json()
            distanceArray = data['resourceSets'][0]['resources'][0]['results']
            return distanceArray[0]['travelDistance'], distanceArray[0]['travelDuration']
        else:
            return -1, -1
    except Exception as e:
        print("Exception occurred : " + str(e))
        raise e


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
    print(query)
    try:
        connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing',
                             user='root', password='rootroot', auth_plugin='mysql_native_password')
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


# function to compute bearing angle in the direction of source and destination and compute an intersection point
# within 0.18 miles of the source
def findNewIntersectionPoint(source_lat, source_long, destination_lat, destination_long):
    from math import radians, cos, asin, sin, atan2, degrees
    lat2 = radians(source_lat)
    lat1 = radians(destination_lat)
    lon1 = radians(destination_long)
    lon2 = radians(source_long)
    bearing = atan2(sin(lon2 - lon1) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lon2 - lon1))
    bearing = degrees(bearing)
    bearing = (bearing + 360) % 360
    print(bearing)
    R = 3959
    latB = asin(sin(lat1) * cos(0.18 / R) + cos(lat1) * sin(0.18 / R) * cos(radians(bearing)))
    lonB = lon1 + atan2(sin(radians(bearing)) * sin(0.18 / R) * cos(lat1), cos(0.18 / R) - sin(lat1) * sin(latB))
    return degrees(latB), degrees(lonB)


# Checks if there exists an intersection in the radius else it would consider the destinations as the new intersection
# and returns the destination itself as the new intersection.
# precisionLong is just placed as a place holder for now.
def getMinDistanceIntersection(sourceLat: str, sourceLong: str, destLat: str, destLong: str, origin):
    if origin == "To Laguardia":
        instance = sourceLat, sourceLong
        sourceLat, sourceLong = destLat, destLong
        # for to laguardia case, destination is Laguardia airport. Swap the destination with source at different origins
        destLat, destLong = instance

    result = getNearestIntersections(destLat, destLong)
    if result is None:

        destLat, destLong = findNewIntersectionPoint(float(sourceLat), float(sourceLong), float(destLat),
                                                     float(destLong))
        destLat = str(destLat)
        destLong = str(destLong)
        distance,time = calculateDistance(sourceLat, sourceLong, str(destLat), str(destLong))
        query = "insert into intersections(latitude, longitude, distance) values (" + destLat + "," + destLong + "," + \
                str(distance) + ")"
        insertRecord(query)
        return destLat, destLong, distance
    else:
        if len(result) > 0:
            #                 print(result)
            return result[0][0], result[0][1], result[0][2]
        else:

            destLat, destLong = findNewIntersectionPoint(float(sourceLat), float(sourceLong), float(destLat),
                                                         float(destLong))
            distance, time = calculateDistance(sourceLat, sourceLong, str(destLat), str(destLong))
            query = "insert into intersections (latitude, longitude, distance) values (" + str(
                destLat) + "," + str(destLong) + "," + str(distance) + ")"
            insertRecord(query)
            return destLat, destLong, distance

# code which was written before. Donot delete

#         if result is None:
#             precisionLong=10*precisionLong
#             result = getNearestIntersections(destLat, destLong, precisionLong)
#         else:
#             if len(result)>0:
#                 destToIntersectionsList = []
#                 for i in range(len(result)):
#                     destToIntersectionsList.append(calculateDistance(str(destLat),str(destLong),str(result[i][0]),str(result[i][1])))
#                 #     returns the lat and long tuple of minimum distance in the list
#                 return (result[destToIntersectionsList.index(min(destToIntersectionsList))])
#             else:                
#                 precisionLong=10*precisionLong
#                 result = getNearestIntersections(destLat, destLong, precisionLong)
#                 if len(result)>0:
#                     print("------------")
#                     print(precisionLong)
#                     print("Got result")
