from mysql.connector import connect
import sys
import requests

connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing', user='root', password='rootroot', auth_plugin='mysql_native_password')
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
def checkIfRecordExists(query):
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
        connection = connect(host='locations.c1vvuhtpuoui.us-west-1.rds.amazonaws.com', database='ride_sharing', user='root', password='rootroot',auth_plugin='mysql_native_password')
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
        params = "origins=" + source_latitude1 + "," + source_longitude1  + \
                 "&destinations=" + destination_latitude1 + "," + destination_longitude1 + "&travelMode=driving"
        finalURL = url.replace("$$$$", params)
        response = requests.get(url=finalURL)
        if response.status_code==200:
            # extracting data in json format
            data = response.json()
            distanceArray = data['resourceSets'][0]['resources'][0]['results']
            return distanceArray[0]['travelDistance'], distanceArray[0]['travelDuration']
        else :
            return -1,-1
    except Exception as e:
        print("Exception occurred : " + str(e))
        raise e
     
    
# Returns nearest intersection near to the detination considering 1.5 mile radius
def getNearestIntersections(destLat:str, destLong:str, precisionLong:str):
    query = "SELECT latitude, longitude, (3959 * acos(cos(radians(" + str(destLat) + "))*cos(radians(latitude))*"+"cos(radians(longitude) - radians(" + str(destLong) + ")) + sin(radians(" + str(destLat) + ")) * sin(radians(latitude)))) AS distance FROM intersections HAVING distance < 1.50 ORDER BY distance LIMIT 0 , 1;"

    print(query)  
    try:    
        cursor = connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except Exception as e:
        print("Error while connecting to MySQL", e)
#     finally:
#         cursor.close()
#         connection.close()
#         print("MySQL connection is closed")


# Checks if there exists an intersection in the radius else it would consider the destinations as the new intersection
# and returns the destination itself as the new intersection.
# precisionLong is just placed as a place holder for now. 
def getMinDistanceIntersection(sourceLat:str, sourceLong:str, destLat:str, destLong:str, precisionLong):
        result = getNearestIntersections(destLat, destLong, precisionLong=0)
        if result is None:
            distance = str(calculateDistance(sourceLat,sourceLong,destLat,destLong))
            query = "insert into intersections(latitude, longitude, distance) values ("+destLat+","+destLong+","+distance+")"
            insertRecord(query)
            return (destLat, destLong) 
        else:
            if len(result)>0:
#                 print(result)
                return result
            else: 
                distance = str(calculateDistance(sourceLat,sourceLong,destLat,destLong))
                query = "insert into intersections(latitude, longitude, distance) values ("+destLat+","+destLong+","+distance+")"
                insertRecord(query)
                return (destLat, destLong)

                   
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