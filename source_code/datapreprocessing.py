from Project.source_code.mysqlUtilities import getRecords, insertRecord
import requests

from_count = 0
to_count = 0

# Laguardia Airport Co-ordinates
source_latitude_min = 40.7774
source_latitude_max = 40.7694
source_longitude_max = -73.8572
source_longitude_min = -73.8875

# API KEY
API_KEY = "Asui_QOxZdbG4g0U9i_XayOUyZAJrCyI6PXqD_RCdi-wKDRnT-y73DOZgBmymjJY"

# BING MAPS API
url = 'https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix?' \
      '$$$$' \
      + '&key=' + API_KEY + '&distanceUnit=mi'


def processInformation(row):
    #
    destination_latitude = row['dropoff_latitude']
    destination_longitude = row['dropoff_longitude']
    source_latitude = row['pickup_latitude']
    source_longitude = row['pickup_longitude']
    # source_longitude = -73.87476
    # source_latitude = 40.77394
    # destination_longitude = -73.88225
    # destination_latitude = 40.75431
    # Check if the entry is in the database.
    query = "select * from places where source_latitude=" + str(source_latitude) + ' AND source_longitude=' + str(
        source_longitude) + \
            ' AND destination_latitude=' + str(destination_latitude) + ' AND destination_longitude=' + str(
        destination_longitude)
    result = getRecords(query)
    if len(result) > 0:
        # Record already exists; retrieve the field
        distance = 0
        for _row in result:
            # get the distance
            distance = _row[4]
            # [TODO] compute shortest path and delay factor
    else:
        # The record does not exist; Query osm for shortest path for source and destination
        # sending get request and saving the response as response object
        params = str(source_longitude) + "," + str(source_latitude) + ";" + str(destination_longitude) + "," + str(
            destination_latitude) + '?overview=false'
        r = requests.get(url=url + params)

        # extracting data in json format
        data = r.json()
        distance_in_metres = data['routes'][0]['distance']
        # 1 metre = 0.006 miles
        distance_in_miles = distance_in_metres * 0.0006
        # Form the store query
        insert_record = "INSERT INTO places (source_latitude, source_longitude, destination_latitude," + \
                        "destination_longitude,distance) VALUES "
        record_tuple = "(" + str(source_latitude) + "," + str(source_longitude) + "," + str(
            destination_latitude) + "," + \
                       str(destination_longitude) + "," + str(distance_in_miles) + ")"

        insertRecord(insert_record + record_tuple)


# Function to calculate the distance for a given set of latitude and longitude values for source
# and destination and returns the distance and time. Throws exception otherwise.
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


try:
    # origins=47.6044,-122.3345;47.6731,-122.1185;47.6149,-122.1936&destinations=45.5347,-122.6231;47.4747,-122.2057
    # result = calculateDistance("47.6044", "-122.3345", "45.5347", "-122.6231")
    print("")
except Exception as err:
    print(err.args)
