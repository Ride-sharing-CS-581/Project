import pandas as pd

from Project.source_code.mysqlUtilities import checkIfRecordExists, insertRecord
import requests

from_count = 0
to_count = 0

source_latitude_min = 40.7713
source_latitude_max = 40.7748
source_longitude_max = -73.8681
source_longitude_min = -73.8772

# API
url = 'http://router.project-osrm.org/route/v1/driving/'

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
    result = checkIfRecordExists(query)
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

"""
for i in range(1):
    # file_name = 'G:/Advanced dbms/data/yellow_tripdata_2016-01' + '.csv'
    file_name = 'data/yellow_tripdata_2016-12' + str(1) + '.csv'
    df = pd.read_csv(file_name)

    # from the airport

    fromT = df.loc[
        (df['pickup_longitude'].between(source_longitude_min, source_longitude_max)) & (
            df['pickup_latitude'].between(source_latitude_min, source_latitude_max))]

    # to the airport

    to = df.loc[
        (df['dropoff_longitude'].between(-73.8772, -73.8681)) & (df['dropoff_latitude'].between(40.7713, 40.7748))]
    from_count += len(fromT)
    to_count += len(to)

    for index, row in fromT.iterrows():
        processInformation(row)

    for index, row in to.iterrows():
        processInformation(row)
"""
#Function to calculate the distance for a given set of latitude and longitude values for source
#and destination and returns the distance, rounded to 5 decimal places. Throws exception otherwise.
def calculateDistance(source_latitude: str, source_longitude: str, destination_latitude: str,
                      destination_longitude: str) -> float:
    try:
        # Check if the parameters are empty
        if source_latitude == "" or source_longitude == "" or destination_latitude == "" or destination_longitude == "":
            raise Exception("At least 1 argument is empty")

        params = (source_longitude) + "," + (source_latitude) + ";" + (destination_longitude) + "," + (
            destination_latitude) + '?overview=false'
        r = requests.get(url=url + params)
        # extracting data in json format
        data = r.json()
        distance_in_metres = data['routes'][0]['distance']
        # 1 metre = 0.006 miles. Convert the api distance from meters to miles
        distance_in_miles = distance_in_metres * 0.0006
        return round(distance_in_miles,5)
    except Exception as e:
        print("Exception occurred : " + str(e))
        raise e

try:
    result= calculateDistance("40.77394","-73.87476","40.75431","-73.88225")
    print(result)
except Exception as err:
    print(err.args)
