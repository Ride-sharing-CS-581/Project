# from source_code.mysqlUtilities import getRecords, insertRecord
import requests

# API
url = 'http://127.0.0.1:5000/route/v1/driving/'
#url = 'http://router.project-osrm.org/route/v1/driving/'


# Function to calculate the distance for a given set of latitude and longitude values for source
# and destination and returns the distance and time. Throws exception otherwise.
def calculateDistance(source_latitude: str, source_longitude: str, destination_latitude: str,
                      destination_longitude: str):
    try:
        # Check if the parameters are empty
        if source_latitude == "" or source_longitude == "" or destination_latitude == "" or destination_longitude == "":
            raise Exception("At least 1 argument is empty")
        #print("Coordinates are "+source_latitude+" "+source_longitude+" "+destination_latitude+" "+destination_longitude)
        params = (source_longitude) + "," + (source_latitude) + ";" + (destination_longitude) + "," + (
            destination_latitude) + '?overview=false'
        r = requests.get(url=url + params)
        if r.status_code == 200:
            # extracting data in json format
            data = r.json()
            #print('Data ' + str(data))
            #print(source_latitude + ' ' + source_longitude + ' ' + destination_latitude + ' ' + destination_longitude)
            if len(data['routes']) == 0:
                return -1, -1
            distance_in_metres = data['routes'][0]['distance']
            # 1 metre = 0.006 miles. Convert the api distance from meters to miles
            distance_in_miles = distance_in_metres * 0.0006
            return distance_in_miles, data['routes'][0]['duration']
        else:
            print("No API response. Status is " + str(r.status_code))
            print(source_latitude + ' ' + source_longitude + ' ' + destination_latitude + ' ' + destination_longitude)
            return -1, -1
    except Exception as e:
        print("Exception occurred : " + str(e))
        print(source_latitude + ' ' + source_longitude + ' ' + destination_latitude + ' ' + destination_longitude)
        raise e

