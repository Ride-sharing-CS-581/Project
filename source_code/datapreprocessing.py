from Project.source_code.mysqlUtilities import getRecords, insertRecord
import requests

from_count = 0
to_count = 0

# API KEY
API_KEY = "Asui_QOxZdbG4g0U9i_XayOUyZAJrCyI6PXqD_RCdi-wKDRnT-y73DOZgBmymjJY"

# BING MAPS API
url = 'https://dev.virtualearth.net/REST/v1/Routes/DistanceMatrix?' \
      '$$$$' \
      + '&key=' + API_KEY + '&distanceUnit=mi%timeUnit=second'


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
