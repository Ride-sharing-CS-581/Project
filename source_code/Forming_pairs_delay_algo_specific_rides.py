import logging
import threading
import time
from datetime import datetime, timedelta
from math import ceil
from os import _exit

import networkx as nx
import networkx.algorithms.matching as max_weight_matching
from Project.source_code.datapreprocessing import calculateDistance
from Project.source_code.mysqlUtilities import insertRecord, getMinDistanceIntersection, connection, getRecords
from pandas import read_sql

tripWindow_start_time = "2016-05-17 14:00:00"
tripWindow_end_time = "2016-05-17 23:59:59"
percent_of_ride_willing_to_ride_share = 30


class Alarm(threading.Thread):
    def __init__(self, timeout):
        threading.Thread.__init__(self)
        self.timeout = timeout
        self.setDaemon(True)

    def run(self):
        time.sleep(self.timeout)
        end_time = datetime.now()
        processed_pools_query = "select * from pool_details where record_entry between \"" + str(start_time) + "\"" + \
                                " and \"" + str(end_time) + "\""

        print("Processed pools query " + processed_pools_query)

        average_trips_saved_query = " select rideLabel,pool_window,sum(initial_trips), sum(final_trips),sum(unshared_trips),count(*)," \
                                    "avg(trips_saved)/avg(initial_trips) * 100 as saved from " \
                                    "pool_details where record_entry between \"" + str(start_time) + "\" and \"" + \
                                    str(end_time) + "\"" + " group by rideLabel, pool_window"

        df_mysql, column_names = getRecords(processed_pools_query)

        if (len(df_mysql) == 0):
            logging.info("No data present in db")
        else:
            number_of_pools_created = len(df_mysql)
            print("Number of pools created " + str(number_of_pools_created))
            df_mysql, column_names = getRecords(average_trips_saved_query)
            # print(column_names)

            for row in df_mysql:
                print("\n\nOrigin :" + row[0] + "\nPool Window :" + str(row[1]) + "\nTrips Saved :" + str(row[6]
                                                                                                          ) + "\nInitial Trips: " + str(
                    row[2]) + "\nFinal Trips: " + str(row[3]) + "\nUnshared Trips: " + str(row[4]))

        _exit(1)


logging.basicConfig(filename='app.log', level=logging.INFO)

# Average speed taken by analyzing the distance and time taken for each trips from 6 month dump
Average_speedinmiles = 22
pool_rides = list()
pool_window_time1 = 5
pool_window_time2 = 10
delay_factor_percent = 20

total_saved_trips = 0

# Laguardia Airport Co-ordinates
source_latitude_min = 40.7714
source_latitude_max = 40.7754
source_longitude_max = -73.8572
source_longitude_min = -73.8875
random_pool_Ids = list(range(7039000, 7050000))
start_time = ""

G = nx.Graph()


def pick_a_ride(pool_rides, origin, pool_window_time):
    global total_saved_trips

    try:
        logging.info("Forming pools for the origin " + origin + " for pool window " + str(pool_window_time))
        # print("Forming pools for the origin " + origin + " for pool window " + str(pool_window_time))
        if len(pool_rides) == 0:
            logging.info("No trips present to form pools for the given origin " + origin)
            # print("No trips present to form pools for the given origin " + origin)
        else:
            logging.info("Number of requests {}".format(len(pool_rides)))
            # print("Number of requests ", len(pool_rides))
            pool = random_pool_Ids.pop()

            if origin == "From Laguardia":
                rideLabel = "From LaGuardia"
            else:
                rideLabel = "To LaGuardia"
            G.clear()
            pool_shares = pool_rides
            total_distance_saved = 0

            logging.info("POOL ID {}".format(pool))
            print("POOL ID ", pool)
            rideIDS = set()
            rideIDSWithDistance = dict()
            start_time = datetime.now()
            print("population "+str(len(pool_rides)))
            passengers_willing_to_ride_share_count = ceil(percent_of_ride_willing_to_ride_share / 100* (
                len(pool_rides)))
            print("sample "+str(passengers_willing_to_ride_share_count))
            pool_rides = pool_rides.sample(n=passengers_willing_to_ride_share_count)
            print("Willing to share" + str(passengers_willing_to_ride_share_count))
            for i in range(0, len(pool_rides)):
                rideIDS.add(pool_shares.iloc[i]['RideID'])
                rideIDSWithDistance[pool_shares.iloc[i]['RideID']] = pool_shares.iloc[i]['dist_airport']
                logging.info("RIDEID {}".format(pool_shares.iloc[i]['RideID']))

                for j in range(i + 1, len(pool_rides)):
                    rideA = pool_shares.iloc[i]
                    rideB = pool_shares.iloc[j]
                    distance_saved = sharing_condition(rideA, rideB, origin)
                    # if distance_saved is > 0, it means ride-sharing condition has been satisfied
                    if distance_saved > 0:
                        logging.info("Ride Sharing condition satisfied for ride ids {} {} "
                                     "with distance saved {}".format(rideA[0], rideB[0], distance_saved))
                        G.add_node(rideA[0])
                        G.add_node(rideB[0])
                        # store the values with ride id a and b along with the max distance saved
                        G.add_edge(rideA[0], rideB[0], weight=distance_saved)

            record_entry = datetime.now()

            # run maximum matching algorithm for ride-shareable graph G
            ride_shareable_nodes = max_weight_matching.max_weight_matching(G)

            # total distance of all rides in a pool
            total_distance_in_pool = sum(rideIDSWithDistance.values())

            ride_shared_nodes_count = len(ride_shareable_nodes)
            # Remove ride IDS that are paired from the array of RideIDS to filter non-shared Ride ids
            for nodes in ride_shareable_nodes:
                if nodes[0] in rideIDS:
                    rideIDS.remove(nodes[0])
                    del rideIDSWithDistance[nodes[0]]
                if nodes[1] in rideIDS:
                    rideIDS.remove(nodes[1])
                    del rideIDSWithDistance[nodes[1]]
                total_distance_saved = total_distance_saved + G.get_edge_data(nodes[0], nodes[1])['weight']

            total_distance_saved = float(total_distance_saved)

            final_trips = len(pool_shares) - ride_shared_nodes_count

            trips_saved = len(pool_shares) - final_trips
            total_saved_trips = total_saved_trips + trips_saved

            unshared_trips = len(pool_shares) - ride_shared_nodes_count * 2
            # get the end time meaning that pool processing is complete
            end_time = datetime.now()
            difference = end_time - start_time


            difference = float(difference.total_seconds())

            # store in db
            pool_insert_query = "insert into pool_details (pool_id,count_of_rides,time_taken,trips_saved," \
                                "final_trips,dist_saved,rideLabel,pool_window,record_entry," \
                                "unshared_trips,initial_trips,initial_trips_distance) values (" + \
                                str(pool) + "," + str(len(ride_shareable_nodes)) + "," + str(
                difference) + "," + str(trips_saved) + "," + str(final_trips) + "," + str(
                total_distance_saved) + "," + "\"" + rideLabel + "\"," + str(pool_window_time) + ",\"" + str(
                record_entry) + "\"," + str(unshared_trips) + "," + str(len(pool_shares)) + "," + str(
                total_distance_in_pool) + ");"
            # print(pool_insert_query)
            database_response = insertRecord(pool_insert_query)
            # print(database_response)
            print("Time taken in seconds for processing pool " + str(pool) + " with " + str(len(
                pool_shares)) + " rides " + str(difference * 0.0166667) + " minutes")
    except Exception as e:
        raise e


# Function to check if trips are mergeable ?
def sharing_condition(rideA, rideB, origin):
    A_dlat = str(rideA[4])
    A_dlon = str(rideA[5])
    A_plat = str(rideA[2])
    A_plon = str(rideA[3])

    B_dlat = str(rideB[4])
    B_dlon = str(rideB[5])
    B_plat = str(rideB[2])
    B_plon = str(rideB[3])
    logging.info(
        "Ride coordinates " + A_plat + " " + A_plon + " " + A_dlat + " " + A_dlon + " " + B_plat + " " + B_plon + " " + B_dlat + " " + B_dlon)
    # print( "Ride coordinates " + A_plat + " " + A_plon + " " + A_dlat + " " + A_dlon + " " + B_plat + " " + B_plon
    # + " " + B_dlat + " " + B_dlon)
    if origin == "To Laguardia":
        A_plat, A_plon, dist_HA = getMinDistanceIntersection(A_plat, A_plon, A_dlat, A_dlon, origin, rideA[6])
        B_plat, B_plon, dist_HB = getMinDistanceIntersection(B_plat, B_plon, B_dlat, B_dlon, origin, rideB[6])
    else:
        A_dlat, A_dlon, dist_HA = getMinDistanceIntersection(A_plat, A_plon, A_dlat, A_dlon, origin, rideA[6])
        B_dlat, B_dlon, dist_HB = getMinDistanceIntersection(B_plat, B_plon, B_dlat, B_dlon, origin, rideB[6])

    if dist_HA == -1 or dist_HB == -1:
        logging.info("Distance is negative(no route) for dist_HA {} or dist_HB {}".format(dist_HA, dist_HB))
        return -1

    if origin == "To Laguardia":
        # the distance is in miles and the time returned is in seconds.
        dist_AB, time_AB = calculateDistance(str(A_plat), str(A_plon), str(B_plat), str(B_plon))
        time_AB = dist_AB / Average_speedinmiles
    else:
        # the distance is in miles and the time returned is in seconds.

        dist_AB, time_AB = calculateDistance(str(A_dlat), str(A_dlon), str(B_dlat), str(B_dlon))
        time_AB = dist_AB / Average_speedinmiles

    # The distance is not found, so do not process this ride pair combination
    if dist_AB == -1:
        logging.info("Distance is negative for dist_AB {}".format(dist_AB))
        return -1

    time_HA = dist_HA / Average_speedinmiles
    time_HB = dist_HB / Average_speedinmiles

    logging.info("Distance AB {} Distance HA {} Distance HB {}".format(dist_AB, dist_HA, dist_HB))
    # print("Distance AB", dist_AB, "Distance HA", dist_HA, "Distance HB", dist_HB)

    # If destination A is visited first and then B is visited or In To Laguardia case, B->A->LaGuardia
    # then calculate delayfactor(source->B or B->Destination(LaGuardia))
    B_delay = (delay_factor_percent / 100) * time_HB
    # If destination B is visited first and then A is visited or In To Laguardia case, A->B->Laguardia
    # then calculate delayfactor(source(LaGuardia)->A or A->Destination(LaGuardia))
    A_delay = (delay_factor_percent / 100) * time_HA


    tempDistanceArray = []

    if origin == "To Laguardia":
        if A_plat == B_plat and A_plon == B_plon:
            dist_AB = 0
        if dist_AB + dist_HB < dist_HA + dist_HB:
            # Apply Euclidean Elimination : 𝑆𝑃(𝐴B) + 𝑇(H,B) < 𝑆𝑃(HA) + 𝐷elay(A)
            if dist_AB + time_HB <= dist_HA + A_delay:
                tempDistanceArray.append(dist_HA - dist_AB)
        if dist_AB + dist_HA < dist_HB + dist_HA:
            # Apply Euclidean Elimination : 𝑆𝑃(𝐴B) + 𝑇(H,B) < 𝑆𝑃(H𝐵) + 𝐷elay(𝐵)
            if dist_AB + time_HA <= dist_HB + B_delay:
                tempDistanceArray.append(dist_HB - dist_AB)
        if not len(tempDistanceArray) > 0:
            tempDistanceArray.append(-1)

    else:
        # If the intersections are common, there is no time elapsed for travelling from one intersection
        # to another and both the riders can be dropped at either of their points.
        if A_dlat == B_dlat and A_dlon == B_dlon:
            time_AB = 0

        if dist_HA + dist_AB < dist_HA + dist_HB:
            # Apply Euclidean Elimination : 𝑆𝑃(HA) + 𝑇(A,B) < 𝑆𝑃(H𝐵) + 𝐷elay(𝐵)
            if dist_HA + time_AB <= dist_HB + B_delay:
                tempDistanceArray.append(dist_HB - dist_AB)
        if dist_HB + dist_AB < dist_HA + dist_HB:
            # Apply Euclidean Elimination : 𝑆𝑃(HB) + 𝑇(A,B) < 𝑆𝑃(HA) + 𝐷elay(A)
            if dist_HB + time_AB <= dist_HA + A_delay:
                tempDistanceArray.append(dist_HA - dist_AB)
        if not len(tempDistanceArray) > 0:
            tempDistanceArray.append(-1)
    distance_saved = min(tempDistanceArray)
    return distance_saved


def load_data_from_source():
    global tripWindow_start_time, tripWindow_end_time
    global fromLaguardiaPoolsCreatedCount
    global toLaguardiaPoolsCreatedCount
    global fromLaguardiaPoolsProcessedCount
    global toLaguardiaPoolsProcesedCount

    # print("TRIPS WINDOW " + tripWindow_start_time + " " + tripWindow_end_time)
    # Get the 1st starting trip record whose pickup time is in between trip window start time and trip window end time
    trip_records_query = "select RideID, tpep_pickup_datetime ,pickup_latitude," + "pickup_longitude, dropoff_latitude," \
                                                                                   "dropoff_longitude,dist_airport from taxitrips_v2 " \
                                                                                   "where tpep_pickup_datetime " \
                                                                                   "between \"" \
                         + tripWindow_start_time + "\" and \"" + tripWindow_end_time + "\" ORDER BY tpep_pickup_datetime ASC "
    # print(trip_records_query)
    df_mysql = read_sql(trip_records_query, con=connection)



    if len(df_mysql) == 0:
        logging.info("No records exist between the given dates " + tripWindow_start_time + " " + tripWindow_end_time)
        # print("No records exist between the given dates " + tripWindow_start_time + " " + tripWindow_end_time)
    else:
        # Put it all to a data frame
        tripData = df_mysql

        # Get 1st records's start date
        pool_start_date = tripData.iloc[0]['tpep_pickup_datetime']

        # Set pool end date based on pool start date and pool window
        pool_end_date = pool_start_date + timedelta(minutes=pool_window_time1)

        # print("Started Analyzing trip requests for pool windows of " + str(pool_window_time1) + " minutes")
        tripWindow_end_time = datetime.strptime(tripWindow_end_time, "%Y-%m-%d %H:%M:%S")

        while pool_end_date <= tripWindow_end_time:
            FromLaguardiaRecords = tripData.loc[
                (tripData['pickup_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['pickup_latitude'].between(source_latitude_min, source_latitude_max))
                & (tripData['tpep_pickup_datetime']).between(pool_start_date, pool_end_date)]
            ToLaguardiaRecords = tripData.loc[
                (tripData['dropoff_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['dropoff_latitude'].between(source_latitude_min, source_latitude_max)) &
                (tripData['tpep_pickup_datetime']).between(pool_start_date, pool_end_date)]

            # print("len is " + str(len(FromLaguardiaRecords)) + " " + str(len(ToLaguardiaRecords)))

            pick_a_ride(FromLaguardiaRecords, "From Laguardia", pool_window_time1)
            pick_a_ride(ToLaguardiaRecords, "To Laguardia", pool_window_time1)

            pool_start_date = pool_end_date + timedelta(seconds=1)
            pool_end_date = pool_end_date + timedelta(minutes=pool_window_time1)

        if pool_start_date < tripWindow_end_time and pool_end_date > tripWindow_end_time:
            FromLaguardiaRecords = tripData.loc[
                (tripData['pickup_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['pickup_latitude'].between(source_latitude_min, source_latitude_max))
                & (tripData['tpep_pickup_datetime']).between(pool_start_date, tripWindow_end_time)]
            ToLaguardiaRecords = tripData.loc[
                (tripData['dropoff_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['dropoff_latitude'].between(source_latitude_min, source_latitude_max)) &
                (tripData['tpep_pickup_datetime']).between(pool_start_date, tripWindow_end_time)]
            print("len is " + str(len(FromLaguardiaRecords)) + " " + str(len(ToLaguardiaRecords)))

            pick_a_ride(FromLaguardiaRecords, "From Laguardia", pool_window_time1)
            pick_a_ride(ToLaguardiaRecords, "To Laguardia", pool_window_time1)

        pool_start_date = tripData.iloc[0]['tpep_pickup_datetime']
        logging.info("Starting processing for 10 minutes window")
        # Set pool end date based on pool start date and pool window
        pool_end_date = pool_start_date + timedelta(minutes=pool_window_time2)

        # Comment the loop below to not process for pool window of size 10 mins
        while pool_end_date <= tripWindow_end_time:
            # print(" Fetching records for pool start and end dates:{} {} ".format(pool_start_date, pool_end_date))
            FromLaguardiaRecords = tripData.loc[
                (tripData['pickup_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['pickup_latitude'].between(source_latitude_min, source_latitude_max))
                &
                (tripData['tpep_pickup_datetime']).between(pool_start_date, pool_end_date)]
            ToLaguardiaRecords = tripData.loc[
                (tripData['dropoff_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['dropoff_latitude'].between(source_latitude_min, source_latitude_max)) &
                (tripData['tpep_pickup_datetime']).between(pool_start_date, pool_end_date)]
            # print("len is " + str(len(FromLaguardiaRecords)) + " " + str(len(ToLaguardiaRecords)))
            pick_a_ride(FromLaguardiaRecords, "From Laguardia", pool_window_time2)
            pick_a_ride(ToLaguardiaRecords, "To Laguardia", pool_window_time2)

            pool_start_date = pool_end_date + timedelta(seconds=1)
            pool_end_date = pool_end_date + timedelta(minutes=pool_window_time2)
        # Comment the loop below to not process for pool window of size 10 mins
        if pool_start_date < tripWindow_end_time and pool_end_date > tripWindow_end_time:
            FromLaguardiaRecords = tripData.loc[
                (tripData['pickup_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['pickup_latitude'].between(source_latitude_min, source_latitude_max))
                & (tripData['tpep_pickup_datetime']).between(pool_start_date, tripWindow_end_time)]
            ToLaguardiaRecords = tripData.loc[
                (tripData['dropoff_longitude'].between(source_longitude_min, source_longitude_max)) & (
                    tripData['dropoff_latitude'].between(source_latitude_min, source_latitude_max)) &
                (tripData['tpep_pickup_datetime']).between(pool_start_date, tripWindow_end_time)]
            # print("len is " + str(len(FromLaguardiaRecords)) + " " + str(len(ToLaguardiaRecords)))

            pick_a_ride(FromLaguardiaRecords, "From Laguardia", pool_window_time2)
            pick_a_ride(ToLaguardiaRecords, "To Laguardia", pool_window_time2)


def main():
    global start_time
    print("Starting Program...")
    program_start_time = time.time()
    try:
        alarm = Alarm(300)
        # Uncomment the below statement to start the alarm to run program for 300 seconds
        # alarm.start()
        start_time = datetime.now()
        load_data_from_source()
        print("Terminating.")
        del alarm

    except Exception as e:
        print("Exception thrown " + str(e))


if __name__ == "__main__":
    main()
