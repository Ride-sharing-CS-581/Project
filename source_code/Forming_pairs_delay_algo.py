import random
import time
from Project.source_code.datapreprocessing import calculateDistance
from Project.source_code.mysqlUtilities import insertRecord, getRecords, getMinDistanceIntersection
from datetime import datetime, timedelta
import networkx as nx
import networkx.algorithms.matching as max_weight_matching
from haversine import haversine, Unit

Average_speedinmiles = 29
pool_rides = list()
pool_window_time1 = 5
pool_window_time2 = 10
delay_factor_percent = 20
tripWindow_start_time = "2016-05-17 00:00:00"
tripWindow_end_time = "2016-05-17 12:00:00"
total_time_delta_minutes = 5

total_pools_running_time = 0
total_pools_processed = 0

# Laguardia Airport Co-ordinates
source_latitude_min = 40.7714
source_latitude_max = 40.7754
source_longitude_max = -73.8572
source_longitude_min = -73.8875
random_pool_Ids = list(range(7100000, 8100000))
random_trip_Ids = list(range(5100000, 6000000))
trips_From_Laguardia = []
trips_To_Laguardia = []
G = nx.Graph()


def pick_a_ride(pool_map, origin, pool_window_time):
    global total_pools_running_time, total_pools_processed, total_time_delta_minutes
    try:

        print("Forming pools for the origin " + origin + " for pool window " + str(pool_window_time))
        if len(pool_map.keys()) == 0:
            print("No trips present to form pools for the given origin " + origin)
        else:

            pools_ending_time = timedelta(total_time_delta_minutes).total_seconds()
            poolsCount = 0
            cumulative_pools_processing_time = 0
            rideLabel = ""
            if origin == "From Laguardia":
                rideLabel = "From LaGuardia"
            else:
                rideLabel = "To LaGuardia"

            # Get the list of rides within the pool window period
            for pool in pool_map:
                G.clear()
                total_distance_saved = 0
                ride_shared_nodes_count = 0
                print("POOL ID ", pool)
                pool_shares = pool_map[pool]
                print("Number of requests ", len(pool_shares))
                number_of_rides = len(pool_shares)
                isRideSharingDone = False
                start_time = datetime.utcnow()
                final_array = []
                index1 = 0
                length = len(pool_shares)
                rideIDS = set()
                rideIDSWithDistance = dict()
                # Print the obtained combinations
                while index1 < length:
                    rideIDS.add(pool_shares[index1][0])
                    rideIDSWithDistance[pool_shares[index1][0]] = pool_shares[index1][6]
                    index2 = index1 + 1
                    while index2 < length:
                        rideA = pool_shares[index1]
                        rideB = pool_shares[index2]
                        distance_saved = sharing_condition(rideA, rideB, origin)
                        # if distance_saved is > 0, it means ride-sharing condition has been satisfied
                        if distance_saved > 0:
                            G.add_node(rideA[0])
                            G.add_node(rideB[0])
                            # store the values with ride id a and b along with the max distance saved
                            G.add_edge(rideA[0], rideB[0], weight=distance_saved)
                        index2 = index2 + 1
                    index1 = index1 + 1

                # Means that there is at least 1 pair satisfying ride-sharing condition
                if G.number_of_nodes() > 0:

                    # run maximum matching algorithm for ride-shareable graph G
                    ride_shareable_nodes = max_weight_matching.max_weight_matching(G)

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

                    # get the end time meaning that pool processing is complete
                    end_time = datetime.utcnow()
                    difference = end_time - start_time
                    # Keep track of cumulative time that is elapsed
                    cumulative_pools_processing_time = cumulative_pools_processing_time + difference.total_seconds()
                    total_pools_running_time = total_pools_running_time + cumulative_pools_processing_time
                    difference = float(difference.total_seconds())
                    record_entry = datetime.utcnow()
                    # store in db
                    pool_insert_query = "insert into pool_details (pool_id,count_of_rides,time_taken,dist_saved,rideLabel,pool_window,record_entry) values (" + \
                                        str(pool) + "," + str(len(ride_shareable_nodes)) + "," + str(
                        difference) + "," + str(
                        total_distance_saved) + "," + "\"" + rideLabel + "\"," + str(pool_window_time) + ",\"" + str(
                        record_entry) + "\");"
                    print(pool_insert_query)
                    database_response = insertRecord(pool_insert_query)
                    print(database_response)
                    isRideSharingDone = True

                    # Insert ride-sharable requests as individual trips
                    for nodes in ride_shareable_nodes:
                        tripID = random_trip_Ids.pop()
                        trip_detail_insert_query = "insert into trip_details (trip_id,pool_id,rideT_id,isRideShared,rideLabel,record_entry) " \
                                                   "values (" + \
                                                   str(tripID) + "," + str(pool) + "," + str(
                            nodes[0]) + "," + "1" + "," + "\"" + rideLabel + "\"" + \
                                                   ",\"" + str(record_entry) + "\");"
                        insertRecord(trip_detail_insert_query)
                        trip_detail_insert_query = "insert into trip_details (trip_id,pool_id,rideT_id,isRideShared,rideLabel,record_entry) values (" + \
                                                   str(tripID) + "," + str(pool) + "," + str(
                            nodes[1]) + "," + "1" + "," + "\"" + rideLabel + "\"" + \
                                                   ",\"" + str(record_entry) + "\")"
                        insertRecord(trip_detail_insert_query)
                record_entry = datetime.utcnow()
                # If ridesharing is not done, no trips are combined.
                if not isRideSharingDone:
                    end_time = datetime.utcnow()
                    difference = end_time - start_time
                    difference = float(difference.total_seconds())

                    dist_saved = sum(rideIDSWithDistance.values())
                    # store in db
                    pool_insert_query = "insert into pool_details (pool_id,count_of_rides,time_taken,dist_saved,rideLabel,pool_window,record_entry) values (" + \
                                        str(pool) + "," + str(len(rideIDS)) + "," + str(difference) + "," + str(
                        dist_saved) + "," + "\"" + rideLabel + "\"," + str(pool_window_time) + ",\"" + str(
                        record_entry) + "\");"
                    print(pool_insert_query)
                    database_response = insertRecord(pool_insert_query)
                    print(database_response)
                else:
                    dist_saved = sum(rideIDSWithDistance.values())
                    # Update pool entry to update count to the sum of existing value+ count of individual trips
                    update_pool_query = "update pool_details set count_of_rides=" + str(
                        ride_shared_nodes_count + len(rideIDS)) + \
                                        ",dist_saved=" + str(
                        total_distance_saved + dist_saved) + " where pool_id=" + str(pool)
                    print(update_pool_query)
                    insertRecord(update_pool_query)
                # insert records that are not ride-shared
                for rideID in rideIDS:
                    # store in db
                    tripID = random_trip_Ids.pop()
                    trip_detail_insert_query = "insert into trip_details (trip_id,pool_id,rideT_id,isRideShared," \
                                               "rideLabel,record_entry) values (" + \
                                               str(tripID) + "," + str(pool) + "," + str(
                        rideID) + "," + "0" + "," + "\"" + rideLabel + "\"" + \
                                               ",\"" + str(record_entry) + "\")"
                    insertRecord(trip_detail_insert_query)

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

    if origin == "To Laguardia":
        A_plat, A_plon, dist_HA = getMinDistanceIntersection(A_plat, A_plon, A_dlat, A_dlon, origin)
        B_plat, B_plon, dist_HB = getMinDistanceIntersection(B_plat, B_plon, B_dlat, B_dlon, origin)
    else:
        A_dlat, A_dlon, dist_HA = getMinDistanceIntersection(A_plat, A_plon, A_dlat, A_dlon, origin)
        B_dlat, B_dlon, dist_HB = getMinDistanceIntersection(B_plat, B_plon, B_dlat, B_dlon, origin)

    A_dlat = str(A_dlat)
    A_dlon = str(A_dlon)
    A_plon = str(A_plon)
    A_plat = str(A_plat)

    B_dlat = str(B_dlat)
    B_dlon = str(B_dlon)
    B_plon = str(B_plon)
    B_plat = str(B_plat)

    dist_AB = 0

    if origin == "To Laguardia":
        # the distance is in miles and the time returned is in seconds.
        dist_AB = haversine((float(A_plat), float(A_plon)), (float(B_plat), float(B_plon)), unit=Unit.MILES)
        time_AB = dist_AB / Average_speedinmiles
        # dist_AB, time_AB = calculateDistance(A_plat, A_plon, B_plat, B_plon)
    else:
        # the distance is in miles and the time returned is in seconds.
        dist_AB = haversine((float(A_dlat), float(A_dlon)), (float(B_dlat), float(B_dlon)), unit=Unit.MILES)
        # dist_AB, time_AB = calculateDistance(A_dlat, A_dlon, B_dlat, B_dlon)
        time_AB = dist_AB / Average_speedinmiles

    time_HA = dist_HA / Average_speedinmiles
    time_HB = dist_HB / Average_speedinmiles
    # dist_HA, time_HA = calculateDistance(A_plat, A_plon, A_dlat, A_dlon)
    # dist_HB, time_HB = calculateDistance(B_plat, B_plon, B_dlat, B_dlon)

    # Convert the seconds to hour
    # 1 hours = 3600s
    # time_HA = time_HA / 3600
    # time_HB = time_HB / 3600
    # time_AB = time_AB / 3600

    # If destination A is visited first and then B is visited or In To Laguardia case, B->A->LaGuardia
    # then calculate delayfactor(source->B or B->Destination(LaGuardia))
    B_delay = (delay_factor_percent / 100) * time_HB
    # If destination B is visited first and then A is visited or In To Laguardia case, A->B->Laguardia
    # then calculate delayfactor(source(LaGuardia)->A or A->Destination(LaGuardia))
    A_delay = (delay_factor_percent / 100) * time_HA

    print(dist_AB, dist_HA, dist_HB)

    # 𝑆𝑃(𝐴) + 𝑇(𝑑𝑒𝑠𝑡 𝐴 , 𝑑𝑒𝑠𝑡(𝐵)) < 𝑆𝑃(𝐵) + 𝐷elay(𝐵)
    tempDistanceArray = []

    if origin == "To Laguardia":
        if A_plat == B_plat and A_plon == B_plon:
            dist_AB = 0
        if dist_AB + dist_HB < dist_HA + dist_HB:  # 0 + HB < 2HA and 0 + X <= HA + A_Delay.
            if dist_AB + time_HB <= dist_HA + A_delay:
                tempDistanceArray.append(dist_HA - dist_AB)
        if dist_AB + dist_HA < dist_HB + dist_HA:
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
            # if dist_HA + time_AB <= dist_HB + B_delay:
            #     distance_saved = dist_HB - dist_AB
            if dist_HA + time_AB <= dist_HB + B_delay:
                tempDistanceArray.append(dist_HB - dist_AB)
        if dist_HB + dist_AB < dist_HA + dist_HB:
            # if dist_HB + dist_AB <= dist_HA + A_delay:
            #     distance_saved = dist_HA - dist_AB
            if dist_HB + time_AB <= dist_HA + A_delay:
                tempDistanceArray.append(dist_HA - dist_AB)
        if not len(tempDistanceArray) > 0:
            tempDistanceArray.append(-1)
    distance_saved = min(tempDistanceArray)
    return distance_saved


# function to form pools for the given pool window
def formPools(trips, pool_window: int):
    try:

        data = trips
        # The starting trip time of the first trip from the pool of requests
        pool_beginning_time = data[0][1]
        pool_ending_time = pool_beginning_time + timedelta(minutes=pool_window)
        poolMap = dict()
        poolRequests = list()
        # Iterate through every request, check if they fall within the pool's time frame and create a
        # dictionary of poolID-rides
        for row in data:
            print(row[1])
            # Check if the current trip pickup time is within the pool's timeframe
            if pool_beginning_time <= row[1] < pool_ending_time:
                poolRequests.append(row)
            else:
                # Get poolID, create map entry for data in poolRequests array and extend pool timeframe based on
                # current trip time
                poolID = random_pool_Ids.pop()
                poolMap[poolID] = poolRequests
                poolRequests = list()
                poolRequests.append(row)
                pool_beginning_time = row[1]
                pool_ending_time = pool_beginning_time + timedelta(minutes=pool_window)

        # Add the pending request as another pool entry
        if len(poolRequests) > 0:
            poolID = random_pool_Ids.pop()
            poolMap[poolID] = poolRequests
            print(poolMap)
            return poolMap
    except Exception as e:
        raise Exception("Exception at forming pools " + str(e))


def load_data_from_source():
    global tripWindow_start_time, tripWindow_end_time
    poolsCount = 0
    fromLaguardiaPoolsCreatedCount = 0
    toLaguardiaPoolsCreatedCount = 0
    fromLaguardiaPoolsProcessedCount = 0
    toLaguardiaPoolsProcesedCount = 0
    cumulativeSum = 0
    isDataShown = False
    computationTimeInSeconds = timedelta(minutes=total_time_delta_minutes).total_seconds()
    # Get the 1st starting trip record whose pickup time is in between trip window start time and trip window end time
    trip_records_query = "select RideID, tpep_pickup_datetime ,pickup_latitude," + "pickup_longitude, dropoff_latitude," \
                                                                                   "dropoff_longitude,dist_airport from taxitrips " \
                                                                                   "where tpep_pickup_datetime " \
                                                                                   "between \"" \
                         + tripWindow_start_time + "\" and \"" + tripWindow_end_time + "\" ORDER BY tpep_pickup_datetime ASC LIMIT 0,1"
    print(trip_records_query)
    # Create pools for 2nd pool window
    result = getRecords(trip_records_query)
    if len(result) == 0:
        print("No records exist between the given dates " + tripWindow_start_time + " " + tripWindow_end_time)
    else:
        # Create pools for 1st pool window
        pool_start_date = result[0][1]
        pool_end_date = pool_start_date + timedelta(minutes=pool_window_time1)

        print("Started Analyzing trip requests for pool windows of " + str(pool_window_time1) + " minutes")
        tripWindow_end_time = datetime.strptime(tripWindow_end_time, "%Y-%m-%d %H:%M:%S")
        '''while pool_end_date <= tripWindow_end_time:

            # Read pool requests from database
            fromlaguardia_query = "select RideID, tpep_pickup_datetime ,pickup_latitude," + "pickup_longitude, dropoff_latitude," \
                                                                                            " dropoff_longitude,dist_airport from taxitrips where tpep_pickup_datetime between \"" \
                                  + str(pool_start_date) + "\" and \"" + str(pool_end_date) + "\" and pickup_latitude " \
                                                                                              "between " + \
                                  str(source_latitude_min) + \
                                  " and " + str(source_latitude_max) + " and pickup_longitude between " + str(
                source_longitude_min) + " and " + \
                                  str(source_longitude_max) + " ORDER BY tpep_pickup_datetime ASC"

            # Read pool requests from database
            tolaguardia_query = "select RideID, tpep_pickup_datetime ,pickup_latitude," \
                                + "pickup_longitude, dropoff_latitude, dropoff_longitude,dist_airport from taxitrips where tpep_pickup_datetime " \
                                  "between \"" + str(pool_start_date) + "\" and \"" + str(pool_end_date) + "\" and " \
                                                                                                           "dropoff_latitude " \
                                                                                                           "between " + str(
                source_latitude_min) + \
                                " and " + str(source_latitude_max) + " and dropoff_longitude between " + str(
                source_longitude_min) + " and " + \
                                str(source_longitude_max) + " ORDER BY tpep_pickup_datetime ASC"

            records1 = getRecords(fromlaguardia_query)
            records2 = getRecords(tolaguardia_query)
            print("Fetched # records from the database ", len(records1))
            print("Fetched # records from the database ", len(records2))

            if len(records1) == 0:
                print("No records fetched for the given query : " + fromlaguardia_query)

            else:

                pool_map = dict()
                pool_map[random_pool_Ids.pop()] = records1
                poolsCount = poolsCount + 1
                fromLaguardiaPoolsCreatedCount = fromLaguardiaPoolsCreatedCount + 1
                starting_time = datetime.utcnow()
                pick_a_ride(pool_map, "From Laguardia", pool_window_time1)
                fromLaguardiaPoolsProcessedCount = fromLaguardiaPoolsProcessedCount + 1
                ending_time = datetime.utcnow()
                cumulativeSum = cumulativeSum + (ending_time - starting_time).total_seconds()
                if cumulativeSum >= computationTimeInSeconds and isDataShown == False:
                    print("Trips created in {} for pool window of {} minutes is given below".format(
                        total_time_delta_minutes, pool_window_time1))
                    print("--- Total Pools Created is {}".format(
                        fromLaguardiaPoolsCreatedCount + toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Created for trips starting at Laguardia {}".format(
                        fromLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips starting at Laguadia {}".format(
                        fromLaguardiaPoolsProcessedCount))
                    print(
                        "--- Total Pools Created for trips ending at Laguardia {}".format(toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips ending at Laguardia {}".format(
                        toLaguardiaPoolsProcesedCount))
                    isDataShown = True

            if len(records2) == 0:
                print("No records fetched for the given query : " + tolaguardia_query)

            else:

                pool_map = dict()
                pool_map[random_pool_Ids.pop()] = records2
                poolsCount = poolsCount + 1
                toLaguardiaPoolsCreatedCount = toLaguardiaPoolsCreatedCount + 1
                starting_time = datetime.utcnow()
                pick_a_ride(pool_map, "To Laguardia", pool_window_time1)
                toLaguardiaPoolsProcesedCount = toLaguardiaPoolsProcesedCount + 1
                ending_time = datetime.utcnow()
                cumulativeSum = cumulativeSum + (ending_time - starting_time).total_seconds()
                if cumulativeSum >= computationTimeInSeconds and isDataShown == False:
                    print("Trips created in {} for pool window of {} minutes is given below".format(
                        total_time_delta_minutes, pool_window_time1))
                    print("--- Total Pools Created is {}".format(
                        fromLaguardiaPoolsCreatedCount + toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Created for trips starting at Laguardia {}".format(
                        fromLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips starting at Laguadia {}".format(
                        fromLaguardiaPoolsCreatedCount - fromLaguardiaPoolsProcessedCount))
                    print(
                        "--- Total Pools Created for trips ending at Laguardia {}".format(toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips ending at Laguardia {}".format(
                        toLaguardiaPoolsCreatedCount - toLaguardiaPoolsProcesedCount))
                    isDataShown = True

            pool_start_date = pool_end_date + timedelta(minutes=1)
            pool_end_date = pool_end_date + timedelta(minutes=pool_window_time1)'''

        pool_start_date = result[0][1]
        pool_end_date = pool_start_date + timedelta(minutes=pool_window_time2)
        fromLaguardiaPoolsCreatedCount = 0
        fromLaguardiaPoolsProcessedCount = 0
        toLaguardiaPoolsProcesedCount = 0
        toLaguardiaPoolsCreatedCount = 0
        cumulativeSum = 0
        isDataShown = False
        print("Started Analyzing trip requests for pool windows of " + str(pool_window_time2) + " minutes")
        # Create pools for 2st pool window
        while pool_end_date <= tripWindow_end_time:
            print("POOL start date",str(pool_start_date))
            print("POOL end date",str(pool_end_date))
            # Read pool requests from database
            fromlaguardia_query = "select RideID, tpep_pickup_datetime ,pickup_latitude," + "pickup_longitude, dropoff_latitude," \
                                                                                            " dropoff_longitude,dist_airport from taxitrips where tpep_pickup_datetime between \"" \
                                  + str(pool_start_date) + "\" and \"" + str(pool_end_date) + "\" and pickup_latitude " \
                                                                                              "between " + \
                                  str(source_latitude_min) + \
                                  " and " + str(source_latitude_max) + " and pickup_longitude between " + str(
                source_longitude_min) + " and " + \
                                  str(source_longitude_max) + " ORDER BY tpep_pickup_datetime ASC"

            # Read pool requests from database
            tolaguardia_query = "select RideID, tpep_pickup_datetime ,pickup_latitude," \
                                + "pickup_longitude, dropoff_latitude, dropoff_longitude,dist_airport from taxitrips where tpep_pickup_datetime " \
                                  "between \"" + str(pool_start_date) + "\" and \"" + str(pool_end_date) + "\" and " \
                                                                                                           "dropoff_latitude " \
                                                                                                           "between " + str(
                source_latitude_min) + \
                                " and " + str(source_latitude_max) + " and dropoff_longitude between " + str(
                source_longitude_min) + " and " + \
                                str(source_longitude_max) + " ORDER BY tpep_pickup_datetime ASC"

            records1 = getRecords(fromlaguardia_query)
            records2 = getRecords(tolaguardia_query)
            print("Fetched # records from the database ", len(records1))
            print("Fetched # records from the database ", len(records2))

            if len(records1) == 0:
                print("No records fetched for the given query : " + fromlaguardia_query)

            else:
                fromLaguardiaPoolsCreatedCount = fromLaguardiaPoolsCreatedCount + 1

                pool_map = dict()
                pool_map[random_pool_Ids.pop()] = records1
                starting_time = datetime.utcnow()
                pick_a_ride(pool_map, "From Laguardia", pool_window_time2)
                fromLaguardiaPoolsProcessedCount = fromLaguardiaPoolsProcessedCount + 1
                ending_time = datetime.utcnow()
                cumulativeSum = cumulativeSum + (ending_time - starting_time).total_seconds()
                if cumulativeSum >= computationTimeInSeconds and isDataShown == False:
                    print("Trips created in {} for pool window of {} minutes is given below".format(
                        total_time_delta_minutes, pool_window_time2))
                    print("--- Total Pools Created is {}".format(
                        fromLaguardiaPoolsCreatedCount + toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Created for trips starting at Laguardia {}".format(
                        fromLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips starting at Laguadia {}".format(
                        fromLaguardiaPoolsCreatedCount - fromLaguardiaPoolsProcessedCount))
                    print(
                        "--- Total Pools Created for trips ending at Laguardia {}".format(toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips ending at Laguardia {}".format(
                        toLaguardiaPoolsCreatedCount - toLaguardiaPoolsProcesedCount))
                    isDataShown = True

            if len(records2) == 0:
                print("No records fetched for the given query : " + tolaguardia_query)

            else:
                pool_map = dict()
                pool_map[random_pool_Ids.pop()] = records2
                toLaguardiaPoolsCreatedCount = toLaguardiaPoolsCreatedCount + 1
                starting_time = datetime.utcnow()
                pick_a_ride(pool_map, "To Laguardia", pool_window_time2)
                toLaguardiaPoolsProcesedCount = toLaguardiaPoolsProcesedCount + 1
                ending_time = datetime.utcnow()
                cumulativeSum = cumulativeSum + (ending_time - starting_time).total_seconds()
                if cumulativeSum >= computationTimeInSeconds and isDataShown == False:
                    print("Trips created in {} for pool window of {} minutes is given below".format(
                        total_time_delta_minutes, pool_window_time2))
                    print("--- Total Pools Created is {}".format(
                        fromLaguardiaPoolsCreatedCount + toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Created for trips starting at Laguardia {}".format(
                        fromLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips starting at Laguadia {}".format(
                        fromLaguardiaPoolsCreatedCount - fromLaguardiaPoolsProcessedCount))
                    print(
                        "--- Total Pools Created for trips ending at Laguardia {}".format(toLaguardiaPoolsCreatedCount))
                    print("--- Total Pools Analyzed for trips ending at Laguardia {}".format(
                        toLaguardiaPoolsCreatedCount - toLaguardiaPoolsProcesedCount))
                    isDataShown = True
            pool_start_date = pool_end_date + timedelta(minutes=1)
            pool_end_date = pool_end_date + timedelta(minutes=pool_window_time2)




def main():
    print("Starting Program...")
    program_start_time = time.time()
    random.shuffle(random_pool_Ids)
    random.shuffle(random_trip_Ids)
    load_data_from_source()
    print("Starting Ride-Sharing logic for pool window of time: " + str(pool_window_time1) + " minutes")
    print("---Total Program Run time %s minutes ---{}".format((time.time() - program_start_time) / 60))


if __name__ == "__main__":
    main()
