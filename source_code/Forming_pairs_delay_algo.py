import pandas as pd
import random
import time
from itertools import combinations
from Project.source_code.datapreprocessing import calculateDistance
from Project.source_code.mysqlUtilities import insertRecord, getRecords
from datetime import datetime, timedelta

pool_rides = list()
pool_window_time1 = 5
pool_window_time2 = 10
delay_factor_percent = 20
tripWindow_start_time = "29-01-2016 11:32"
tripWindow_end_time = "29-01-2016 11:35"
total_time_delta_minutes = 5
ride_combinations = 2


# Supports upto 1 Million ride-requests
def generatePoolID():
    list_of_ids = list(range(1000000))
    random.shuffle(list_of_ids)
    if len(list_of_ids) == 0:
        raise Exception("All Pool Ids are exhausted. Cannot form pool. Quitting program")
    else:
        return list_of_ids.pop()


# Function to form a networkx graph and run max_weight_matching
# to get best set of edges for ride-sharing
def formAdjacencyGraph(final_array):
    import networkx as nx
    import networkx.algorithms.matching as max_weight_matching
    G = nx.Graph()
    # Sample Input for testing purposes
    temp = dict()
    temp['D1'] = 1
    temp['D2'] = 3
    temp['D_save'] = 5
    final_array.append(temp)
    temp = dict()
    temp['D1'] = 3
    temp['D2'] = 4
    temp['D_save'] = 2
    final_array.append(temp)
    # temp=dict()
    # temp['D1']=2
    # temp['D2']=4
    # temp['D_save']=3
    # final_array.append(temp)
    # temp=dict()
    # temp['D1']=2
    # temp['D2']=3
    # temp['D_save']=0
    # final_array.append(temp)
    for ride in final_array:
        ride1 = ride['D1']
        ride2 = ride['D2']
        G.add_node(ride1)
        G.add_node(ride2)
        G.add_edge(ride1, ride2, weight=ride['D_save'])
    print(G.number_of_nodes())
    print(G.get_edge_data(1, 3))
    return max_weight_matching.max_weight_matching(G), G


def generateTripID():
    tripIDS = list(range(3000))
    random.shuffle(tripIDS)
    return tripIDS.pop()


def pick_a_ride(pool_window_time, origin):
    # Form the pool map for the given pool window
    pool_map = formPools(pool_window_time)
    pools_ending_time = timedelta(total_time_delta_minutes).total_seconds()
    poolsCount = 0
    cumulative_pools_processing_time = -1
    rideLabel = ""
    if origin == "Laguardia":
        rideLabel = "From LaGuardia"
    else:
        rideLabel = "To LaGuardia"

    # Get the list of rides within the pool window period
    for pool in pool_map:
        print("POOL ID ", pool)
        pool_shares = pool_map[pool]
        print("Number of requests ", len(pool_shares))
        number_of_rides = len(pool_shares)
        rideIDS = []
        for x in range(number_of_rides):
            rideIDS.append(pool_shares[x][0])

        start_time = datetime.utcnow()

        comb = combinations(pool_shares, ride_combinations)
        final_array = []
        # Print the obtained combinations
        for ride_combination in list(comb):
            rideA = ride_combination[0]
            rideB = ride_combination[1]
            localData = dict()
            distance_saved = sharing_condition(rideA, rideB)
            # if distance_saved is > 0, it means ride-sharing condition has been satisfied
            if distance_saved > 0:
                # store the values with ride id a and b along with final=the max distance saved
                localData["D1"] = rideA[0];
                localData["D2"] = rideB[1];
                localData["D_save"] = distance_saved
                final_array.append(localData)

        print(final_array)

        # Form graph and get best set of edges for ride-sharing
        best_nodes, Graph = formAdjacencyGraph(final_array)

        total_distance_saved = 0

        # Remove ride IDS that are paired from the array of RideIDS to filter non-shared Ride ids
        for nodes in best_nodes:
            if nodes[0] in rideIDS:
                rideIDS.remove(nodes[0])
            if nodes[1] in rideIDS:
                rideIDS.remove(nodes[1])
            total_distance_saved = total_distance_saved + Graph.get_edge_data(nodes[0], nodes[1])['weight']
        total_distance_saved = float(total_distance_saved)
        end_time = datetime.utcnow()
        difference = end_time - start_time
        cumulative_pools_processing_time = cumulative_pools_processing_time + difference.total_seconds()
        difference = float(difference.total_seconds())

        # check if the cumulative time has crossed the timeframe
        if cumulative_pools_processing_time >= pools_ending_time:
            print("Total Pools processed in " + total_time_delta_minutes + " is ", poolsCount)
        else:
            poolsCount = poolsCount + 1

        print("Time taken in seconds for processing pool " + pool, difference)

        # store in db
        pool_insert_query = "insert into pool_details (pool_id,count_of_rides,time_taken,dist_saved,rideLabel) values (" + \
                            str(pool) + "," + str(len(best_nodes)) + "," + str(difference) + "," + str(
            total_distance_saved) + "," + rideLabel + ");"
        print(pool_insert_query)
        database_response = insertRecord(pool_insert_query)
        print(database_response)
        tripID = generateTripID()
        # Insert ride-sharable requests as
        for nodes in best_nodes:
            trip_detail_insert_query = "insert into trip_details (trip_id,pool_id,ride_t_id,isRideShared,rideLabel) " \
                                       "values (" + \
                                       str(tripID) + "," + str(pool) + "," + str(
                nodes[0]) + "," + "True" + "," + rideLabel + \
                                       ");"
            insertRecord(trip_detail_insert_query)
            trip_detail_insert_query = "insert into trip_details (trip_id,pool_id,ride_t_id,isRideShared,rideLabel) values (" + \
                                       str(tripID) + "," + str(pool) + "," + str(
                nodes[1]) + "," + "True" + "," + rideLabel + \
                                       ")"
            insertRecord(trip_detail_insert_query)

        # insert records that are not ride-shared
        for rideID in rideIDS:
            trip_detail_insert_query = "insert into trip_details (trip_id,pool_id,ride_t_id,isRideShared,rideLabel) values (" + \
                                       str(tripID) + "," + str(pool) + "," + str(
                rideID) + "," + "False" + "," + rideLabel + \
                                       ")"
            insertRecord(trip_detail_insert_query)


def sharing_condition(rideA, rideB):
    # check for the 2 conditions
    A_dlat = str(rideA['dropoff_latitude']).strip()
    A_dlon = str(rideA['dropoff_longitude']).strip()
    A_plat = str(rideA['pickup_latitude']).strip()
    A_plon = str(rideA['pickup_longitude']).strip()
    B_dlat = str(rideB['dropoff_latitude']).strip()
    B_dlon = str(rideB['dropoff_longitude']).strip()
    B_plat = str(rideB['pickup_latitude']).strip()
    B_plon = str(rideB['pickup_longitude']).strip()

    dist_AB, time_AB = calculateDistance(A_dlat, A_dlon, B_dlat, B_dlon)
    dist_HA, time_HA = calculateDistance(A_plat, A_plon, A_dlat, A_dlon)
    dist_HB, time_HB = calculateDistance(B_plat, B_plon, B_dlat, B_dlon)

    # If destination A is visited first and then B is visited, calculate delayfactor(source->B)
    B_delay = (delay_factor_percent / 100) * time_HB
    # If destination B is visited first and then A is visited, calculate delayfactor(source->A)
    A_delay = (delay_factor_percent / 100) * time_HA

    print(dist_AB, dist_HA, dist_HB)
    distance_saved = -1

    if dist_HA + dist_AB < dist_HA + dist_HB:
        if dist_HA + dist_AB <= dist_HB + B_delay:
            distance_saved = dist_HB - dist_AB
    if dist_HB + dist_AB < dist_HA + dist_HB:
        if dist_HB + dist_AB <= dist_HA + A_delay:
            distance_saved = dist_HA - dist_AB

    return distance_saved


def select_Rides_pool(pool_time: int):
    # Read pool requests from database
    select_query = "select RideID, tpep_pickup_datetime, tpep_dropoff_datetime ,pickup_latitude," \
                   "pickup_longitude, DelayFactor from taxitrips where tpep_pickup_datetime between " \
                   + "str_to_date(tripWindow_start_time,'%d-%m-%Y %H:%i:%s') " \
                     "and str_to_date(tripWindow_end_time,'%d-%m-%Y %H:%i:%s')" \
                   + " ORDER BY tpep_pickup_datetime ASC"
    records = getRecords(select_query)
    if records.count() == 0:
        print("The are no records in the database that satisfied the given query: " + select_query)
    else:
        data = records
        # dataframe will have first ride which has not yet been rideshared.
        # The starting trip time of the first trip from the pool of requests
        pool_beiginning_time = data['tpep_pickup_datetime'].iloc[0]
        timeObject = datetime.strptime(pool_beiginning_time, '%d-%m-%Y %H:%M')
        pool_ending_time = timeObject + timedelta(minutes=pool_time)
        # Finding all the rides which belongs to the pool window
        # Checking the condition pool_beginning_time >= pickup time <= pool_ending_time
        # Here we are assuming the pickup time as ride request time

        locs = (data['tpep_pickup_datetime'] > timeObject.strftime("%d-%m-%Y %H:%M")) & (
                data['tpep_pickup_datetime'] < pool_ending_time.strftime("%d-%m-%Y %H:%M"))
        pool_rides = data[locs]
        # print(pool_rides)
        return pool_rides


def formPools(pool_time: int):
    global pool_rides

    # Read pool requests from database
    select_query = "select RideID, tpep_pickup_datetime ,pickup_latitude," \
                   "pickup_longitude, DelayFactor from taxitrips where tpep_pickup_datetime between " \
                   + "str_to_date('" + tripWindow_start_time + "','%d-%m-%Y %H:%i:%s') " \
                                                               "and str_to_date('" + tripWindow_end_time + "','%d-%m-%Y %H:%i:%s')" \
                   + " ORDER BY tpep_pickup_datetime ASC"

    records = getRecords(select_query)
    if len(records) == 0:
        print("No records fetched for the given query : " + select_query)
    else:

        data = records
        start_time = data[0][1]
        # The starting trip time of the first trip from the pool of requests
        pool_beginning_time = start_time
        pool_ending_time = pool_beginning_time + timedelta(minutes=pool_time)
        poolMap = dict()
        poolRequests = list()
        for row in data:
            print(row[1])

            if pool_beginning_time <= row[1] < pool_ending_time:
                poolRequests.append(row)
            else:
                poolID = generatePoolID()
                poolMap[poolID] = poolRequests
                poolRequests = list()
                poolRequests.append(row)
                pool_beginning_time = row[1]
                pool_ending_time = pool_beginning_time + timedelta(minutes=pool_time)

        # Add the pending request as another pool entry
        if len(poolRequests) > 0:
            poolID = generatePoolID()
            poolMap[poolID] = poolRequests
        print(poolMap)
        return poolMap


print("Starting Program")
program_start_time = time.time()
print("Starting Ride-Sharing logic for pool window of time: " + pool_window_time1 + " minutes")
pick_a_ride(pool_window_time1)
print("Starting Ride-Sharing logic for pool window of time: " + pool_window_time2 + " minutes")
pick_a_ride(pool_window_time2)
print("--- %s seconds ---" % (time.time() - program_start_time))
