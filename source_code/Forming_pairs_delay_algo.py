import pandas as pd
import random
import time
from itertools import combinations
from Project.source_code.datapreprocessing import calculateDistance
from Project.source_code.mysqlUtilities import insertRecord
from datetime import datetime, timedelta

pool_rides = list()
pool_window_time1 = 5
pool_window_time2 = 10
delay_factor_percent1 = 20
delay_factor_percent2 = 10


def generatePoolID():
    list_of_ids = list(range(1000))
    random.shuffle(list_of_ids)
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
    print(G.get_edge_data(1,3))
    return max_weight_matching.max_weight_matching(G),G


def generateTripID():
    tripIDS=list(range(3000))
    random.shuffle(tripIDS)
    return tripIDS.pop()




def pick_a_ride():
    # Form the pool map for the given pool window and source file
    pool_map = formPools(pool_window_time1, "temp_s.csv")

    # Get the list of rides within the pool window period
    for pool in pool_map:
        print("POOL ID ", pool)
        pool_shares = pool_map[pool]
        print("Number of requests ", len(pool_shares))
        number_of_rides = len(pool_shares)
        rideIDS = []
        for x in range(number_of_rides):
            rideIDS.append(pool_shares[x]['RideID'])

        start_time = datetime.utcnow()
        comb = combinations(pool_shares, 2)
        final_array = []
        # Print the obtained combinations
        for ride_combination in list(comb):
            rideA = ride_combination[0]
            rideB = ride_combination[1]
            localData = dict()
            distance_saved = sharing_condition(rideA, rideB)
            # store the values with ride id a and b along with final=the max distance saved
            localData["D1"] = rideA['RideID'];
            localData["D2"] = rideB['RideID'];
            localData["D_save"] = distance_saved
            final_array.append(localData)

        print(final_array)

        # Form graph and get best set of edges for ride-sharing
        best_nodes,Graph = formAdjacencyGraph(final_array)

        total_distance_saved=0
        for nodes in best_nodes:
            if nodes[0] in rideIDS:
                rideIDS.remove(nodes[0])
            if nodes[1] in rideIDS:
                rideIDS.remove(nodes[1])
            total_distance_saved=total_distance_saved+Graph.get_edge_data(nodes[0],nodes[1])['weight']
        total_distance_saved=float(total_distance_saved)
        end_time = datetime.utcnow()
        difference = end_time - start_time
        difference = float(difference.total_seconds())
        print("Time taken in seconds ",difference)
        # store in db
        pool_insert_query= "insert into pool_details (pool_id,count_of_rides,time_taken,dist_saved) values (" + \
               str(pool)+","+str(len(best_nodes))+","+str(difference)+","+ str(total_distance_saved)+");"
        print(pool_insert_query)
        database_response=insertRecord(pool_insert_query)
        print(database_response)
        tripID= generateTripID()
        # Insert ride-sharable requests as
        for nodes in best_nodes:
            trip_detail_insert_query="insert into trip_details (trip_id,pool_id,ride_t_id,isRideShared) values (" + \
                str(tripID)+","+str(pool)+","+str(nodes[0])+","+"True"
            insertRecord(trip_detail_insert_query)
            trip_detail_insert_query="insert into trip_details (trip_id,pool_id,ride_t_id,isRideShared) values (" + \
                                     str(tripID)+","+str(pool)+","+str(nodes[1])+","+"True"
            insertRecord(trip_detail_insert_query)

        # insert records that are not ride-shared
        for rideID in rideIDS:
            trip_detail_insert_query="insert into trip_details (trip_id,pool_id,ride_t_id,isRideShared) values (" + \
                                     str(tripID)+","+str(pool)+","+str(rideID)+","+"True"
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

    A_delay = str(rideA['DelayFactor']).strip()
    B_delay = str(rideB['DelayFactor']).strip()

    A_delay = float(A_delay)
    B_delay = float(B_delay)

    # print(type(A_delay))

    AB = calculateDistance(A_dlat, A_dlon, B_dlat, B_dlon)
    HA = calculateDistance(A_plat, A_plon, A_dlat, A_dlon)
    HB = calculateDistance(B_plat, B_plon, B_dlat, B_dlon)
    print(AB, HA, HB)
    distance_saved = 0
    if HA + AB < HA + HB:
        if HA + AB <= HB + B_delay:
            distance_saved = HB - AB
    if HB + AB < HA + HB:
        if HB + AB <= HA + A_delay:
            distance_saved = HA - AB

    return distance_saved
    # source to d1 and d2 - findSS the optimal route
    # enter the value from d1 to d2 in matrix format i.e the dist saved


def select_Rides_pool(pool_time: int):
    # Read pool requests from csv file
    data = pd.read_csv('temp_s.csv', index_col=False)
    # sort the data with respect to start time
    data = data.sort_values(by='tpep_pickup_datetime')
    data.reset_index(inplace=True)
    # delete the index
    del data['index']
    print(data.head())
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


def formPools(pool_time: int, fileName: str):
    global pool_rides
    # Read pool requests from csv file
    data = pd.read_csv(fileName, index_col=False)
    # sort the data with respect to start time
    data = data.sort_values(by='tpep_pickup_datetime')
    data.reset_index(inplace=True)
    # delete the index
    del data['index']

    print(data.head())
    start_time = data['tpep_pickup_datetime'].iloc[0]
    # dataframe will have first ride which has not yet been rideshared.
    # The starting trip time of the first trip from the pool of requests
    pool_beiginning_time = start_time
    pool_beiginning_time = datetime.strptime(pool_beiginning_time, '%d-%m-%Y %H:%M')
    pool_ending_time = pool_beiginning_time + timedelta(minutes=pool_time)
    poolMap = dict()
    poolRequests = list()
    for index, row in data.iterrows():
        if type(row["tpep_pickup_datetime"]) is float:
            continue
        print(row["tpep_pickup_datetime"])

        if pool_beiginning_time.strftime("%d-%m-%Y %H:%M") <= row['tpep_pickup_datetime'] < pool_ending_time.strftime(
                "%d-%m-%Y %H:%M"):
            poolRequests.append(row)
        else:
            poolID = generatePoolID()
            poolMap[poolID] = poolRequests
            poolRequests = list()
            poolRequests.append(row)
            pool_beiginning_time = datetime.strptime(row["tpep_pickup_datetime"], '%d-%m-%Y %H:%M')
            pool_ending_time = pool_beiginning_time + timedelta(minutes=pool_time)
            # 12 3456
    # Add the pending request as another pool entry
    if len(poolRequests) > 0:
        poolID = generatePoolID()
        poolMap[poolID] = poolRequests
        # poolRequests.clear()

    print(poolMap)
    return poolMap


# sorting_starttime()
# select_Rides_pool()
print("Starting")
start_time = time.time()
pick_a_ride()
print("--- %s seconds ---" % (time.time() - start_time))
# find_best_pair()
