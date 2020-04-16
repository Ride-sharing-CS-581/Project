import pandas as pd

def select_Rides_pool():
    """selects all the rides in pool
    Pool starting time is the first ride which is 
    is in the list
    Input : pool start time
    Output : Rides that are in the pool"""
    data = pd.read_csv('temp.csv')
    #Lenght of the pool window
    pool_time = 5
    #dataframe will have first ride which has not yet been rideshared.
    pool_beiginning_time = data['time'].iloc[0]
    pool_ending_time = pool_beiginning_time + pool_time
    # Finding all the rides which belongs to the pool window
    # Checking the codition pool_beginning_time >= pickup time <= pool_endin_time
    # Here we are assuming the pickup time as ride request time
    locs = (data['time'] > pool_beiginning_time ) & (data['time'] <= pool_ending_time)
    pool_rides = data[locs]
    return pool_rides

import random
def distance():
    random_distance = [10,2,5,4,2,6,7,2,7,9,2,3,6,7,2,6,9,3,6,9,3,4,6,7,4,3,6,6,5,4,6,7,3,6]
    return random_distance[random.randint(0,len(random_distance))]

d = {}
d [(1,2)] = 2

data = select_Rides_pool()
RideID = 88
print(data.iloc[data['RideID'] == RideID])