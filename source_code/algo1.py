import pandas as pd

import random



def distance():
    random_distance = [10,2,5,4,2,6,7,2,7,9,2,3,6,7,2,6,9,3,6,9,3,4,6,7,4,3,6,6,5,4,6,7,3,6]
    return random_distance[random.randint(0,len(random_distance))]

def pick_a_ride():
    #select according to order
    pool_shares = select_Rides_pool()
    #print(pool_shares)
    #print(pool_shares['RideID'])
    leng1=len(pool_shares)
    #print(leng1)
    comArr=[]
    Dict={}
    rideA=pool_shares['RideID'].iloc[0]
    for x in range(1,leng1):
        rideB=pool_shares['RideID'].iloc[x]
        final=sharing_condition(rideA,rideB)
        Dict[(rideA,rideB)]=final
    Ffinal=max(Dict.values())
    #print(Dict)
    #print("result",Ffinal)
    for rides,values in Dict.items():
        #print("inside for")
        if values==Ffinal:
            #print("inside if",Dict.values())
            pool_shares.drop(pool_shares[pool_shares['RideID'] == rideA].index, inplace=True)
            pool_shares.drop(pool_shares[pool_shares['RideID'] == rideB].index, inplace=True)
    #remove the respective pair of rides

    print("final length",len(pool_shares))

def sharing_condition(rideA,rideB):
    #check for the 2 conditions
    calculateDistance(rideA.pickup_longitude,rideA.pickup_latitude,rideB.dropoff_longitude,rideB.dropoff_latitude)
    HA=10
    HB=12
    AB=2
    arr=[]
    if HA+AB<HA+HB:
        arr.append(HA+AB)
    elif HB+AB<HA+HB:
        arr.append(HA+AB)
    final=min(arr)
    #print("final",final)
    return final
    #source to d1 and d2 - find the optimal route
    #enter the value from d1 to d2 in matrix format i.e the dist saved


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
    #print(pool_rides)
    return pool_rides
    #select the 1st time and find all the rides in that 4-5min pool end


   # sorting_starttime()
#select_Rides_pool()

pick_a_ride()
    #find_best_pair()