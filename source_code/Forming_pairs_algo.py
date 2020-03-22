import pandas as pd
import random
import time
from itertools import combinations
from ride_sharing.source_code.datapreprocessing import calculateDistance

def distance():
    random_distance = [10,2,5,4,2,6,7,2,7,9,2,3,6,7,2,6,9,3,6,9,3,4,6,7,4,3,6,6,5,4,6,7,3,6]
    return random_distance[random.randint(0,len(random_distance))]


"""
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
    """

def pick_a_ride():
    #select according to order
    pool_shares = select_Rides_pool()
    leng1=len(pool_shares)
    rideB=[]
    Dict={}
    for x in range(leng1):
        rideB.append(pool_shares['RideID'].iloc[x])
    #print(rideB)
    comb = combinations(rideB, 2)
    final_array=[]
    final_df = pd.DataFrame(columns=['D1', 'D2', 'D_save'])
    # Print the obtained combinations
    for i in list(comb):
        a=i[0]
        b=i[1]
        rideA=pool_shares.loc[pool_shares['RideID']==a]
        rideB=pool_shares.loc[pool_shares['RideID']==b]
        final=sharing_condition(rideA,rideB)
        #store the values with ride id a and b along with final=the max distance saved
        print("start",len(final_df))

        final_df=final_df.append({'D1':a,'D2':b,'D_save':final},ignore_index=True)
        final_df=final_df.sort_values(by='D_save',ascending=False)

        #print("best match",final_df.iloc[0])

    dest1 = final_df['D1'].iloc[0]
    dest2 = final_df['D2'].iloc[0]
    dsave = final_df['D_save'].iloc[0]
    print(dest1,dest2,dsave)
    final_df.drop(final_df[final_df['D_save']==dsave].index,inplace=True)
    final_df.drop(final_df[final_df['D1']==dest1].index,inplace=True)
    final_df.drop(final_df[final_df['D2']==dest2].index,inplace=True)
    final_df.drop(final_df[final_df['D1'] == dest2].index, inplace=True)
    final_df.drop(final_df[final_df['D2'] == dest1].index, inplace=True)
        #pool_shares.drop(pool_shares[pool_shares['RideID'] == rideA].index, inplace=True)

    print(final_df)
       # print(final_df)

    print(final_array)

def sharing_condition(rideA,rideB):
    #check for the 2 conditions

    A_dlat=(rideA['dropoff_latitude'].to_string(index=False)).strip()
    A_dlon=(rideA['dropoff_longitude'].to_string(index=False)).strip()
    A_plat = (rideA['pickup_latitude'].to_string(index=False)).strip()
    A_plon = (rideA['pickup_longitude'].to_string(index=False)).strip()
    B_dlat=(rideB['dropoff_latitude'].to_string(index=False)).strip()
    B_dlon=(rideB['dropoff_longitude'].to_string(index=False)).strip()
    B_plat = (rideB['pickup_latitude'].to_string(index=False)).strip()
    B_plon = (rideB['pickup_longitude'].to_string(index=False)).strip()
    AB=calculateDistance(A_dlat,A_dlon,B_dlat,B_dlon)
    HA=calculateDistance(A_plat,A_plon,A_dlat,A_dlon)
    HB=calculateDistance(B_plat,B_plon,B_dlat,B_dlon)
    #print(AB,HA,HB)
    arr=[]
    if HA+AB<HA+HB:
        arr.append(HB-AB)
    elif HB+AB<HA+HB:
        arr.append(HA-AB)
    final=max(arr)
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
    data = pd.read_csv('temp.csv', index_col=False)
    print(data.head())
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
print("Starting")
start_time = time.time()
pick_a_ride()
print("--- %s seconds ---" % (time.time() - start_time))
    #find_best_pair()