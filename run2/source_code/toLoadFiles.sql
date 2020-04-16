-- Script to load the date column in file properly

-- Create the below function in the ride_sharing database

DELIMITER $$

CREATE FUNCTION SPLIT_STR(
  x VARCHAR(255),
  delim VARCHAR(12),
  pos INT
)
RETURNS VARCHAR(255) DETERMINISTIC
BEGIN 
    RETURN REPLACE(SUBSTRING(SUBSTRING_INDEX(x, delim, pos),
       LENGTH(SUBSTRING_INDEX(x, delim, pos -1)) + 1),
       delim, '');
END$$

DELIMITER ;

-- drop table temp
CREATE TABLE x(
	RideID float NOT null primary key AUTO_INCREMENT,
	pickup_latitude float,
    pickup_longitude float,
    dropoff_latitude float,
    dropoff_longitude float,
    tpep_pickup_datetime varchar(100),
    dist_airport float
);

show columns from temp;

-- Load the excel data to the temp file. Before loading the file make sure the date format in excel is in dd-mm-yyyy HH:mm:ss format
-- Donot load the RideID from excel, the temp table auto generates this. Also, RideID in taxitrips isn't auto_increment for now. 
-- Once the loading is complete enable that value.

-- Creating a temporary table. Just so that anything goes wrong you could get the data from temp table again.
create temporary table temp_tt_secondary
select * from temp;

/* Ignore 
-- select DATE_FORMAT(STR_TO_DATE(tpep_pickup_datetime,'%d-%m-%Y %H:%i%:%s'),'%Y-%m-%d %H:%i%:%s') from temp_tt

-- select convert(DATE_FORMAT(STR_TO_DATE(SPLIT_STR(tpep_pickup_datetime,' ',1),'%d-%m-%Y'),'%Y-%m-%d'),varchar(50))
-- + ' ' + 
--  SPLIT_STR(tpep_pickup_datetime,' ',2) from temp_tt
*/

alter table temp_tt_secondary
add column timestamp varchar(10);

alter table temp_tt_secondary
add column date varchar(15);

alter table temp_tt_secondary
add column modified_date varchar(100);

show columns from temp_tt_secondary;

update temp_tt_secondary
set timestamp = SPLIT_STR(tpep_pickup_datetime,' ',2);

update temp_tt_secondary
set date = SPLIT_STR(tpep_pickup_datetime,' ',1);

-- select STR_TO_DATE(SPLIT_STR(tpep_pickup_datetime,' ',1),'%d-%m-%Y') from temp_tt_secondary

update temp_tt_secondary
set date = STR_TO_DATE(SPLIT_STR(tpep_pickup_datetime,' ',1),'%d-%m-%Y');

update temp_tt_secondary
set modified_date = concat(date,' ',timestamp);

-- After the below step, there shouldnt be any null values in date column and the statement should have executed with no warnings
alter table temp_tt_secondary
modify modified_date datetime;

show columns from temp_tt_secondary;

insert into taxitrips(RideID,pickup_latitude,pickup_longitude,dropoff_latitude,dropoff_longitude,tpep_pickup_datetime,dist_airport)
select RideID,pickup_latitude,pickup_longitude,dropoff_latitude,dropoff_longitude,tpep_pickup_datetime,dist_airport from temp_tt_secondary

-- Check by running the below step
-- select * from taxitrips where tpep_pickup_datetime is null

-- Ignore the below step
-- update taxitrips
-- inner join temp_tt_secondary on temp_tt_secondary.RideId = taxitrips.RideID
-- set taxitrips.tpep_pickup_datetime = temp_tt_secondary.modified_date

