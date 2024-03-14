
In order to get a static set of results, we will use historical data from the dataset.
Run the following commands:
# Load the cluster op commands.
source commands.sh

python3 -m venv .venv
source .venv/bin/activate

# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka

New session
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
 
select column_name, data_type, character_maximum_length, column_default, is_nullable
from INFORMATION_SCHEMA.COLUMNS where table_name = ‘trip_data’;
 
 

#join trip_data and taxi_zone tables to use in the homework questions
Create MATERIALIZED VIEW trip_data_zone AS
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    taxi_zone_pu.Zone as pickup_zone,
    taxi_zone_do.Zone as dropoff_zone,
    trip_distance
FROM
    trip_data
        JOIN taxi_zone as taxi_zone_pu
             ON trip_data.PULocationID = taxi_zone_pu.location_id
        JOIN taxi_zone as taxi_zone_do
             ON trip_data.DOLocationID = taxi_zone_do.location_id;




select column_name, data_type, character_maximum_length, column_default, is_nullable
from INFORMATION_SCHEMA.COLUMNS where table_name = ‘trip_data_zone’;
 


Question 0
What are the dropoff taxi zones at the latest dropoff times?
CREATE MATERIALIZED VIEW LATEST_DROPOFF_TIME AS
WITH max_dropoff_time AS (SELECT max(tpep_dropoff_datetime) max FROM trip_data_zone) 
SELECT dropoff_zone,max, tpep_dropoff_datetime FROM trip_data_zone, max_dropoff_time
WHERE tpep_dropoff_datetime = max;

OR
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

Question 1
Create a materialized view to compute the average, min and max trip time between each taxi zone.
From this MV, find the pair of taxi zones with the highest average trip time
 
CREATE MATERIALIZED VIEW TRIP_TIME_STATS AS
select avg(tpep_dropoff_datetime-tpep_pickup_datetime) avg_trip_time,
min(tpep_dropoff_datetime-tpep_pickup_datetime) min_trip_time,
max(tpep_dropoff_datetime-tpep_pickup_datetime) max_trip_time,
count(*) count_trips,
pickup_zone,
dropoff_zone 
from trip_data_zone
group by pickup_zone,dropoff_zone
order by avg_trip_time desc
LIMIT 1;

select tpep_dropoff_datetime,tpep_pickup_datetime
from trip_data_zone
where pickup_zone='Yorkville East'
and dropoff_zone= 'Steinway';
Question 3
From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 12:00:00, then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.
HINT: You can use dynamic filter pattern to create a filter condition based on the latest pickup time.

WITH max_pickup_time AS (SELECT max(tpep_pickup_datetime) max FROM trip_data_zone) 
SELECT pickup_zone,count(*) FROM trip_data_zone, max_pickup_time
WHERE tpep_pickup_datetime between max - interval '17 hours' and max
group by pickup_zone
order by 2 desc
limit 3;
LaGuardia Airport, Lincoln Square East, JFK Airport
 
select tpep_pickup_datetime, tpep_pickup_datetime - interval '17 hours' from trip_data_zone limit 10;



