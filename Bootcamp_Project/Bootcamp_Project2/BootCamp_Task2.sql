-- Databricks notebook source
describe taxi_data;

-- COMMAND ----------

select * from taxi_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###1.	Determine the average trip distance for each hour of the day.

-- COMMAND ----------

select extract(hour from lpep_pickup_datetime) as day_hour, round(avg(trip_distance),3) as average_trip_distance from taxi_data
group by day_hour
order by day_hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2.Find the percentage of trips where the passenger count is greater than the average passenger count for all trips.

-- COMMAND ----------

with people_count as (select count(passenger_count) as count_of_passenger from taxi_data where passenger_count > (select avg(passenger_count)from taxi_data))

select round(count_of_passenger * 100 / (select count(*) as total_no_of_trips from taxi_data),3) as percentage_of_trips from people_count;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3.	Identify the top 3 busiest pickup locations based on the total number of trips.

-- COMMAND ----------

select PULocationID, count(*) as total_no_of_trips from taxi_data 
group by PULocationID order by total_no_of_trips desc
limit 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###4.	Calculate the median fare amount for trips with different rate codes.

-- COMMAND ----------

select RatecodeID , median(fare_amount) from taxi_data where RatecodeID is not null
group by RatecodeID order by RatecodeID ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###5.	Calculate the average trip distance for trips where the store and forward flag is enabled

-- COMMAND ----------

select round(avg(trip_distance),3) as average_trip_distance from taxi_data where store_and_fwd_flag = 'Y';
