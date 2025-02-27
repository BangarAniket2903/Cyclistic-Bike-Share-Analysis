CREATE TABLE trips_raw (
    ride_id TEXT,
    rideable_type TEXT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name TEXT,
    start_station_id TEXT,
    end_station_name TEXT,
    end_station_id TEXT,
    start_lat NUMERIC,
    start_lng NUMERIC,
    end_lat NUMERIC,
    end_lng NUMERIC,
    member_casual TEXT
);

--- total records
select count(*) from trips_raw;


-- check missing data 

SELECT 
    COUNT(*) FILTER (WHERE ride_id IS NULL) AS missing_ride_id,
    COUNT(*) FILTER (WHERE rideable_type IS NULL) AS missing_rideable_type,
    COUNT(*) FILTER (WHERE started_at IS NULL) AS missing_started_at,
    COUNT(*) FILTER (WHERE ended_at IS NULL) AS missing_ended_at,
    COUNT(*) FILTER (WHERE start_station_name IS NULL) AS missing_start_station_name,
    COUNT(*) FILTER (WHERE start_station_id IS NULL) AS missing_start_station_id,
    COUNT(*) FILTER (WHERE end_station_name IS NULL) AS missing_end_station_name,
    COUNT(*) FILTER (WHERE end_station_id IS NULL) AS missing_end_station_id,
    COUNT(*) FILTER (WHERE start_lat IS NULL) AS missing_start_lat,
    COUNT(*) FILTER (WHERE start_lng IS NULL) AS missing_start_lng,
    COUNT(*) FILTER (WHERE end_lat IS NULL) AS missing_end_lat,
    COUNT(*) FILTER (WHERE end_lng IS NULL) AS missing_end_lng,
    COUNT(*) FILTER (WHERE member_casual IS NULL) AS missing_member_casual
FROM trips_raw;


-- station name and station id both have null values, so we can not map names
-- remove the missing values 

SELECT COUNT(*) 
FROM trips_raw
WHERE start_station_name IS NULL 
AND start_station_id IS NULL;         -- 1073951 rows 

DELETE FROM trips_raw
WHERE start_station_name IS NULL 
AND start_station_id IS NULL;

SELECT COUNT(*) 
FROM trips_raw

SELECT COUNT(*) 
FROM trips_raw
WHERE end_station_name IS NULL 
AND end_station_id IS NULL;      -- 578308

DELETE FROM trips_raw
WHERE end_station_name IS NULL 
AND end_station_id IS NULL;


-- duplicate records

SELECT ride_id, COUNT(*) 
FROM trips_raw
GROUP BY ride_id 
HAVING COUNT(*) > 1;    -- 121 rows 

DELETE FROM trips_raw
WHERE ride_id IN (
    SELECT ride_id 
    FROM trips_raw
    GROUP BY ride_id
    HAVING COUNT(*) > 1
);


-- check data consistency

SELECT COUNT(*) 
FROM trips_raw 
WHERE started_at >= ended_at;    -- 213 records 


DELETE FROM trips_raw WHERE started_at >= ended_at;


-- final check

SELECT COUNT(*) 
FROM trips_raw
WHERE ride_id IS NULL OR started_at IS NULL OR ended_at IS NULL;

SELECT ride_id, COUNT(*) 
FROM trips_raw
GROUP BY ride_id 
HAVING COUNT(*) > 1;

SELECT COUNT(*) 
FROM trips_raw 
WHERE started_at >= ended_at;

select count(*) from trips_raw ;  


-- backup data 

CREATE TABLE trips_raw_backup AS 
SELECT * FROM trips_raw;

SELECT COUNT(*) FROM trips_raw_backup;
SELECT COUNT(*) FROM trips_raw;

--- drop unwanted columns 

ALTER TABLE trips_raw 
DROP COLUMN start_station_id,
DROP COLUMN end_station_id,
DROP COLUMN start_lat,
DROP COLUMN start_lng,
DROP COLUMN end_lat,
DROP COLUMN end_lng;

SELECT column_name FROM information_schema.columns 
WHERE table_name = 'trips_raw';


--- EDA ----

--- Check total rides for casual riders vs. members
SELECT 
    TotalTrips,
    TotalMemberTrips,
    TotalCasualTrips,
    ROUND(TotalMemberTrips * 100.0 / TotalTrips, 2) AS MemberPercentage,
    ROUND(TotalCasualTrips * 100.0 / TotalTrips, 2) AS CasualPercentage
FROM 
    (
    SELECT
        COUNT(ride_id) AS TotalTrips,
        COUNT(ride_id) FILTER (WHERE member_casual = 'member') AS TotalMemberTrips,
        COUNT(ride_id) FILTER (WHERE member_casual = 'casual') AS TotalCasualTrips
    FROM
        trips_raw
    ) AS TripCounts;


--ride duration (avg) for both user types 

SELECT member_casual, 
       ROUND(AVG(EXTRACT(MINUTE FROM (ended_at - started_at))), 2) AS avg_ride_duration
FROM trips_raw
GROUP BY member_casual;

-- optimized solution for above query
SELECT member_casual, 
       ROUND(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)) / 60.0), 2) AS avg_ride_duration
FROM trips_raw
GROUP BY member_casual;


--- ride duration greater than 12 hours as this act as a outliers 
DELETE FROM trips_raw 
WHERE EXTRACT(EPOCH FROM ended_at - started_at) / 60 > 720;   -- 2500 records 

select count(*) FROM trips_raw 
WHERE EXTRACT(EPOCH FROM ended_at - started_at) / 60 < 5   -- 811581 records 


-- Identify peak hours for both user types
SELECT EXTRACT(HOUR FROM started_at) AS s_hour, 
       member_casual, 
       COUNT(ride_id) AS ride_count
FROM trips_raw
GROUP BY s_hour, member_casual
ORDER BY ride_count desc;

--- uses group by 1,2
SELECT EXTRACT(HOUR FROM started_at) AS s_hour, 
       member_casual, 
       COUNT(*) AS ride_count
FROM trips_raw
GROUP BY 1,2
ORDER BY ride_count DESC;


--Check usage patterns by day of the week
-- dow 0 for sunday, 6 for saturday
SELECT TO_CHAR(started_at, 'Day') AS day_of_week, 
       COUNT(ride_id) AS total_trips,
       COUNT(CASE WHEN member_casual = 'member' THEN ride_id END) AS member_trips,
       COUNT(CASE WHEN member_casual = 'casual' THEN ride_id END) AS casual_trips
FROM trips_raw
GROUP BY day_of_week
ORDER BY total_trips DESC;


SELECT EXTRACT(DOW FROM started_at) AS day_index, 
       TO_CHAR(started_at, 'Day') AS day_of_week, 
       COUNT(ride_id) AS total_trips,
       COUNT(CASE WHEN member_casual = 'member' THEN ride_id END) AS member_trips,
       COUNT(CASE WHEN member_casual = 'casual' THEN ride_id END) AS casual_trips
FROM trips_raw
GROUP BY day_index, day_of_week
ORDER BY day_index;


--Find the most popular start stations for both user types
SELECT start_station_name, 
       COUNT(ride_id) AS total_trips,
       COUNT(CASE WHEN member_casual = 'member' THEN ride_id END) AS member_trips,
       COUNT(CASE WHEN member_casual = 'casual' THEN ride_id END) AS casual_trips
FROM trips_raw
WHERE start_station_name IS NOT NULL
GROUP BY start_station_name
ORDER BY total_trips DESC
LIMIT 10;

-- top 10 most populer station for casual riders 

select start_station_name, count(*) as ride_counts
from trips_raw
where member_casual = 'casual'
group by 1
order by ride_counts desc
limit 10;


-- top 10 most populer station for member riders 

select start_station_name, count(*) as ride_counts
from trips_raw
where member_casual = 'member'
group by 1
order by ride_counts desc
limit 10;


--Find the most common routes (start to end station)
SELECT start_station_name, 
       end_station_name, 
       member_casual, 
       COUNT(ride_id) AS ride_count
FROM trips_raw
WHERE start_station_name IS NOT NULL AND end_station_name IS NOT NULL
GROUP BY start_station_name, end_station_name, member_casual
ORDER BY ride_count DESC
LIMIT 10;


-- Top 10 Most Popular Start Stations for member Riders

SELECT start_station_name, 
       COUNT(ride_id) AS member_trips
FROM trips_raw
WHERE member_casual = 'member' AND start_station_name IS NOT NULL
GROUP BY start_station_name
ORDER BY member_trips DESC
LIMIT 10;


-- Top 10 Most Popular Start Stations for Casual Riders    --- for converion to memberships by offering special offer

SELECT start_station_name, 
       COUNT(ride_id) AS casual_trips
FROM trips_raw
WHERE member_casual = 'casual' AND start_station_name IS NOT NULL
GROUP BY start_station_name
ORDER BY casual_trips DESC
LIMIT 10;


--- biketype usage

SELECT rideable_type AS ride_type, 
       COUNT(CASE WHEN member_casual = 'member' THEN ride_id END) AS member_rides,
       COUNT(CASE WHEN member_casual = 'casual' THEN ride_id END) AS casual_rides,
       COUNT(ride_id) AS total_rides
FROM trips_raw
GROUP BY rideable_type
ORDER BY total_rides DESC;


--- total rides per month for members 

select extract(month from started_at) as ride_month , count(*) as ride_count
from trips_raw
where member_casual = 'member'
group by ride_month
order by ride_count desc;


--- total rides per month for casuals

select extract(month from started_at) as ride_month , count(*) as ride_count
from trips_raw
where member_casual = 'casual'
group by ride_month
order by ride_count desc;

--- seasonal trend analysis

SELECT 
    CASE 
        WHEN EXTRACT(MONTH FROM started_at) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM started_at) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM started_at) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM started_at) IN (9, 10, 11) THEN 'Fall'
    END AS season,
    member_casual,
    COUNT(*) AS total_rides
FROM trips_raw
GROUP BY season, member_casual
ORDER BY total_rides DESC
limit 20;







