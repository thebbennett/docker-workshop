
# Homework 
**See this homework exercise**: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/01-docker-terraform/homework.md

### Question 3
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

**SQL**
```bash
docker compose exec pgdatabase psql -U root -d ny_taxi -c 
"SELECT COUNT(*) as trips_under_1_mile 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01 00:00:00'::TIMESTAMP 
     AND lpep_pickup_datetime < '2025-12-01 00:00:00'::TIMESTAMP 
     AND trip_distance::NUMERIC <= 1.0;"

```

**Answer**: 8007

### Question 4
Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

**SQL**
```bash
docker compose exec pgdatabase psql -U root -d ny_taxi -c 
"SELECT 
		DATE(lpep_pickup_datetime::TIMESTAMP) as pickup_date, 
		MAX(trip_distance::NUMERIC) as max_distance 
FROM green_taxi_trips 
WHERE trip_distance::NUMERIC < 100 
GROUP BY pickup_date 
ORDER BY max_distance 
DESC LIMIT 1;"
```

**Answer**
 pickup_date | max_distance 
-------------+--------------
 2025-11-14  |        88.03
(1 row)

### Question 5
Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

**SQL**
```SQL
SELECT 
    z.zone,
    SUM(g."total_amount"::NUMERIC) as total_revenue
FROM green_taxi_trips g
JOIN taxi_zones z ON g."PULocationID" = z.locationid
WHERE DATE(g."lpep_pickup_datetime"::TIMESTAMP) = '2025-11-18'
GROUP BY z.zone
ORDER BY total_revenue DESC
LIMIT 1;
```

**Answer**

        zone        | total_revenue 
-------------------+---------------
 East Harlem North |       9281.92


### Question 6
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

- JFK Airport
- Yorkville West
- East Harlem North
- LaGuardia Airport

**SQL**
```SQL
SELECT 
    dz.zone as dropoff_zone,
    MAX(g."tip_amount"::NUMERIC) as max_tip
FROM green_taxi_trips g
JOIN taxi_zones pz ON g."PULocationID" = pz.locationid
JOIN taxi_zones dz ON g."DOLocationID" = dz.locationid
WHERE pz.zone = 'East Harlem North'
  AND DATE(g."lpep_pickup_datetime"::TIMESTAMP) >= '2025-11-01'
  AND DATE(g."lpep_pickup_datetime"::TIMESTAMP) < '2025-12-01'
GROUP BY dz.zone
ORDER BY max_tip DESC
LIMIT 1;
```

**Answer**
  dropoff_zone  | max_tip 
----------------+---------
 Yorkville West |   81.89
(1 row)



