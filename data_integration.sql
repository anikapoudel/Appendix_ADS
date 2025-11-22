-----------------------------------------------------
-- Task 3.1 – Data Integration
-- Staging tables to integrate PS_Wales data into PRCS system
-----------------------------------------------------

-- ----------------------------
-- Drop staging tables if exist
 DROP TABLE temp_stations CASCADE CONSTRAINTS;
 DROP TABLE temp_status CASCADE CONSTRAINTS;
 DROP TABLE temp_dates CASCADE CONSTRAINTS;


-- ----------------------------
-- Staging table for stations
-- ----------------------------
CREATE TABLE temp_stations AS
SELECT DISTINCT
    location_id AS station_id,
    city_name AS station_name,
    region_id
FROM LOCATION
WHERE location_id IS NOT NULL;

-- ----------------------------
-- Staging table for crime status
-- ----------------------------
CREATE TABLE temp_status AS
SELECT DISTINCT
    UPPER(TRIM(crime_status)) AS status_name
FROM CRIME_REGISTER
WHERE crime_status IS NOT NULL;

-- ----------------------------
-- Staging table for crime dates
-- ----------------------------
CREATE TABLE temp_dates AS
SELECT DISTINCT
    reported_date AS crime_date
FROM CRIME_REGISTER
WHERE reported_date IS NOT NULL;

