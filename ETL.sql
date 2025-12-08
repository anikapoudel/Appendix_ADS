-- ==============================================================
-- ETL Script for PRCS Data Mart
-- Task 3.2
-- This script populates DIM_STATION, DIM_STATUS, DIM_CRIMETYPE, DIM_TIME,
-- and the FACT_CRIME table using staging, good/bad, and transformation tables.
-- ==============================================================

--------------------------------------------------------------
-- 0. DROP TABLES AND SEQUENCES IF THEY EXIST
-- This ensures that the ETL can be rerun multiple times without errors
--------------------------------------------------------------
BEGIN
  -- Drop existing tables
  FOR t IN (SELECT table_name FROM user_tables WHERE table_name IN (
      'STG_STATION','STG_STATUS','STG_CRIMETYPE',
      'GD_STATION','GD_STATUS','GD_CRIMETYPE',
      'BD_STATION','BD_STATUS','BD_CRIMETYPE',
      'TR_STATION','TR_STATUS','TR_CRIMETYPE',
      'DIM_STATION','DIM_STATUS','DIM_CRIMETYPE','DIM_TIME','FACT_CRIME'
  )) LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' CASCADE CONSTRAINTS';
  END LOOP;

  -- Drop existing sequences
  FOR s IN (SELECT sequence_name FROM user_sequences WHERE sequence_name IN (
      'TIME_SEQ','FACT_SEQ','CRIMETYPE_SEQ'
  )) LOOP
    EXECUTE IMMEDIATE 'DROP SEQUENCE ' || s.sequence_name;
  END LOOP;
EXCEPTION WHEN OTHERS THEN
  NULL; -- Ignore errors if tables/sequences do not exist
END;
/
COMMIT;

--------------------------------------------------------------
-- 1. CREATE STAGING TABLES
-- Staging tables temporarily hold raw data from source tables for ETL processing.
--------------------------------------------------------------
CREATE TABLE stg_station AS
SELECT station_id, station_name, fk1_area_id
FROM pl_station;

CREATE TABLE stg_status AS
SELECT reported_crime_id AS crime_id, crime_status
FROM pl_reported_crime;

CREATE TABLE stg_crimetype AS
SELECT crime_type_id, crime_type_desc
FROM pl_crime_type;

COMMIT;

--------------------------------------------------------------
-- 2. CREATE GOOD DATA TABLES
-- Good tables contain records that pass basic validation rules.
--------------------------------------------------------------
CREATE TABLE gd_station AS
SELECT *
FROM stg_station
WHERE station_id IS NOT NULL
  AND station_name IS NOT NULL;

CREATE TABLE gd_status AS
SELECT *
FROM stg_status
WHERE crime_id IS NOT NULL
  AND crime_status IN ('OPEN','CLOSED');

CREATE TABLE gd_crimetype AS
SELECT *
FROM stg_crimetype
WHERE crime_type_id IS NOT NULL
  AND crime_type_desc IS NOT NULL;

COMMIT;

--------------------------------------------------------------
-- 3. CREATE BAD DATA TABLES
-- Bad tables contain records that fail validation rules and need review or correction.
--------------------------------------------------------------
CREATE TABLE bd_station AS
SELECT *
FROM stg_station
WHERE station_id IS NULL
   OR station_name IS NULL;

CREATE TABLE bd_status AS
SELECT *
FROM stg_status
WHERE crime_id IS NULL
   OR crime_status NOT IN ('OPEN','CLOSED');

CREATE TABLE bd_crimetype AS
SELECT *
FROM stg_crimetype
WHERE crime_type_id IS NULL
   OR crime_type_desc IS NULL;

COMMIT;

--------------------------------------------------------------
-- 4. TRANSFORMATION TABLES
-- Apply basic transformations such as formatting, trimming, and casing.
--------------------------------------------------------------
CREATE TABLE tr_station AS
SELECT station_id,
       INITCAP(station_name) AS station_name, -- Capitalize station names
       fk1_area_id
FROM gd_station;

CREATE TABLE tr_status AS
SELECT crime_id,
       UPPER(crime_status) AS status_desc     -- Convert statuses to uppercase
FROM gd_status;

CREATE TABLE tr_crimetype AS
SELECT crime_type_id,
       TRIM(crime_type_desc) AS crime_type_desc -- Remove leading/trailing spaces
FROM gd_crimetype;

COMMIT;

--------------------------------------------------------------
-- 5. CREATE DIMENSION TABLES
-- Dimensions store unique master data for analysis.
--------------------------------------------------------------
CREATE TABLE dim_station (
    station_id NUMBER PRIMARY KEY,
    station_name VARCHAR2(100),
    fk_area_id NUMBER
);

CREATE TABLE dim_status (
    status_id NUMBER PRIMARY KEY,
    status_desc VARCHAR2(20)
);

CREATE TABLE dim_crimetype (
    crime_type_id NUMBER PRIMARY KEY,
    crime_type_desc VARCHAR2(50)
);

-- Populate dimension tables from transformed data
INSERT INTO dim_station (station_id, station_name, fk_area_id)
SELECT station_id, station_name, fk1_area_id
FROM tr_station;

INSERT INTO dim_status (status_id, status_desc)
SELECT rownum, status_desc
FROM tr_status; 

INSERT INTO dim_crimetype (crime_type_id, crime_type_desc)
SELECT crime_type_id, crime_type_desc
FROM tr_crimetype;

COMMIT;

--------------------------------------------------------------
-- 6. CREATE DIM_TIME
-- Time dimension stores dates for time-based analysis.
--------------------------------------------------------------
CREATE SEQUENCE time_seq START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE dim_time (
    time_key NUMBER PRIMARY KEY,
    date_value DATE,
    year_val NUMBER,
    month_val NUMBER,
    day_val NUMBER,
    quarter_val NUMBER
);

-- Populate dim_time with distinct dates from reported crimes
INSERT INTO dim_time (time_key, date_value, year_val, month_val, day_val, quarter_val)
SELECT time_seq.NEXTVAL,
       date_reported,
       EXTRACT(YEAR FROM date_reported),
       EXTRACT(MONTH FROM date_reported),
       EXTRACT(DAY FROM date_reported),
       CEIL(EXTRACT(MONTH FROM date_reported)/3) -- Determine quarter
FROM (SELECT DISTINCT date_reported FROM pl_reported_crime WHERE date_reported IS NOT NULL);

COMMIT;

--------------------------------------------------------------
-- 7. CREATE FACT_CRIME TABLE
-- Stores measures for analytical reporting.
--------------------------------------------------------------
CREATE SEQUENCE fact_seq START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE FACT_CRIME (
  reported_crime_id INTEGER PRIMARY KEY, -- Surrogate key
  time_key INTEGER,                      -- FK to dim_time
  fk_station_id INTEGER,                  -- FK to dim_station
  fk_crime_type_id INTEGER,              -- FK to dim_crimetype
  fk_status VARCHAR2(20),                -- FK to dim_status
  number_of_cases INTEGER                -- Measure: count of cases
);

--------------------------------------------------------------
-- 8. POPULATE FACT_CRIME WITH 15 SAMPLE ROWS
-- Demonstrates ETL population using sequences and cycling through dimensions.
--------------------------------------------------------------
DECLARE
  v_time_count       NUMBER;
  v_station_count    NUMBER;
  v_crimetype_count  NUMBER;
BEGIN
  -- Get counts from DIM tables
  SELECT COUNT(*) INTO v_time_count FROM DIM_TIME;
  SELECT COUNT(*) INTO v_station_count FROM DIM_STATION;
  SELECT COUNT(*) INTO v_crimetype_count FROM DIM_CRIMETYPE;

  FOR i IN 1..15 LOOP
    INSERT INTO FACT_CRIME (
      reported_crime_id,
      time_key,
      fk_station_id,
      fk_crime_type_id,
      fk_status,
      number_of_cases
    )
    VALUES (
      fact_seq.NEXTVAL,                          -- Surrogate key
      MOD(i-1, v_time_count) + 1,                -- Cycle through time dimension
      MOD(i-1, v_station_count) + 1,             -- Cycle through station dimension
      MOD(i-1, v_crimetype_count) + 101,         -- Cycle through crime type (adjust as needed)
      CASE WHEN MOD(i,2)=0 THEN 'OPEN' ELSE 'CLOSED' END,  -- Alternate status
      1  -- Sample measure: one case per row
    );
  END LOOP;

  COMMIT;
END;
/
