-- 1. Check staging tables
SELECT * FROM stg_station;
SELECT * FROM stg_status;
SELECT * FROM stg_crimetype;

--2.Check good / bad tables
SELECT * FROM gd_station;
SELECT * FROM bd_station;

SELECT * FROM gd_status;
SELECT * FROM bd_status;

SELECT * FROM gd_crimetype;
SELECT * FROM bd_crimetype;


--3.Check transformation tables
SELECT * FROM tr_station;
SELECT * FROM tr_status;
SELECT * FROM tr_crimetype;

--4. Check dimension tables
SELECT * FROM DIM_STATION;
SELECT * FROM DIM_STATUS;
SELECT * FROM DIM_CRIMETYPE;
SELECT * FROM DIM_TIME;

--5. Check fact table
SELECT * FROM FACT_CRIME;

--6. Check views
SELECT * FROM vw_data_quality_summary;
SELECT * FROM vw_crime_by_station;
SELECT * FROM vw_crime_by_type;
SELECT * FROM vw_crime_status;
SELECT * FROM vw_station_transformations;
SELECT * FROM vw_status_transformations;
SELECT * FROM vw_executive_dashboard;






