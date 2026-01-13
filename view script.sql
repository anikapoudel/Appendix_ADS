--------------------------------------------------------------
--  VIEWS SCRIPT --
--------------------------------------------------------------

-- Drop all views --
BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW vw_data_quality_summary';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW vw_crime_by_station';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW vw_crime_by_type';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW vw_crime_status';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW vw_station_transformations';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW vw_status_transformations';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW vw_executive_dashboard';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

--------------------------------------------------------------
-- 1. DATA QUALITY VIEWS
--------------------------------------------------------------

-- View 1: Good vs Bad Data Summary
CREATE OR REPLACE VIEW vw_data_quality_summary AS
SELECT 
    'STATION' as entity_type,
    (SELECT COUNT(*) FROM gd_station) as good_count,
    (SELECT COUNT(*) FROM bd_station) as bad_count,
    (SELECT COUNT(*) FROM stg_station) as total_staging,
    (SELECT COUNT(*) FROM tr_station) as transformed_count
FROM DUAL
UNION ALL
SELECT 
    'STATUS',
    (SELECT COUNT(*) FROM gd_status),
    (SELECT COUNT(*) FROM bd_status),
    (SELECT COUNT(*) FROM stg_status),
    (SELECT COUNT(*) FROM tr_status)
FROM DUAL
UNION ALL
SELECT 
    'CRIME_TYPE',
    (SELECT COUNT(*) FROM gd_crimetype),
    (SELECT COUNT(*) FROM bd_crimetype),
    (SELECT COUNT(*) FROM stg_crimetype),
    (SELECT COUNT(*) FROM tr_crimetype)
FROM DUAL;


--------------------------------------------------------------
-- 2. BUSINESS REPORTING VIEWS
--------------------------------------------------------------

-- View 2: Crime Statistics by Station
CREATE OR REPLACE VIEW vw_crime_by_station AS
SELECT 
    ds.station_name,
    ds.region,
    COUNT(fc.crime_id) as total_crimes,
    ROUND(COUNT(fc.crime_id) * 100.0 / NULLIF((SELECT COUNT(*) FROM FACT_CRIME), 0), 2) as percentage
FROM DIM_STATION ds
LEFT JOIN FACT_CRIME fc ON ds.station_key = fc.station_key
GROUP BY ds.station_name, ds.region
ORDER BY total_crimes DESC;

-- View 3: Crime Statistics by Type
CREATE OR REPLACE VIEW vw_crime_by_type AS
SELECT 
    dct.crime_type_name,
    COUNT(fc.crime_id) as total_cases,
    COUNT(DISTINCT fc.station_key) as stations_affected
FROM DIM_CRIMETYPE dct
LEFT JOIN FACT_CRIME fc ON dct.crime_type_key = fc.crime_type_key
GROUP BY dct.crime_type_name
ORDER BY total_cases DESC;

-- View 4: Crime Status Summary
CREATE OR REPLACE VIEW vw_crime_status AS
SELECT 
    dst.status_name,
    COUNT(fc.crime_id) as total_crimes,
    ROUND(COUNT(fc.crime_id) * 100.0 / NULLIF((SELECT COUNT(*) FROM FACT_CRIME), 0), 2) as percentage
FROM DIM_STATUS dst
LEFT JOIN FACT_CRIME fc ON dst.status_key = fc.status_key
GROUP BY dst.status_name
ORDER BY total_crimes DESC;

--------------------------------------------------------------
-- 3. TRANSFORMATION VIEWS
--------------------------------------------------------------

-- View 5: Station Transformations
CREATE OR REPLACE VIEW vw_station_transformations AS
SELECT 
    ts.station_id,
    ts.station_name as original_name,
    ts.station_name_standard as standardized_name,
    ts.region as original_region,
    ts.region_standard as standardized_region,
    ts.source_system,
    ts.transformation_notes
FROM tr_station ts
ORDER BY ts.station_id;

-- View 6: Status Transformations
CREATE OR REPLACE VIEW vw_status_transformations AS
SELECT 
    ts.status_id,
    ts.status_name as original_status,
    ts.status_name_standard as standardized_status,
    ts.status_description,
    ts.source_system
FROM tr_status ts
ORDER BY ts.status_id;

--------------------------------------------------------------
-- 4. EXECUTIVE VIEW
--------------------------------------------------------------

-- View 7: Executive Dashboard Summary
CREATE OR REPLACE VIEW vw_executive_dashboard AS
SELECT 
    (SELECT COUNT(*) FROM FACT_CRIME) as total_crimes,
    (SELECT COUNT(DISTINCT station_key) FROM FACT_CRIME) as active_stations,
    (SELECT COUNT(DISTINCT crime_type_key) FROM FACT_CRIME) as crime_types,
    (SELECT COUNT(*) FROM FACT_CRIME WHERE status_key IN 
        (SELECT status_key FROM DIM_STATUS WHERE status_name = 'OPEN')) as open_cases,
    (SELECT COUNT(*) FROM gd_station) as quality_stations,
    ROUND((SELECT COUNT(*) FROM gd_station) * 100.0 / 
          NULLIF((SELECT COUNT(*) FROM stg_station), 0), 2) as data_quality_score
FROM DUAL;

