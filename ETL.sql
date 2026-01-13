--This script has package for 3 dim table's good data table & bad data table and transformaion table--

--------------------------------------------------------------
-- GOOD DATA AND BAD DATA TABLES
--------------------------------------------------------------
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE gd_station CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE bd_station CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE gd_status CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE bd_status CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE gd_crimetype CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE bd_crimetype CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP PACKAGE gd_bd_pkg';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

--------------------------------------------------------------
-- 2. CREATING gd_* and bd_* TABLES 
--------------------------------------------------------------

-- gd_station table
CREATE TABLE gd_station AS (
    SELECT 
        station_new_key,
        station_id,
        station_name,
        region,
        source_system,
        record_status,
        error_message
    FROM stg_station WHERE 1=0
);

-- bd_station table
CREATE TABLE bd_station AS (
    SELECT 
        station_new_key,
        station_id,
        station_name,
        region,
        source_system,
        record_status,
        error_message,
        CAST('ERROR' AS VARCHAR2(200)) as error_reason
    FROM stg_station WHERE 1=0
);

-- gd_status table
CREATE TABLE gd_status AS (
    SELECT 
        status_new_key,
        status_id,
        status_name,
        status_description,
        source_system,
        record_status,
        error_message
    FROM stg_status WHERE 1=0
);

-- bd_status table
CREATE TABLE bd_status AS (
    SELECT 
        status_new_key,
        status_id,
        status_name,
        status_description,
        source_system,
        record_status,
        error_message,
        CAST('ERROR' AS VARCHAR2(200)) as error_reason
    FROM stg_status WHERE 1=0
);

-- gd_crimetype table
CREATE TABLE gd_crimetype AS (
    SELECT 
        crimetype_new_key,
        crime_type_id,
        crime_type_name,
        crime_category,
        source_system,
        record_status,
        error_message
    FROM stg_crimetype WHERE 1=0
);

-- bd_crimetype table
CREATE TABLE bd_crimetype AS (
    SELECT 
        crimetype_new_key,
        crime_type_id,
        crime_type_name,
        crime_category,
        source_system,
        record_status,
        error_message,
        CAST('ERROR' AS VARCHAR2(200)) as error_reason
    FROM stg_crimetype WHERE 1=0
);

--------------------------------------------------------------
-- 3. CREATE gd_bd_pkg PACKAGE
--------------------------------------------------------------

CREATE OR REPLACE PACKAGE gd_bd_pkg AS
    PROCEDURE process_all_data;
    PROCEDURE process_station_data;
    PROCEDURE process_status_data;
    PROCEDURE process_crimetype_data;
END gd_bd_pkg;
/

CREATE OR REPLACE PACKAGE BODY gd_bd_pkg AS

    ----------------------------------------------------------
    -- PROCESS STATION DATA 
    ----------------------------------------------------------
    PROCEDURE process_station_data IS
    BEGIN
        -- Clear existing data
       -- Clear existing data
DELETE FROM gd_station;
DELETE FROM bd_station;

-- Insert GOOD station data
INSERT INTO gd_station
SELECT
    station_new_key,
    station_id,
    station_name,
    region,
    source_system,
    record_status,
    error_message
FROM stg_station
WHERE station_name IS NOT NULL
  AND region IS NOT NULL
  AND source_system IN ('PRCS', 'PS_WALES')
  AND REGEXP_LIKE(
        station_name,
        '^([A-Z][a-z]+)( [A-Z][a-z]+)*(, [A-Z][a-z]+( [A-Z][a-z]+)*)*$'
      )
  AND REGEXP_LIKE(
        region,
        '^([A-Z][a-z]+)( [A-Z][a-z]+)*(, [A-Z][a-z]+( [A-Z][a-z]+)*)*$'
      )
  AND LENGTH(station_name) <= 30;


-- Insert BAD station data 
INSERT INTO bd_station
SELECT 
    station_new_key,
    station_id,
    station_name,
    region,
    source_system,
    record_status,
    error_message,
    'Failed station data quality validation' AS error_reason
FROM stg_station 
WHERE station_new_key NOT IN (
    SELECT station_new_key 
    FROM gd_station
);
    
        COMMIT;
    END process_station_data;
    
    ----------------------------------------------------------
    -- PROCESS STATUS DATA 
    ----------------------------------------------------------
    PROCEDURE process_status_data IS
    BEGIN
        -- Clear existing data
        DELETE FROM gd_status;
        DELETE FROM bd_status;
        
        -- Insert GOOD status data
        INSERT INTO gd_status
        SELECT 
            status_new_key,
            status_id,
            status_name,
            status_description,
            source_system,
            record_status,
            error_message
        FROM stg_status
        WHERE status_name IS NOT NULL
          AND UPPER(status_name) IN ('OPEN', 'CLOSED', 'ESCALATE')
          AND source_system IN ('PRCS', 'PS_WALES');
        
        -- Insert BAD status data
        INSERT INTO bd_status
        SELECT 
            status_new_key,
            status_id,
            status_name,
            status_description,
            source_system,
            record_status,
            error_message,
            CASE 
                WHEN status_name IS NULL THEN 'Status name is NULL'
                WHEN UPPER(status_name) NOT IN ('OPEN', 'CLOSED', 'ESCALATE') THEN 'Invalid status value'
                WHEN source_system NOT IN ('PRCS', 'PS_WALES') THEN 'Invalid source system'
                ELSE 'Unknown data quality issue'
            END as error_reason
        FROM stg_status
        WHERE status_name IS NULL
           OR UPPER(status_name) NOT IN ('OPEN', 'CLOSED', 'ESCALATE')
           OR source_system NOT IN ('PRCS', 'PS_WALES');
        
        COMMIT;
    END process_status_data;
    
    ----------------------------------------------------------
    -- PROCESS CRIME TYPE DATA 
    ----------------------------------------------------------
    PROCEDURE process_crimetype_data IS
    BEGIN
        -- Clear existing data
        DELETE FROM gd_crimetype;
        DELETE FROM bd_crimetype;
        
        -- Insert GOOD crime type data
        INSERT INTO gd_crimetype
        SELECT 
            crimetype_new_key,
            crime_type_id,
            crime_type_name,
            crime_category,
            source_system,
            record_status,
            error_message
        FROM stg_crimetype
        WHERE crime_type_name IS NOT NULL
          AND crime_category IS NOT NULL
          AND source_system IN ('PRCS', 'PS_WALES')
          AND REGEXP_LIKE(
          crime_type_name,
        '^([A-Z][a-z]+)( [A-Z][a-z]+)*(, [A-Z][a-z]+( [A-Z][a-z]+)*)*$'
      )
         AND REGEXP_LIKE(
        crime_category,
        '^([A-Z][a-z]+)( [A-Z][a-z]+)*(, [A-Z][a-z]+( [A-Z][a-z]+)*)*$'
      )          AND LENGTH(crime_type_name) <= 50;
        
        -- Insert BAD crime type data
        INSERT INTO bd_crimetype
    SELECT
    s.crimetype_new_key,
    s.crime_type_id,
    s.crime_type_name,
    s.crime_category,
    s.source_system,
    s.record_status,
    s.error_message,
    'Failed crime type data quality validation' AS error_reason
    FROM stg_crimetype s
WHERE NOT EXISTS (
    SELECT 1
    FROM gd_crimetype g
    WHERE g.crimetype_new_key = s.crimetype_new_key
);

        
        COMMIT;
    END process_crimetype_data;
    
    ----------------------------------------------------------
    -- MAIN AND UTILITY PROCEDURES
    ----------------------------------------------------------
    PROCEDURE process_all_data IS
    BEGIN
        process_station_data;
        process_status_data;
        process_crimetype_data;
    END process_all_data;
    
    

END gd_bd_pkg;
/
BEGIN
    gd_bd_pkg.process_all_data;
END;
/

--------------------------------------------------------------
-- TRANSFORMATION TABLES (tr_*) - FOR CLEANED/STANDARDIZED DATA
--------------------------------------------------------------

--------------------------------------------------------------
-- 1. DROP EXISTING OBJECTS
--------------------------------------------------------------
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tr_station CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tr_status CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE tr_crimetype CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL; END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE tr_pkg';
EXCEPTION WHEN OTHERS THEN NULL; END;
/

--------------------------------------------------------------
-- 2. CREATE TRANSFORMATION TABLES (tr_*)
--------------------------------------------------------------

----- tr_station table (Cleaned/Standardized stations) -----
CREATE TABLE tr_station(
    tr_station_key INTEGER,
    station_id INTEGER NOT NULL,
    station_name VARCHAR(60) NOT NULL,
    station_name_standard VARCHAR(60) NOT NULL,  -- Standardized name
    region VARCHAR(50) NOT NULL,
    region_standard VARCHAR(50) NOT NULL,        -- Standardized region
    source_system VARCHAR(20) NOT NULL,
    transformation_notes VARCHAR(200),
    CONSTRAINT pk_tr_station PRIMARY KEY(tr_station_key)
);

----- tr_status table (Cleaned/Standardized statuses) -----
CREATE TABLE tr_status(
    tr_status_key INTEGER,
    status_id INTEGER NOT NULL,
    status_name VARCHAR(20) NOT NULL,
    status_name_standard VARCHAR(20) NOT NULL,   -- Always uppercase
    status_description VARCHAR(50) NOT NULL,
    source_system VARCHAR(20) NOT NULL,
    CONSTRAINT pk_tr_status PRIMARY KEY(tr_status_key)
);

----- tr_crimetype table (Cleaned/Standardized crime types) -----
CREATE TABLE tr_crimetype(
    tr_crimetype_key INTEGER,
    crime_type_id INTEGER NOT NULL,
    crime_type_name VARCHAR(50) NOT NULL,
    crime_type_name_standard VARCHAR(50) NOT NULL,  -- Standardized name
    crime_category VARCHAR(30) NOT NULL,
    crime_category_standard VARCHAR(30) NOT NULL,   -- Standardized category
    source_system VARCHAR(20) NOT NULL,
    business_rule_applied VARCHAR(100),
    CONSTRAINT pk_tr_crimetype PRIMARY KEY(tr_crimetype_key)
);

--------------------------------------------------------------
-- 3. CREATE SEQUENCES FOR SURROGATE KEYS
--------------------------------------------------------------
DROP SEQUENCE tr_station_seq;
DROP SEQUENCE tr_status_seq;
DROP SEQUENCE tr_crimetype_seq;

CREATE SEQUENCE tr_station_seq START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE tr_status_seq START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE tr_crimetype_seq START WITH 1 INCREMENT BY 1 NOCACHE;

--------------------------------------------------------------
-- 4. CREATE TRANSFORMATION PACKAGE
--------------------------------------------------------------

CREATE OR REPLACE PACKAGE tr_pkg AS
    PROCEDURE process_all_transformations;
    PROCEDURE process_tr_station;
    PROCEDURE process_tr_status;
    PROCEDURE process_tr_crimetype;
    PROCEDURE clear_all_tables;
END tr_pkg;
/

CREATE OR REPLACE PACKAGE BODY tr_pkg AS

    ----------------------------------------------------------
    -- TRANSFORM STATION DATA
    ----------------------------------------------------------
    PROCEDURE process_tr_station IS
    BEGIN
        DELETE FROM tr_station;
        
        INSERT INTO tr_station (
            tr_station_key,
            station_id,
            station_name,
            station_name_standard,
            region,
            region_standard,
            source_system,
            transformation_notes
        )
        SELECT 
            tr_station_seq.NEXTVAL,
            gs.station_id,
            gs.station_name,
            INITCAP(TRIM(gs.station_name)) as station_name_standard,
            gs.region,
            INITCAP(TRIM(gs.region)) as region_standard,
            gs.source_system,
            CASE 
                WHEN UPPER(gs.station_name) LIKE '%KING%' AND UPPER(gs.station_name) LIKE '%CROSS%' 
                THEN 'Standardized Kings Cross station naming'
                WHEN UPPER(gs.station_name) LIKE '%LAWNSWOOD%' 
                THEN 'Corrected Lawnswood capitalization'
                ELSE 'Standard naming convention applied'
            END as transformation_notes
        FROM bd_station gs;
        
        COMMIT;
    END process_tr_station;

    ----------------------------------------------------------
    -- TRANSFORM STATUS DATA 
    ----------------------------------------------------------
    PROCEDURE process_tr_status IS
    BEGIN
        DELETE FROM tr_status;
        
        INSERT INTO tr_status (
            tr_status_key,
            status_id,
            status_name,
            status_name_standard,
            status_description,
            source_system  
        )  
        SELECT 
            tr_status_seq.NEXTVAL,
            gs.status_id,
            gs.status_name,
            UPPER(gs.status_name) as status_name_standard,
            CASE UPPER(gs.status_name)
                WHEN 'OPEN' THEN 'Case is under active investigation'
                WHEN 'CLOSED' THEN 'Case has been resolved and closed'
                WHEN 'ESCALATE' THEN 'Case requires senior management attention'
                ELSE gs.status_description
            END as status_description,
            gs.source_system
        FROM gd_status gs;
        
        COMMIT;
    END process_tr_status;
    
    ----------------------------------------------------------
    -- TRANSFORM CRIME TYPE DATA
    ----------------------------------------------------------
    PROCEDURE process_tr_crimetype IS
    BEGIN
        DELETE FROM tr_crimetype;
        
        INSERT INTO tr_crimetype (
            tr_crimetype_key,
            crime_type_id,
            crime_type_name,
            crime_type_name_standard,
            crime_category,
            crime_category_standard,
            source_system,
            business_rule_applied
        )
        SELECT 
            tr_crimetype_seq.NEXTVAL,
            gc.crime_type_id,
            gc.crime_type_name,
            INITCAP(TRIM(gc.crime_type_name)) as crime_type_name_standard,
            gc.crime_category,
            CASE 
                WHEN gc.crime_category = 'White Collar Crime' THEN 'Financial Crime'
                WHEN gc.crime_category = 'Substance Offence' THEN 'Drug Offence'
                ELSE gc.crime_category
            END as crime_category_standard,
            gc.source_system,
            CASE 
                WHEN UPPER(gc.crime_type_name) LIKE '%ARMED%ROBBERY%' THEN 'Forced category: Armed Robbery → Violent Crime'
                WHEN UPPER(gc.crime_type_name) LIKE '%DRUG%' THEN 'Standardized: Drug-related offences'
                WHEN UPPER(gc.crime_type_name) LIKE '%DRUNK%' THEN 'Standardized: Public order offences'
                WHEN gc.crime_category = 'White Collar Crime' THEN 'Renamed: White Collar Crime → Financial Crime'
                WHEN gc.crime_category = 'Substance Offence' THEN 'Renamed: Substance Offence → Drug Offence'
                ELSE 'Standard naming conventions applied'
            END as business_rule_applied
        FROM bd_crimetype gc;
        
        COMMIT;
    END process_tr_crimetype;
    
    ----------------------------------------------------------
    -- MAIN AND UTILITY PROCEDURES
    ----------------------------------------------------------
    PROCEDURE process_all_transformations IS
    BEGIN
        process_tr_station;
        process_tr_status;
        process_tr_crimetype;
    END process_all_transformations;
    
    PROCEDURE clear_all_tables IS
    BEGIN
        DELETE FROM tr_station;
        DELETE FROM tr_status;
        DELETE FROM tr_crimetype;
        COMMIT;
    END clear_all_tables;

END tr_pkg;
/

--------------------------------------------------------------
-- EXECUTE TRANSFORMATION PROCESSING
--------------------------------------------------------------
BEGIN
    tr_pkg.clear_all_tables;
    tr_pkg.process_all_transformations;
END;
/

--------------------------------------------------------------
-- POPULATE DIMENSION TABLES 
--------------------------------------------------------------


DECLARE
    v_count NUMBER;
    v_max_key NUMBER;
BEGIN
    DBMS_OUTPUT.ENABLE(1000000);
    DBMS_OUTPUT.PUT_LINE('=== STARTING STAR SCHEMA LOAD ===');
    
    -- 1. CLEAR ALL TABLES
    DBMS_OUTPUT.PUT_LINE('1. Clearing all tables...');
    DELETE FROM FACT_CRIME;
    DELETE FROM DIM_TIME;
    DELETE FROM DIM_STATION;
    DELETE FROM DIM_STATUS;
    DELETE FROM DIM_CRIMETYPE;
    COMMIT;
    
    -- 2. LOAD DIM_TIME
    DBMS_OUTPUT.PUT_LINE('2. Loading DIM_TIME...');
    INSERT INTO DIM_TIME (time_key, full_date, year, month, day, quarter)
    SELECT TO_NUMBER(TO_CHAR(date_reported, 'YYYYMMDD')),
           TRUNC(date_reported),
           EXTRACT(YEAR FROM date_reported),
           EXTRACT(MONTH FROM date_reported),
           EXTRACT(DAY FROM date_reported),
           TO_CHAR(date_reported, 'Q')
    FROM pl_reported_crime
    WHERE date_reported IS NOT NULL
    UNION
    SELECT TO_NUMBER(TO_CHAR(reported_date, 'YYYYMMDD')),
           TRUNC(reported_date),
           EXTRACT(YEAR FROM reported_date),
           EXTRACT(MONTH FROM reported_date),
           EXTRACT(DAY FROM reported_date),
           TO_CHAR(reported_date, 'Q')
    FROM CRIME_REGISTER
    WHERE reported_date IS NOT NULL;
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   Loaded: ' || v_count || ' records');
    COMMIT;
    
    -- 3. LOAD DIM_STATION 
    DBMS_OUTPUT.PUT_LINE('3. Loading DIM_STATION...');
    
    -- First get the maximum key to start from
    SELECT NVL(MAX(station_key), 0) INTO v_max_key FROM DIM_STATION;
    
    -- Load from GOOD data with new sequential keys
    INSERT INTO DIM_STATION (station_key, station_id, station_name, region)
    SELECT ROWNUM + v_max_key,
           station_id,
           INITCAP(TRIM(station_name)),
           INITCAP(TRIM(region))
    FROM (
        SELECT DISTINCT station_id, station_name, region
        FROM gd_station
        WHERE station_name IS NOT NULL AND region IS NOT NULL
    );
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   From gd_station: ' || v_count || ' records');
    COMMIT;
    
    -- Get new max key
    SELECT NVL(MAX(station_key), 0) INTO v_max_key FROM DIM_STATION;
    
    -- Load from TRANSFORMED data with new sequential keys
    INSERT INTO DIM_STATION (station_key, station_id, station_name, region)
    SELECT ROWNUM + v_max_key,
           station_id,
           station_name_standard,
           region_standard
    FROM (
        SELECT DISTINCT station_id, station_name_standard, region_standard
        FROM tr_station ts
        WHERE station_name_standard IS NOT NULL 
          AND region_standard IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM DIM_STATION ds 
            WHERE UPPER(ds.station_name) = UPPER(ts.station_name_standard)
        )
    );
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   From tr_station: ' || v_count || ' records');
    COMMIT;
    
    -- 4. LOAD DIM_STATUS
    DBMS_OUTPUT.PUT_LINE('4. Loading DIM_STATUS...');
    
    -- Start from 1
    v_max_key := 0;
    
    -- Load from GOOD data
    INSERT INTO DIM_STATUS (status_key, status_name)
    SELECT ROWNUM + v_max_key,
           UPPER(TRIM(status_name))
    FROM (
        SELECT DISTINCT status_name
        FROM gd_status
        WHERE status_name IS NOT NULL
    );
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   From gd_status: ' || v_count || ' records');
    COMMIT;
    
    -- Get new max key
    SELECT NVL(MAX(status_key), 0) INTO v_max_key FROM DIM_STATUS;
    
    -- Load from TRANSFORMED data
    INSERT INTO DIM_STATUS (status_key, status_name)
    SELECT ROWNUM + v_max_key,
           status_name_standard
    FROM (
        SELECT DISTINCT status_name_standard
        FROM tr_status ts
        WHERE status_name_standard IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM DIM_STATUS ds 
            WHERE ds.status_name = ts.status_name_standard
        )
    );
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   From tr_status: ' || v_count || ' records');
    COMMIT;
    
    -- 5. LOAD DIM_CRIMETYPE
    DBMS_OUTPUT.PUT_LINE('5. Loading DIM_CRIMETYPE...');
    
    -- Start from 1
    v_max_key := 0;
    
    -- Load from GOOD data
    INSERT INTO DIM_CRIMETYPE (crime_type_key, crime_type_name)
    SELECT ROWNUM + v_max_key,
           INITCAP(TRIM(crime_type_name))
    FROM (
        SELECT DISTINCT crime_type_name
        FROM gd_crimetype
        WHERE crime_type_name IS NOT NULL
    );
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   From gd_crimetype: ' || v_count || ' records');
    COMMIT;
    
    -- Get new max key
    SELECT NVL(MAX(crime_type_key), 0) INTO v_max_key FROM DIM_CRIMETYPE;
    
    -- Load from TRANSFORMED data
    INSERT INTO DIM_CRIMETYPE (crime_type_key, crime_type_name)
    SELECT ROWNUM + v_max_key,
           crime_type_name_standard
    FROM (
        SELECT DISTINCT crime_type_name_standard
        FROM tr_crimetype tc
        WHERE crime_type_name_standard IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM DIM_CRIMETYPE dc 
            WHERE UPPER(dc.crime_type_name) = UPPER(tc.crime_type_name_standard)
        )
    );
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   From tr_crimetype: ' || v_count || ' records');
    COMMIT;
    
    -- 6. LOAD FACT TABLE - PRCS CRIMES
   --LOAD FACT TABLE - PRCS CRIMES (with offset for crime_id)
    DBMS_OUTPUT.PUT_LINE('6. Loading FACT_CRIME from PRCS...');
    
    -- Generate unique crime_id for PRCS (add 100000 offset)
    INSERT INTO FACT_CRIME (crime_id, station_key, time_key, status_key, crime_type_key, number_of_cases)
    SELECT rc.reported_crime_id + 100000,  -- Add offset to make IDs unique
           ds.station_key,
           dt.time_key,
           st.status_key,
           ct.crime_type_key,
           1
    FROM pl_reported_crime rc
    JOIN pl_crime_type pct ON rc.fk1_crime_type_id = pct.crime_type_id
    JOIN DIM_STATION ds ON rc.fk2_station_id = ds.station_id
    JOIN DIM_TIME dt ON TRUNC(rc.date_reported) = dt.full_date
    JOIN DIM_STATUS st ON UPPER(rc.crime_status) = st.status_name
    JOIN DIM_CRIMETYPE ct ON UPPER(pct.crime_type_desc) = UPPER(ct.crime_type_name);
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   PRCS crimes: ' || v_count || ' records');
    COMMIT;
    
    -- 7. LOAD FACT TABLE - PS WALES CRIMES (using original crime_id)
    DBMS_OUTPUT.PUT_LINE('7. Loading FACT_CRIME from PS Wales...');
    
    -- Using original crime_id for PS Wales 
    INSERT INTO FACT_CRIME (crime_id, station_key, time_key, status_key, crime_type_key, number_of_cases)
    SELECT cr.crime_id,
           ds.station_key,
           dt.time_key,
           st.status_key,
           ct.crime_type_key,
           1
    FROM CRIME_REGISTER cr
    LEFT JOIN OFFENCE o ON cr.crime_id = o.crime_id
    JOIN DIM_STATION ds ON cr.location_id = ds.station_id
    JOIN DIM_TIME dt ON TRUNC(cr.reported_date) = dt.full_date
    JOIN DIM_STATUS st ON UPPER(cr.crime_status) = st.status_name
    JOIN DIM_CRIMETYPE ct ON UPPER(NVL(cr.crime_name, o.offence_type)) = UPPER(ct.crime_type_name);
    v_count := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('   PS Wales crimes: ' || v_count || ' records');
    COMMIT;
    
    END;
    /