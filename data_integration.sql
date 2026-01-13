--------------------------------------------------------------
-- STAGING CODE - RAW EXTRACTION 
--------------------------------------------------------------

-- 1. DROP EXISTING OBJECTS
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE stg_station CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE stg_status CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP TABLE stg_crimetype CASCADE CONSTRAINTS';
    EXECUTE IMMEDIATE 'DROP PACKAGE staging_raw_pkg';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- 2. CREATE STAGING TABLES
CREATE TABLE stg_station(
    station_new_key INTEGER,
    station_id INTEGER NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    source_system VARCHAR(20) NOT NULL,
    record_status VARCHAR(10) DEFAULT 'RAW',
    error_message VARCHAR(500),
    CONSTRAINT pk_stg_station PRIMARY KEY(station_new_key) 
);

CREATE TABLE stg_status(
    status_new_key INTEGER,
    status_id INTEGER NOT NULL,
    status_name VARCHAR(50) NOT NULL,
    status_description VARCHAR(100),
    source_system VARCHAR(20) NOT NULL,
    record_status VARCHAR(10) DEFAULT 'RAW',
    error_message VARCHAR(500),
    CONSTRAINT pk_stg_status PRIMARY KEY(status_new_key)
);

CREATE TABLE stg_crimetype(
    crimetype_new_key INTEGER,
    crime_type_id INTEGER NOT NULL,
    crime_type_name VARCHAR(100) NOT NULL,
    crime_category VARCHAR(50),
    source_system VARCHAR(20) NOT NULL,
    record_status VARCHAR(10) DEFAULT 'RAW',
    error_message VARCHAR(500),
    CONSTRAINT pk_stg_crimetype PRIMARY KEY(crimetype_new_key) 
);

-- 3. CREATE SEQUENCES
BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE stg_station_seq';
EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE stg_status_seq';
EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE stg_crimetype_seq';
EXCEPTION WHEN OTHERS THEN NULL; END;
/

CREATE SEQUENCE stg_station_seq MINVALUE 1 MAXVALUE 99999999 START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE stg_status_seq MINVALUE 1 MAXVALUE 99999999 START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE stg_crimetype_seq MINVALUE 1 MAXVALUE 99999999 START WITH 1 INCREMENT BY 1 NOCACHE;

-- 4. CREATE TRIGGERS
CREATE OR REPLACE TRIGGER stg_station_trigger BEFORE INSERT ON stg_station FOR EACH ROW
BEGIN
    IF :new.station_new_key IS NULL THEN
        SELECT stg_station_seq.NEXTVAL INTO :new.station_new_key FROM DUAL;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER stg_status_trigger BEFORE INSERT ON stg_status FOR EACH ROW
BEGIN
    IF :new.status_new_key IS NULL THEN
        SELECT stg_status_seq.NEXTVAL INTO :new.status_new_key FROM DUAL;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER stg_crimetype_trigger BEFORE INSERT ON stg_crimetype FOR EACH ROW
BEGIN
    IF :new.crimetype_new_key IS NULL THEN
        SELECT stg_crimetype_seq.NEXTVAL INTO :new.crimetype_new_key FROM DUAL;
    END IF;
END;
/

--------------------------------------------------------------
-- CORRECTED STAGING PACKAGE --
--------------------------------------------------------------

CREATE OR REPLACE PACKAGE staging_raw_pkg AS
    PROCEDURE load_all_raw_data;
    PROCEDURE clear_staging_tables;
    PROCEDURE show_raw_data_summary;
END staging_raw_pkg;
/

CREATE OR REPLACE PACKAGE BODY staging_raw_pkg AS

    PROCEDURE load_all_raw_data IS
        v_max_status_id NUMBER;
        v_max_crimetype_id NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Loading RAW data from original sources...');
        
        -- 1. CLEAR EXISTING DATA
        clear_staging_tables;
        
        -- 2. LOAD RAW STATIONS FROM PRCS (ALL columns)
        INSERT INTO stg_station (station_id, station_name, region, source_system, record_status, error_message)
        SELECT s.station_id, 
               s.station_name,  
               a.area_name, 
               'PRCS',
               'RAW',
               NULL  
        FROM pl_station s
        LEFT JOIN pl_area a ON s.fk1_area_id = a.area_id;
        
        DBMS_OUTPUT.PUT_LINE('Loaded ' || SQL%ROWCOUNT || ' RAW stations from PRCS');
        
        -- 3. LOAD RAW STATIONS FROM PS WALES (ALL columns)
        INSERT INTO stg_station (station_id, station_name, region, source_system, record_status, error_message)
        SELECT l.location_id,
               l.city_name,    
               r.region_name, 
               'PS_WALES',
               'RAW',
               NULL
        FROM LOCATION l
        JOIN REGION r ON l.region_id = r.region_id
        WHERE l.city_name IS NOT NULL;
        
        DBMS_OUTPUT.PUT_LINE('Loaded ' || SQL%ROWCOUNT || ' RAW stations from PS Wales');
        
        -- 4. LOAD RAW STATUSES FROM PRCS 
        INSERT INTO stg_status (status_id, status_name, status_description, source_system, record_status, error_message)
        SELECT ROWNUM, 
               crime_status,  
               CASE crime_status
                   WHEN 'OPEN' THEN 'Case is under investigation'
                   WHEN 'CLOSED' THEN 'Case has been resolved'
                   WHEN 'ESCALATE' THEN 'Case requires higher attention'
                   ELSE 'Unknown status: ' || crime_status
               END as status_description,
               'PRCS',
               'RAW',
               NULL
        FROM (
            SELECT DISTINCT crime_status
            FROM pl_reported_crime
            WHERE crime_status IS NOT NULL
        );
        
        DBMS_OUTPUT.PUT_LINE('Loaded ' || SQL%ROWCOUNT || ' RAW statuses from PRCS');
        
        -- 5. LOAD RAW STATUSES FROM PS WALES 
        SELECT NVL(MAX(status_id), 0) INTO v_max_status_id FROM stg_status;
        
        INSERT INTO stg_status (status_id, status_name, status_description, source_system, record_status, error_message)
        SELECT v_max_status_id + ROWNUM,
               crime_status,  
               CASE UPPER(crime_status)
                   WHEN 'OPEN' THEN 'Case is under investigation'
                   WHEN 'CLOSED' THEN 'Case has been resolved'
                   ELSE 'Unknown status: ' || crime_status
               END as status_description,
               'PS_WALES',
               'RAW',
               NULL
        FROM (
            SELECT DISTINCT crime_status
            FROM CRIME_REGISTER
            WHERE crime_status IS NOT NULL
        );
        
        DBMS_OUTPUT.PUT_LINE('Loaded ' || SQL%ROWCOUNT || ' RAW statuses from PS Wales');
        
        -- 6. LOAD RAW CRIME TYPES FROM PRCS 
        INSERT INTO stg_crimetype (crime_type_id, crime_type_name, crime_category, source_system, record_status, error_message)
        SELECT crime_type_id,
               crime_type_desc,  
               CASE 
                   WHEN UPPER(crime_type_desc) LIKE '%DRUG%' THEN 'Substance Offence'
                   WHEN UPPER(crime_type_desc) LIKE '%VIOLENT%' THEN 'Violent Crime'
                   WHEN UPPER(crime_type_desc) LIKE '%BURGLARY%' THEN 'Property Crime'
                   WHEN UPPER(crime_type_desc) LIKE '%THEFT%' THEN 'Property Crime'
                   WHEN UPPER(crime_type_desc) LIKE '%ROBBERY%' THEN 'Financial Crime'
                   WHEN UPPER(crime_type_desc) LIKE '%FRAUD%' THEN 'White Collar Crime'
                   WHEN UPPER(crime_type_desc) LIKE '%FORGERY%' THEN 'White Collar Crime'
                   WHEN UPPER(crime_type_desc) LIKE '%DRUNK%' THEN 'Public Order Offence'
                   ELSE 'Other Crime'
               END as crime_category,
               'PRCS',
               'RAW',
               NULL
        FROM pl_crime_type;
        
        DBMS_OUTPUT.PUT_LINE('Loaded ' || SQL%ROWCOUNT || ' RAW crime types from PRCS');
        
        -- 7. LOAD RAW CRIME TYPES FROM PS WALES
        SELECT NVL(MAX(crime_type_id), 0) INTO v_max_crimetype_id FROM stg_crimetype;
        
        INSERT INTO stg_crimetype (crime_type_id, crime_type_name, crime_category, source_system, record_status, error_message)
        SELECT v_max_crimetype_id + ROWNUM,
               o.offence_type,  
               CASE 
                   WHEN UPPER(o.offence_type) LIKE '%ROBBERY%' THEN 'Financial Crime'
                   WHEN UPPER(o.offence_type) LIKE '%BLACKMAIL%' THEN 'Financial Crime'
                   WHEN UPPER(o.offence_type) LIKE '%FORGERY%' THEN 'White Collar Crime'
                   WHEN UPPER(o.offence_type) LIKE '%DRUNK%' THEN 'Public Order Offence'
                   WHEN UPPER(o.offence_type) LIKE '%KIDNAP%' THEN 'Violent Crime'
                   ELSE 'Other Crime'
               END as crime_category,
               'PS_WALES',
               'RAW',
               NULL
        FROM OFFENCE o
        WHERE o.offence_type IS NOT NULL;
        
        -- 8. LOAD RAW CRIME TYPES FROM PS WALES
        SELECT NVL(MAX(crime_type_id), 0) INTO v_max_crimetype_id FROM stg_crimetype;
        
        INSERT INTO stg_crimetype (crime_type_id, crime_type_name, crime_category, source_system, record_status, error_message)
        SELECT v_max_crimetype_id + ROWNUM,
               c.crime_name,  
               CASE 
                   WHEN UPPER(c.crime_name) LIKE '%THEFT%' THEN 'Property Crime'
                   WHEN UPPER(c.crime_name) LIKE '%ROBBERY%' THEN 'Financial Crime'
                   WHEN UPPER(c.crime_name) LIKE '%MURDER%' THEN 'Violent Crime'
                   WHEN UPPER(c.crime_name) LIKE '%HIT AND RUN%' THEN 'Traffic Offence'
                   WHEN UPPER(c.crime_name) LIKE '%BLACK MAIL%' THEN 'Financial Crime'
                   ELSE 'Other Crime'
               END as crime_category,
               'PS_WALES',
               'RAW',
               NULL
        FROM CRIME_REGISTER c
        WHERE c.crime_name IS NOT NULL;
        
        DBMS_OUTPUT.PUT_LINE('Loaded ' || SQL%ROWCOUNT || ' RAW crime types from PS Wales');
        
        DBMS_OUTPUT.PUT_LINE('=== RAW DATA LOAD COMPLETE ===');
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
            RAISE;
    END load_all_raw_data;
    
    PROCEDURE clear_staging_tables IS
    BEGIN
        DELETE FROM stg_station;
        DELETE FROM stg_status;
        DELETE FROM stg_crimetype;
        DBMS_OUTPUT.PUT_LINE('Staging tables cleared');
    END clear_staging_tables;
    
    PROCEDURE show_raw_data_summary IS
        v_station_count NUMBER;
        v_status_count NUMBER;
        v_crimetype_count NUMBER;
    BEGIN
        -- Get counts
        SELECT COUNT(*) INTO v_station_count FROM stg_station;
        SELECT COUNT(*) INTO v_status_count FROM stg_status;
        SELECT COUNT(*) INTO v_crimetype_count FROM stg_crimetype;
        
        DBMS_OUTPUT.PUT_LINE('=== RAW DATA SUMMARY ===');
        DBMS_OUTPUT.PUT_LINE('Total Stations: ' || v_station_count);
        DBMS_OUTPUT.PUT_LINE('Total Statuses: ' || v_status_count);
        DBMS_OUTPUT.PUT_LINE('Total Crime Types: ' || v_crimetype_count);
        
        DBMS_OUTPUT.PUT_LINE(CHR(10) || '=== ALL DATA QUALITY ISSUES PRESENT ===');
        
        -- 1. STATION ISSUES
        DBMS_OUTPUT.PUT_LINE('1. STATION NAME ISSUES:');
        
        -- All uppercase
        DBMS_OUTPUT.PUT_LINE('   All UPPERCASE stations:');
        FOR r IN (
            SELECT station_name, source_system 
            FROM stg_station 
            WHERE station_name = UPPER(station_name)
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('     - ' || r.station_name || ' (' || r.source_system || ')');
        END LOOP;
        
        -- All lowercase
        DBMS_OUTPUT.PUT_LINE('   All lowercase stations:');
        FOR r IN (
            SELECT station_name, source_system 
            FROM stg_station 
            WHERE station_name = LOWER(station_name)
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('     - ' || r.station_name || ' (' || r.source_system || ')');
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('   Station codes/abbreviations:');
        FOR r IN (
            SELECT station_name, source_system 
            FROM stg_station 
            WHERE REGEXP_LIKE(station_name, '^[A-Z_]+$')
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('     - ' || r.station_name || ' (' || r.source_system || ')');
        END LOOP;
        
        -- Duplicate names
        DBMS_OUTPUT.PUT_LINE('   Duplicate station names:');
        FOR r IN (
            SELECT station_name, COUNT(*) as count
            FROM stg_station
            GROUP BY station_name
            HAVING COUNT(*) > 1
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('     - ' || r.station_name || ' (appears ' || r.count || ' times)');
        END LOOP;
        
        -- 2. STATUS ISSUES
        DBMS_OUTPUT.PUT_LINE(CHR(10) || '2. STATUS ISSUES:');
        DBMS_OUTPUT.PUT_LINE('   Statuses not in UPPERCASE:');
        FOR r IN (
            SELECT status_name, source_system 
            FROM stg_status 
            WHERE status_name != UPPER(status_name)
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('     - ' || r.status_name || ' (' || r.source_system || ')');
        END LOOP;
        
        -- 3. CRIME TYPE ISSUES
        DBMS_OUTPUT.PUT_LINE(CHR(10) || '3. CRIME TYPE ISSUES:');
        
        DBMS_OUTPUT.PUT_LINE('   Crime types not in consistent case:');
        FOR r IN (
            SELECT crime_type_name, source_system 
            FROM stg_crimetype 
            WHERE crime_type_name NOT IN (UPPER(crime_type_name), LOWER(crime_type_name), INITCAP(crime_type_name))
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('     - ' || r.crime_type_name || ' (' || r.source_system || ')');
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('   Crime types with extra spaces:');
        FOR r IN (
            SELECT crime_type_name, source_system 
            FROM stg_crimetype 
            WHERE crime_type_name LIKE '%  %' OR crime_type_name LIKE '% ' OR crime_type_name LIKE ' %'
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('     - "' || r.crime_type_name || '" (' || r.source_system || ')');
        END LOOP;
        
    END show_raw_data_summary;

END staging_raw_pkg;
/

--------------------------------------------------------------
-- LOAD AND VERIFY DATA
--------------------------------------------------------------
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== LOADING COMPLETE RAW DATA ===');
    
    -- Load data
    staging_raw_pkg.clear_staging_tables;
    staging_raw_pkg.load_all_raw_data;
   
    
END;
/