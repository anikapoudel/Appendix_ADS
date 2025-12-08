-- data_integration.sql
-- Task 3.1: Integrate PS_WALES → PRCS system (master source)
-- Correct order: parent tables first, then child tables

--------------------------------------------------------------
--  INSERT AREAS (parent for stations)
--------------------------------------------------------------
-- Assuming pl_area has columns: area_id, area_name
INSERT INTO pl_area (area_id, area_name)
SELECT region_id, INITCAP(region_name)
FROM REGION r
WHERE NOT EXISTS (
    SELECT 1 
    FROM pl_area a 
    WHERE a.area_id = r.region_id
);
COMMIT;

--------------------------------------------------------------
--  INTEGRATE CRIME TYPES
--------------------------------------------------------------
INSERT INTO pl_crime_type (crime_type_id, crime_type_desc)
SELECT 
    500 + ROWNUM,
    INITCAP(TRIM(offence_type))
FROM (
    SELECT DISTINCT offence_type 
    FROM OFFENCE 
    WHERE offence_type IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 
        FROM pl_crime_type 
        WHERE UPPER(TRIM(crime_type_desc)) = UPPER(TRIM(OFFENCE.offence_type))
    )
);
COMMIT;

--------------------------------------------------------------
--  INTEGRATE STATIONS
--------------------------------------------------------------
INSERT INTO pl_station (station_id, station_name, fk1_area_id)
SELECT DISTINCT
    location_id + 1000 AS station_id,
    INITCAP(TRIM(city_name)) AS station_name,
    region_id AS fk1_area_id  -- now matches existing area_id in pl_area
FROM LOCATION
WHERE location_id + 1000 NOT IN (SELECT station_id FROM pl_station);
COMMIT;




--------------------------------------------------------------
-- INTEGRATE OFFICERS
--------------------------------------------------------------
INSERT INTO pl_police_employee (emp_id, emp_name, emp_grade)
SELECT 
    2000 + officer_id,
    INITCAP(TRIM(first_name || ' ' || COALESCE(middle_name || ' ', '') || last_name)),
    CASE 
        WHEN rank <= 2 THEN 1
        WHEN rank <= 4 THEN 3
        ELSE 5
    END
FROM OFFICER
WHERE NOT EXISTS (
    SELECT 1 
    FROM pl_police_employee 
    WHERE emp_name = INITCAP(TRIM(first_name || ' ' || COALESCE(middle_name || ' ', '') || last_name))
);
COMMIT;

--------------------------------------------------------------
-- INTEGRATE WITNESSES
--------------------------------------------------------------
INSERT INTO pl_witness (witness_id, witness_name, witness_address, witness_type_id)
SELECT 
    500 + reporter_id,
    INITCAP(TRIM(reporter_name)),
    TRIM(address),
    800  -- External observation witness
FROM crime_reporter
WHERE reporter_id IS NOT NULL
AND NOT EXISTS (
    SELECT 1 
    FROM pl_witness 
    WHERE witness_name = INITCAP(TRIM(crime_reporter.reporter_name))
    AND witness_address = TRIM(crime_reporter.address)
);
COMMIT;

--------------------------------------------------------------
--  INTEGRATE CRIMES
--------------------------------------------------------------
INSERT INTO pl_reported_crime (
    reported_crime_id,
    date_reported,
    crime_status,
    date_closed,
    fk1_crime_type_id,
    fk2_station_id
)
SELECT 
    1000 + c.crime_id,
    c.reported_date,
    CASE UPPER(TRIM(c.crime_status))
        WHEN 'CLOSED' THEN 'CLOSED'
        ELSE 'OPEN'
    END,
    CASE 
        WHEN UPPER(TRIM(c.crime_status)) = 'CLOSED' AND c.closed_date IS NOT NULL 
        THEN c.closed_date
        ELSE NULL
    END,
    COALESCE(
        (SELECT crime_type_id 
         FROM pl_crime_type 
         WHERE UPPER(TRIM(crime_type_desc)) = UPPER(TRIM(o.offence_type))
         FETCH FIRST 1 ROWS ONLY),
        110  -- Default to Violent Crime
    ),
    COALESCE(
        (SELECT station_id 
         FROM pl_station 
         WHERE station_name = INITCAP(TRIM(l.city_name))
         FETCH FIRST 1 ROWS ONLY),
        22  -- Default station
    )
FROM CRIME_REGISTER c
JOIN LOCATION l ON c.location_id = l.location_id
LEFT JOIN OFFENCE o ON c.crime_id = o.crime_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM pl_reported_crime prc
    WHERE prc.date_reported = c.reported_date
);
COMMIT;

--------------------------------------------------------------
-- CREATE WITNESS STATEMENTS
--------------------------------------------------------------
INSERT INTO pl_statement (s_reported_crime_id, d_witness_id, statement_type, statement_location)
SELECT 
    1000 + cr.crime_id,
    500 + cr.reporter_id,
    'Initial Report',
    'Wales'
FROM CRIME_REGISTER cr
WHERE cr.reporter_id IS NOT NULL
AND EXISTS (
    SELECT 1 
    FROM pl_reported_crime prc 
    WHERE prc.reported_crime_id = 1000 + cr.crime_id
)
AND EXISTS (
    SELECT 1 
    FROM pl_witness w 
    WHERE w.witness_id = 500 + cr.reporter_id
)
AND NOT EXISTS (
    SELECT 1 
    FROM pl_statement s 
    WHERE s.s_reported_crime_id = 1000 + cr.crime_id
    AND s.d_witness_id = 500 + cr.reporter_id
);
COMMIT;

--------------------------------------------------------------
--  CREATE WORK ALLOCATIONS
--------------------------------------------------------------
INSERT INTO pl_work_allocation (s_reported_crime_id, d_emp_id, work_desc, lead_police_officer, work_start_date, work_end_date)
SELECT 
    1000 + cr.crime_id,
    2000 + cr.police_id,
    'Wales Crime Investigation',
    2000 + cr.police_id,
    cr.reported_date,
    COALESCE(cr.closed_date, cr.reported_date + 30)
FROM CRIME_REGISTER cr
WHERE cr.police_id IS NOT NULL
AND EXISTS (
    SELECT 1 
    FROM pl_reported_crime prc 
    WHERE prc.reported_crime_id = 1000 + cr.crime_id
)
AND EXISTS (
    SELECT 1 
    FROM pl_police_employee pe 
    WHERE pe.emp_id = 2000 + cr.police_id
)
AND NOT EXISTS (
    SELECT 1 
    FROM pl_work_allocation wa 
    WHERE wa.s_reported_crime_id = 1000 + cr.crime_id
    AND wa.d_emp_id = 2000 + cr.police_id
);
COMMIT;

