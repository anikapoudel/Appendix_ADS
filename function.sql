--------------------------------------------------------------
--  FUNCTIONS SCRIPT
--------------------------------------------------------------

--------------------------------------------------------------
-- 1. VALIDATION: Check station name format
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_validate_station_name(
    p_station_name IN VARCHAR2
) RETURN VARCHAR2
IS
BEGIN
    IF p_station_name IS NULL THEN
        RETURN 'NULL';
    END IF;
    
    IF REGEXP_LIKE(p_station_name, '^[A-Z][a-z]+( [A-Z][a-z]+)*$') THEN
        RETURN 'VALID';
    END IF;
    
    IF p_station_name = UPPER(p_station_name) THEN
        RETURN 'ALL_UPPERCASE';
    END IF;
    
    IF p_station_name = LOWER(p_station_name) THEN
        RETURN 'ALL_LOWERCASE';
    END IF;
    
    RETURN 'INVALID';
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN 'ERROR';
END fn_validate_station_name;
/

--------------------------------------------------------------
-- 2. VALIDATION: Check status value
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_validate_status(
    p_status IN VARCHAR2
) RETURN VARCHAR2
IS
BEGIN
    IF p_status IS NULL THEN
        RETURN 'NULL';
    END IF;
    
    IF UPPER(p_status) IN ('OPEN', 'CLOSED', 'ESCALATE') THEN
        RETURN 'VALID';
    ELSE
        RETURN 'INVALID';
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN 'ERROR';
END fn_validate_status;
/

--------------------------------------------------------------
-- 3. TRANSFORMATION: Clean and format names
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_clean_name(
    p_name IN VARCHAR2
) RETURN VARCHAR2
IS
    v_cleaned VARCHAR2(200);
BEGIN
    IF p_name IS NULL THEN
        RETURN NULL;
    END IF;
    
    v_cleaned := TRIM(p_name);
    
    WHILE INSTR(v_cleaned, '  ') > 0 LOOP
        v_cleaned := REPLACE(v_cleaned, '  ', ' ');
    END LOOP;
    
    v_cleaned := INITCAP(v_cleaned);
    
    RETURN v_cleaned;
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN p_name;
END fn_clean_name;
/

--------------------------------------------------------------
-- 4. BUSINESS LOGIC: Calculate data quality score 
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_calc_quality_score(
    p_station_name IN VARCHAR2,
    p_region IN VARCHAR2,
    p_source IN VARCHAR2
) RETURN NUMBER
IS
    v_score NUMBER := 100;
BEGIN
    IF p_station_name IS NULL THEN 
        v_score := v_score - 40;
    END IF;
    
    IF p_region IS NULL THEN 
        v_score := v_score - 30;
    END IF;
    
    IF p_source IS NULL THEN 
        v_score := v_score - 10;
    END IF;
    
    IF p_station_name IS NOT NULL THEN
        IF fn_validate_station_name(p_station_name) != 'VALID' THEN
            v_score := v_score - 15;
        END IF;
    END IF;
    
    IF p_source NOT IN ('PRCS', 'PS_WALES') THEN
        v_score := v_score - 5;
    END IF;
    
    RETURN GREATEST(LEAST(v_score, 100), 0);
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN 0;
END fn_calc_quality_score;
/

--------------------------------------------------------------
-- 5. UTILITY: Get record count from any table
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_get_record_count(
    p_table_name IN VARCHAR2
) RETURN NUMBER
IS
    v_count NUMBER;
    v_sql VARCHAR2(500);
BEGIN
    v_sql := 'SELECT COUNT(*) FROM ' || p_table_name;
    
    EXECUTE IMMEDIATE v_sql INTO v_count;
    
    RETURN v_count;
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN -1;
END fn_get_record_count;
/

