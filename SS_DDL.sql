-----------------------------------------------------
-- DROP TABLES 
-----------------------------------------------------
DROP TABLE FACT_CRIME CASCADE CONSTRAINTS;
DROP TABLE DIM_STATUS CASCADE CONSTRAINTS;
DROP TABLE DIM_STATION CASCADE CONSTRAINTS;
DROP TABLE DIM_TIME CASCADE CONSTRAINTS;

-----------------------------------------------------
-- DIM_TIME
-----------------------------------------------------
CREATE TABLE DIM_TIME (
    time_key     INTEGER       NOT NULL,
    full_date    DATE,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    quarter      INTEGER,

    CONSTRAINT pk_DIM_TIME PRIMARY KEY (time_key)
);

-----------------------------------------------------
-- DIM_STATION
-----------------------------------------------------
CREATE TABLE DIM_STATION (
    station_key    INTEGER       NOT NULL,
    station_id     INTEGER,
    station_name   VARCHAR2(100),
    region         VARCHAR2(100),

    CONSTRAINT pk_DIM_STATION PRIMARY KEY (station_key)
);

-----------------------------------------------------
-- DIM_STATUS
-----------------------------------------------------
CREATE TABLE DIM_STATUS (
    status_key     INTEGER      NOT NULL,
    status_name    VARCHAR2(50),

    CONSTRAINT pk_DIM_STATUS PRIMARY KEY (status_key)
);

-----------------------------------------------------
-- FACT_CRIME
-----------------------------------------------------
CREATE TABLE FACT_CRIME (
    crime_id         INTEGER       NOT NULL,
    station_key      INTEGER       NOT NULL,
    time_key         INTEGER       NOT NULL,
    status_key       INTEGER       NOT NULL,
    number_of_cases  INTEGER,

    CONSTRAINT pk_FACT_CRIME PRIMARY KEY (crime_id),

    -- Foreign Keys
    CONSTRAINT fk_fact_station FOREIGN KEY (station_key)
        REFERENCES DIM_STATION (station_key),

    CONSTRAINT fk_fact_time FOREIGN KEY (time_key)
        REFERENCES DIM_TIME (time_key),

    CONSTRAINT fk_fact_status FOREIGN KEY (status_key)
        REFERENCES DIM_STATUS (status_key)
);