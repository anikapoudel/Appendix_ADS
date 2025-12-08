--------------------------------------------------------------
-- Star Schema DDL Script
--------------------------------------------------------------

--------------------------------------------------------------
-- DROP TABLES 
--------------------------------------------------------------
DROP TABLE FACT_CRIME CASCADE CONSTRAINTS;
DROP TABLE DIM_TIME CASCADE CONSTRAINTS;
DROP TABLE DIM_STATION CASCADE CONSTRAINTS;
DROP TABLE DIM_STATUS CASCADE CONSTRAINTS;
DROP TABLE DIM_CRIMETYPE CASCADE CONSTRAINTS;

--------------------------------------------------------------
-- DIMENSION TABLES
--------------------------------------------------------------

-- DIM_TIME
CREATE TABLE DIM_TIME(
    time_key    INTEGER NOT NULL,
    full_date   DATE,
    year        INTEGER,
    month       INTEGER,
    day         INTEGER,
    quarter     INTEGER,
    CONSTRAINT pk_DIM_TIME PRIMARY KEY (time_key)
);

-- DIM_STATION
CREATE TABLE DIM_STATION(
    station_key INTEGER NOT NULL,
    station_id  INTEGER,
    station_name VARCHAR(20),
    region      VARCHAR(20),
    CONSTRAINT pk_DIM_STATION PRIMARY KEY (station_key)
);

-- DIM_STATUS
CREATE TABLE DIM_STATUS(
    status_key INTEGER NOT NULL,
    status_name VARCHAR(20),
    CONSTRAINT pk_DIM_STATUS PRIMARY KEY (status_key)
);

-- DIM_CRIMETYPE
CREATE TABLE DIM_CRIMETYPE(
    crime_type_key INTEGER NOT NULL,
    crime_type_name VARCHAR(20),
    CONSTRAINT pk_DIM_CRIMETYPE PRIMARY KEY (crime_type_key)
);

--------------------------------------------------------------
-- FACT TABLE
--------------------------------------------------------------

CREATE TABLE FACT_CRIME(
    crime_id        INTEGER NOT NULL,
    station_key     INTEGER NOT NULL,
    time_key        INTEGER NOT NULL,
    status_key      INTEGER NOT NULL,
    crime_type_key  INTEGER NOT NULL,
    number_of_cases INTEGER,
    CONSTRAINT pk_FACT_CRIME PRIMARY KEY (crime_id)
);

--------------------------------------------------------------
-- FOREIGN KEY CONSTRAINTS
--------------------------------------------------------------

ALTER TABLE FACT_CRIME
ADD CONSTRAINT fk_FACT_CRIME_station FOREIGN KEY(station_key)
REFERENCES DIM_STATION(station_key);

ALTER TABLE FACT_CRIME
ADD CONSTRAINT fk_FACT_CRIME_time FOREIGN KEY(time_key)
REFERENCES DIM_TIME(time_key);

ALTER TABLE FACT_CRIME
ADD CONSTRAINT fk_FACT_CRIME_status FOREIGN KEY(status_key)
REFERENCES DIM_STATUS(status_key);

ALTER TABLE FACT_CRIME
ADD CONSTRAINT fk_FACT_CRIME_crimetype FOREIGN KEY(crime_type_key)
REFERENCES DIM_CRIMETYPE(crime_type_key);


