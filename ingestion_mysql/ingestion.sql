CREATE DATABASE IF NOT EXISTS hdb_prices_dev;

USE hdb_prices_dev;
SHOW VARIABLES LIKE 'datadir'; -- to show where the files are referenced to

CREATE TABLE IF NOT EXISTS raw_sales (
    `month` VARCHAR(50),
    town VARCHAR(100),
    flat_type VARCHAR(100),
    `block` VARCHAR(100),
    street_name VARCHAR(255),
    storey_range VARCHAR(100),
    floor_area_sqm NUMERIC,
    flat_model VARCHAR(100),
    lease_commence_date YEAR,
    remaining_lease VARCHAR(100),
    resale_price NUMERIC
);

LOAD DATA INFILE 'Jan2017-Dec2023_sales.csv' 
INTO TABLE raw_sales 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(`month`, town, flat_type, block, street_name, storey_range, floor_area_sqm, flat_model, lease_commence_date, remaining_lease, resale_price);

CREATE TABLE IF NOT EXISTS raw_rents (
    rent_approval_date VARCHAR(50),
    town VARCHAR(100),
    `block` VARCHAR(100),
    street_name VARCHAR(255),
    flat_type VARCHAR(100),
    monthly_rent NUMERIC
);

LOAD DATA INFILE 'Jan2021-Mar2024_rent.csv' 
INTO TABLE raw_rents 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(rent_approval_date, town, `block`, street_name, flat_type, monthly_rent);