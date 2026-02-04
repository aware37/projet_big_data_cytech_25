-- Nettoyage
DROP TABLE IF EXISTS fact_trips CASCADE;
DROP TABLE IF EXISTS dim_vendor CASCADE;
DROP TABLE IF EXISTS dim_rate_code CASCADE;
DROP TABLE IF EXISTS dim_payment_type CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;

-- Création des tables en étoile

-- Dimensions
CREATE TABLE dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(100)
);

CREATE TABLE dim_rate_code (
    rate_code_id INT PRIMARY KEY,
    rate_description VARCHAR(100)
);

CREATE TABLE dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_name VARCHAR(50)
);

CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- Table de faits
CREATE TABLE fact_trips (
    trip_id SERIAL PRIMARY KEY,
    
    -- Clés étrangères
    vendor_id INT REFERENCES dim_vendor(vendor_id),
    rate_code_id INT REFERENCES dim_rate_code(rate_code_id),
    payment_type_id INT REFERENCES dim_payment_type(payment_type_id),
    pu_location_id INT REFERENCES dim_location(location_id),
    do_location_id INT REFERENCES dim_location(location_id),
    
    -- Données
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DECIMAL(10, 2),
    store_and_fwd_flag VARCHAR(3),
    
    -- Finances
    fare_amount DECIMAL(10, 2),
    extra DECIMAL(10, 2),
    mta_tax DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2),
    tolls_amount DECIMAL(10, 2),
    improvement_surcharge DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    
    -- Taxes 2025
    congestion_surcharge DECIMAL(10, 2),
    airport_fee DECIMAL(10, 2),
    cbd_congestion_fee DECIMAL(10, 2)
);

-- Index pour la performance
CREATE INDEX index_pickup_datetime ON fact_trips(tpep_pickup_datetime);
CREATE INDEX index_vendor_id ON fact_trips(vendor_id);
CREATE INDEX index_rate_code_id ON fact_trips(rate_code_id);
CREATE INDEX index_pu_location_id ON fact_trips(pu_location_id);
CREATE INDEX index_do_location_id ON fact_trips(do_location_id);
CREATE INDEX index_payment_type_id ON fact_trips(payment_type_id);