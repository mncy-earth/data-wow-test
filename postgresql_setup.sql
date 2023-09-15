-- DROP SCHEMA IF EXISTS public;
-- CREATE SCHEMA public;

-- ALTER USER postgres_user SET search_path = public;

----------------------------------------------------------------------------------------------------

-- DROP ROLE IF EXISTS dwh_user;
CREATE ROLE dwh_user SUPERUSER LOGIN PASSWORD 'dwh_pass';

-- DROP SCHEMA IF EXISTS dwh;
CREATE SCHEMA dwh;
GRANT ALL ON ALL TABLES IN SCHEMA dwh TO dwh_user;
ALTER USER dwh_user SET search_path = dwh;

CREATE TABLE dwh.data_sample (
    department_name VARCHAR(32),
    sensor_serial VARCHAR(64),
    create_at TIMESTAMP,
    product_name VARCHAR(16),
    product_expire TIMESTAMP
);

----------------------------------------------------------------------------------------------------

-- DROP DATABASE IF EXISTS airflow_db;
CREATE DATABASE airflow_db;

-- DROP ROLE IF EXISTS airflow_user;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;