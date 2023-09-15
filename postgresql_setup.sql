CREATE ROLE dwh_user SUPERUSER LOGIN PASSWORD 'dwh_pass';

-- DROP SCHEMA IF EXISTS dwh;
CREATE SCHEMA dwh;
GRANT ALL ON ALL TABLES IN SCHEMA dwh TO dwh_user;
ALTER USER dwh_user SET search_path = dwh;

-- DROP TABLE dwh.data_sample;
-- DROP TABLE dwh.master_department; DROP TABLE dwh.master_sensor; DROP TABLE dwh.master_product;
CREATE TABLE IF NOT EXISTS dwh.data_sample (
    department_id BIGINT
    ,sensor_id BIGINT
    ,create_at TIMESTAMP
    ,product_id BIGINT
    ,product_expire TIMESTAMP
--    ,extract_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS dwh.master_department (department_id BIGINT, department_name VARCHAR(32));
CREATE TABLE IF NOT EXISTS dwh.master_sensor (sensor_id BIGINT, sensor_serial VARCHAR(64));
CREATE TABLE IF NOT EXISTS dwh.master_product (product_id BIGINT, product_name VARCHAR(16));

----------------------------------------------------------------------------------------------------

INSERT INTO dwh.data_sample VALUES (1, 1, '1900-01-01 00:00:00', 1, '1900-01-01 00:00:00');
INSERT INTO dwh.data_sample VALUES (1, 1, '1900-01-01 00:00:00', 2, '1900-01-01 00:00:00');
INSERT INTO dwh.data_sample VALUES (1, 2, '1900-01-01 00:00:00', 1, '1900-01-01 00:00:00');
INSERT INTO dwh.data_sample VALUES (1, 2, '1900-01-01 00:00:00', 2, '1900-01-01 00:00:00');
INSERT INTO dwh.data_sample VALUES (2, 1, '1900-01-01 00:00:00', 1, '1900-01-01 00:00:00');
INSERT INTO dwh.data_sample VALUES (2, 1, '1900-01-01 00:00:00', 2, '1900-01-01 00:00:00');
INSERT INTO dwh.data_sample VALUES (2, 2, '1900-01-01 00:00:00', 1, '1900-01-01 00:00:00');
INSERT INTO dwh.data_sample VALUES (2, 2, '1900-01-01 00:00:00', 2, '1900-01-01 00:00:00');
SELECT * FROM dwh.data_sample;

INSERT INTO dwh.master_department (department_id, department_name) VALUES (1, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
SELECT * FROM dwh.master_department;

INSERT INTO dwh.master_sensor (sensor_id, sensor_serial) VALUES (1, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
SELECT * FROM dwh.master_sensor;

INSERT INTO dwh.master_product (product_id, product_name) VALUES (1, 'xxxxxxxxxxxxxxxx');
SELECT * FROM dwh.master_product;

----------------------------------------------------------------------------------------------------

-- DROP VIEW dwh.v_data_sample;
CREATE VIEW dwh.v_data_sample AS (
    SELECT d.department_name, s.sensor_serial, main.create_at, p.product_name, main.product_expire
    FROM dwh.data_sample main
    LEFT JOIN dwh.master_department d ON main.department_id = d.department_id
    LEFT JOIN dwh.master_sensor S ON main.sensor_id = s.sensor_id
    LEFT JOIN dwh.master_product p ON main.product_id = p.product_id
);

SELECT * FROM dwh.v_data_sample LIMIT 5;

----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------

-- DROP DATABASE IF EXISTS airflow_db;
CREATE DATABASE airflow_db;

-- DROP ROLE IF EXISTS airflow_user;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;