# Data Engineer Test - Data Wow
- [How to Run Docker Containers](#1-how-to-run-docker-containers)
- [How to Set Up the Data Pipeline](#2-how-to-set-up-the-data-pipeline)
- [Database Design](#2-database-design)
- [Data Pipeline Design](#data-pipeline-design)

<details>
    <summary>Table of Contents</summary>

1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
    1. [How to Run Docker Containers](#1-how-to-run-docker-containers)
        1. [Install Docker](#step-1-install-docker)
        2. [Set Up the Project Structure](#step-2-set-up-the-project-structure)
        3. [Configure Docker Compose Settings (optional)](#step-3-optional-configure-docker-compose-settings)
        4. [Start Docker Containers](#step-4-start-docker-containers)
    2. [How to Set Up the Data Pipeline](#2-how-to-set-up-the-data-pipeline)
3. [PostgreSQL Database Design](#postgresql-database-design)
    1. [Database Overview](#1-database-overview)
    2. [Database Design](#2-database-design)
    3. [Data Loading Process](#3-data-loading-process)
4. [Data Pipeline Design](#data-pipeline-design)
    1. [Clearing Data from PostgreSQL](#1-clearing-data-from-postgresql)
    2. [ETL: Extraction, Transformation, and Loading](#2-etl-extraction-transformation-and-loading)
5. [My Limitations & Improvement Approaches](#my-limitations--improvement-approaches)
</details>

## Introduction
This project is part of the Data Engineer Test at Data Wow, and its main objective is to build a data pipeline. The pipeline will use **PostgreSQL** as the database, be orchestrated with **Airflow**, and be containerized using **Docker Compose**.

<!-- ================================================== -->
<!-- ================================================== -->

## Quick Start

<!-- ================================================== -->

### 1) How to Run Docker Containers

#### Step 1: Install Docker
If you don't have Docker installed on your system, you can easily install it by following the instructions in the [Docker Documentation](https://docs.docker.com/engine/install/). Choose the installation method that matches your preferred operating system.

#### Step 2: Set Up the Project Structure
Before running Docker containers, it's important to organize the project directories properly. You have two options for achieving this:
##### Option 1: Clone my GitHub Repository
You can clone this repository directly using the following command:
```bash
git clone https://github.com/mncy-earth/data-wow-test.git
```
##### Option 2: Manual Setup
Alternatively, you can set up the project structure manually as follows:
```bash
project-folder/
 |— dags/
 |   |— data_pipeline.py
 |   |— query_data.py
 |— logs/
 |   |—
 |— sampledata/
 |   |— data_sample/
 |       |—
 |   |— requirements.txt
 |   |— sampledata_new.py
 |— .env
 |— docker-compose.yaml
 |— postgresql_setup.sql
```
<details>
    <summary>Explanation of each directory and file</summary>

- `dags/`: Folder of Python scripts to do DAGs
    - `data_pipeline.py`: **Python script for running data pipeline**
    - `query_data.py`: Python script for determining the number of rows in a PostgreSQL database
- `logs/`: Folder of log files generated by Airflow when running DAGs
- `sampledata/`: Folder of data generation
   - `data_sample/`: Folder containing sample data in parquet files
   - `requirements.txt`: Text file listing required Python packages for `sampledata_new.py` (provided by Data Wow's team)
  - `sampledata_new.py`: Python script for data generation (provided by Data Wow's team)
- `.env`: Configuration file for managing environment variables.
- `docker-compose.yaml`: Docker Compose configuration file for defining and managing multi-container Docker containers for this project.
- `postgresql_setup.sql`: SQL script for PostgreSQL database initialization.
</details>

#### Step 3: Configure Docker Compose Settings (Optional)
Before running Docker, you have the option to adjust some configurations:

- **SKIPPING DATA GENERATION**: If you've already generated the data, you can avoid regenerating it by commenting out the `sampledata` service in the `docker-compose.yaml` file. **However, please make sure to locate your existing files inside the `data_sample` directory.** Please note that this folder is empty in my repository.
- **CUSTOMIZE AIRFLOW WEBSERVER CREDENTIALS**: Modify the Airflow Webserver's username and password in the `.env` file by updating the following variables:
    - `AIRFLOW_WWW_USERNAME_INIT`
    - `AIRFLOW_WWW_PASSWORD_INIT`

These optional configurations allow you to tailor the Docker Compose settings to your specific needs.

#### Step 4: Start Docker Containers
Run the following command in your terminal to start the containers:
```
docker-compose up
```
Alternatively, you can run the containers in the background using the `-d` flag:
```
docker-compose up -d
```
The containers will take a few moments to start. You can check their status by opening [localhost:8080](http://localhost:8080) in your web browser.

<!-- ================================================== -->

### 2) How to Set Up the Data Pipeline
After starting all containers, follow these steps to initiate the data pipeline:
1. Open [localhost:8080](http://localhost:8080) in your web browser.
2. Log in using the default credentials:
   - Username: `admin`
   - Password: `password`

   If you have customized the Airflow Webserver's credentials, please use your updated values.`
3. Trigger the DAG named `data_pipeline`.

Please note that the current data pipeline can only run manually and loads all sample data in a single run.
If you wish to make further adjustments to the DAG or task configuration, refer to the [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html) for options like scheduling, email notifications on success or failure, and more.
For more detailed information on current building the data pipeline, refer to the [Data Loading Process](#3-data-loading-process) and [Data Pipeline Design](#data-pipeline-design) sections.

<!-- ================================================== -->
<!-- ================================================== -->

## PostgreSQL Database Design

In this section, you'll find a comprehensive overview of my PostgreSQL database setup, including its structure and data loading process.

<!-- ================================================== -->

### 1) Database Overview

Here are the key components of my database setup:

| Database        | Schema   | Description                                      |
|-----------------|----------|--------------------------------------------------|
| `postgres_db`   | `public` | currently empty and serves no specific purpose   |
|                 | `dwh`    | dedicated to serving as a data warehouse         |
| `airflow_db`    | `public` | used to store metadata related to Airflow        |

During the PostgreSQL container initialization, three roles with different permissions were created:

| Username        | Password        | Permissions                                                 |
|-----------------|-----------------|-------------------------------------------------------------|
| `postgres_user` | `postgres_pass` | holds all privileges on both `postgres_db` and `airflow_db` |
| `dwh_user`      | `dwh_pass`      | holds all permissions on the `dwh` schema of `postgres_db`  |
| `airflow_user`  | `airflow_pass`  | holds all privileges on `airflow_db`                        |

<!-- ================================================== -->

### 2) Database Design

For a visual representation of my database design, please refer to the image below:

![Database Design](/others/images/Database%20Design.png)

<!-- ================================================== -->

### 3) Data Loading Process

The above figure not only illustrates the database design but also provides insights into our data loading process. We store our source data locally, consisting of $43,201$ files, each containing $1,914$ rows. To prevent task failures due to memory constraints, a batching approach was implemented so that we can process data incrementally.

For instance, by loading $720$ files (equivalent to $1,380,480$ rows) at a time, it divides the process into 61 batches (with the last batch consisting of just one file), as demonstrated in the image.

This approach can be considered as loading data twice daily, ensuring efficient data management and performance optimization. These strategies collectively enhance our data handling capabilities within the PostgreSQL database.

<details>
    <summary>Calculation of different batch size (for the given sample data)</summary>

| Batch Size        | Batch Description   | Frequency        | Batch Count | Max Rows per Batch   |
|:------------------|:--------------------|:-----------------|------------:|---------------------:|
| 1,440 files       | daily               | every 24 hours   | 31          | 2,756,160            |
| 720 files         | twice daily         | every 12 hours   | 61          | 1,378,080            |
| 480 files         | three times a day   | every 8 hours    | 121         | 918,720              |
| 360 files         | four times a day    | every 6 hours    | 241         | 689,040              |

Note that the last batch has a batch size of 1 file.
</details>

<!-- ================================================== -->
<!-- ================================================== -->

## Data Pipeline Design
The data pipeline consists of two tasks within a Directed Acyclic Graph (DAG):
> [clear_data_from_postgresql](#1-clearing-data-from-postgresql) >> [extract_and_load_from_parquet](#2-etl-extraction-transformation-and-loading)

<!-- ================================================== -->

### 1) Clearing Data from PostgreSQL
To enable the data pipeline to be rerun without causing data duplication, it's essential to clear the table before loading new data.

For this given sample data, all the data is loaded in a single run. Consequently, the SQL script is designed to remove all data from the table without dropping it, as indicated in the following SQL command:
```sql
TRUNCATE TABLE dwh.data_sample;
```
However, if this data pipeline were to be used in a real production environment with scheduled runs, data scoping should be taken into account.

To illustrate, if we were to schedule daily runs, we could scope the files to start with the same `yyyy-mm-dd` (e.g., `2023-01-01`). In such a scenario, the SQL command could be revised as follows:
```sql
DELETE FROM dwh.data_sample WHERE create_at::VARCHAR(10) = '2023-01-01';
```

<!-- ================================================== -->

### 2) ETL: Extraction, Transformation, and Loading
Once we've ensured that upcoming data won't duplicate existing records in the database, we can proceed with the ETL process.

Recall [Data Loading Process](#3-data-loading-process), we utilize a batch approach to efficiently manage large volumes of data. This involves partitioning the list of Parquet files into batches, as shown below:
```python
batches = [parquet_files[idx:idx+no_files] for idx in range(0, len(parquet_files), no_files)]
```
This partitioning strategy allows us to process the data in manageable chunks, preventing memory-related issues and preserving data sequence. For each batch, the ETL process follows these steps:

1. **EXTRACTION:** Read each Parquet file into a Pandas DataFrame using `pd.read_parquet()`

2. **TRANSFORMATION:** Concatenate all the DataFrames within a batch using `pd.concat()`

3. **LOADING:** Load the transformed data into `dwh.data_sample` using [SQLAlchemy](https://docs.sqlalchemy.org/en/20/core/engines.html)'s `create_engine()` and [Pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)'s `to_sql()`

<!-- ================================================== -->
<!-- ================================================== -->

## My Limitations & Improvement Approaches