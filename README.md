# Data Engineer Test - Data Wow
- [How to Run Docker Containers](#1-how-to-run-docker-containers)
- [How to Set Up the Data Pipeline](#2-how-to-set-up-the-data-pipeline)
- [Database Design](#2-database-design)
- [Data Pipeline Design](#data-pipeline-design)
- [My Limitations & Improvement Approaches](#my-limitations--improvement-approaches)

<details>
    <summary><i>Click here to see document outline</i></summary>

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
    1. [Resource Constraint](#1-resource-constraint)
    2. [Time Constraint](#2-time-constraint)
    3. [Data Constraint](#3-data-constraint)
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
##### <u>Option 1</u>: Clone my GitHub Repository
You can clone this repository directly using the following command:
```bash
git clone https://github.com/mncy-earth/data-wow-test.git
```
Navigate to the project directory::
```bash
cd data-wow-test
```
Create a `logs` folder:
```bash
mkdir ./logs
```
Now, you're ready to proceed with next step.
##### <u>Option 2</u>: Manual Setup
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
    <summary><i>Click here to read description of each directory and file</i></summary>

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
Once your environment is set up, simply use this command in your terminal to start the containers:
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
After starting all containers, you can follow these steps to initiate the data pipeline:
1. Open [localhost:8080](http://localhost:8080) in your web browser.
2. Login using the default credentials:
   - Username: `admin`
   - Password: `password`

   NOTE: *If you have customized the Airflow Webserver's credentials, please use your updated values.*
3. Trigger the `data_pipeline` DAG, as shown in the image below.
![Airflow - Trigger Data Pipeline](/others/images/Airflow%20-%20Trigger%20Data%20Pipeline.png)

<details><summary><i>Click here for a guide on viewing data pipeline logs</i></summary>

![Airflow - See Logs Step 1](/others/images/Airflow%20-%20See%20Logs%20(1).png)
![Airflow - See Logs Step 2](/others/images/Airflow%20-%20See%20Logs%20(2).png)
![Airflow - See Logs Step 3](/others/images/Airflow%20-%20See%20Logs%20(3).png)
</details><br>

Please note that the current data pipeline can only run manually and loads all sample data in a single run.
If you wish to make further adjustments to the DAG or task configuration, refer to the [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html) for options like scheduling, email notifications on success or failure, and more.
For more detailed information on the current building of the data pipeline, refer to the [Data Loading Process](#3-data-loading-process) and [Data Pipeline Design](#data-pipeline-design) sections.

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
| `postgres_db`   | `dwh`    | dedicated to serving as a data warehouse         |
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

In the PostgreSQL database, the table of interest is `data_sample`, which is located within the `postgres_db` database, under the `dwh` schema. This table contains a total of 5 columns, detailed as follows:
| Column Name       | Data Type           |
| ----------------- | ------------------- |
| department_name   | VARCHAR(32)         |
| sensor_serial     | VARCHAR(64)         |
| create_at         | TIMESTAMP           |
| product_name      | VARCHAR(16)         |
| product_expire    | TIMESTAMP           |

<!-- ================================================== -->

### 3) Data Loading Process

The above figure not only illustrates the database design but also provides insights into the data loading process. We store our source data locally, consisting of $43,201$ files, each containing $1,914$ rows. To prevent task failures due to memory constraints, a batching approach was implemented so that we can process data incrementally.

For instance, by loading $720$ files (equivalent to $1,380,480$ rows) at a time, it divides the process into 61 batches (with the last batch consisting of just one file), as demonstrated in the image.

This approach can be considered as loading data twice daily, ensuring efficient data management and performance optimization. These strategies collectively enhance our data handling capabilities within the PostgreSQL database.

<details>
    <summary><i>Click here to see the calculation of different batch sizes (for the given sample data)</i></summary>

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

During the 5-day development of the data pipeline, several limitations slowed down the creation of an efficient data pipeline capable of completing its tasks within the desired 30-minute timeframe. Acknowledging these challenges is essential for future success. Here are some key limitations and potential improvement approaches:

### 1) Resource Constraint

A significant limitation was the constrained resources on my local machine. With only 8 GB of RAM and less than 1 GB available after starting Docker containers, achieving sub-minute completion for tasks involving over 6 GB of data loading was challenging.

While testing the pipeline with a small number of Parquet files showed functionality, scaling up to handle a larger volume led to unexpected errors. This experience underscores the importance of resource management in real-world scenarios.

In the absence of these limitations, I could explore resource-intensive solutions like **batch loading** and leveraging **powerful ETL tools**.

### 2) Time Constraint

This experience highlighted the need to be ready for unexpected issues. I spent a huge amount of time repeatedly testing specific approaches. Not being prepared to handle large amounts of data caused longer troubleshooting times.

<details>
    <summary><i>Click here to explore the experimented strategies</i></summary>

- **Batch Size Optimization:** Tuning the appropriate batch size for the current data pipeline.
- **Table Normalization:** Attempting to normalize tables by integrating them with the three master tables to improve data loading performance and adapt to future changes in the data structure. However, this approach introduced complexities when joining tables and maintaining data consistency.
- **PySpark Integration:** Exploring PySpark integration, although it encountered issues that need further investigation. The PySpark command I aimed to test is as follows:
    ```Python
    spark.read.option("header","false").schema(sampledata_schema).option("recursiveFileLookup","true").parquet(sampledata_folder_path)
    ```
</details><br>

With additional time, I intend to investigate alternative solutions, including utilizing **cloud technologies**, leveraging **distributed processing frameworks** like Apache Spark, configuring parameters to align with requirements, and **optimizing** the algorithms.

### 3) Data Constraint

The absence of a data dictionary and the use of mock data made it challenging to design the pipeline effectively to meet the requirements.

Upon analyzing the sample data generation process, it appears to resemble streaming data. However, it remains unclear whether the data should be loaded in near-real-time or if it's acceptable to schedule the job for later loading. This information would guide decisions on batch sizes and **performance tracking** in the data pipeline.

Another concern is that the data seems to follow a time series pattern, but it's uncertain if the data sequence is prioritized (although I assume it is). If the sequence isn't a priority, implementing **multiprocessing** could reduce processing time.
