# Airflow Sales Data Pipeline

## Project Overview

This project is a simple ETL data pipeline built using **Apache Airflow, Python, and Pandas**.

The pipeline reads raw sales data from a CSV file, performs data cleaning and validation, and then saves the processed data into a new CSV file.

Apache Airflow is used to orchestrate and execute each step of the pipeline as separate tasks inside a DAG.

---

## Technologies Used

* Python
* Apache Airflow
* Pandas
* Ubuntu
* Git
* GitHub

---

## Pipeline Workflow

The pipeline follows the below flow:

```text
Raw CSV Data
     ↓
Read Raw Data
     ↓
Clean Data
     ↓
Validate Data
     ↓
Save Processed Data
```

The Airflow DAG executes the tasks in the following order:

```text
read_raw_data
      ↓
clean_data
      ↓
validate_data
      ↓
save_data
```

---

## Project Structure

```text
airflow-sales-data-pipeline/
│
├── dags/
│   └── sales_data_pipeline.py
│
├── data/
│   └── airflow_sample_sales.csv
│
├── screenshots/
│   ├── dag_graph.png
│   └── successful_run.png
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Dataset

The input dataset contains sample sales information such as:

* Order ID
* Customer Name
* Product
* Quantity
* Sales Amount
* Order Status

The sample dataset is stored inside:

```text
data/airflow_sample_sales.csv
```

---

## Data Processing Steps

### 1. Read Raw Data

The first task reads the raw CSV file using Pandas.

It also displays basic information such as:

* Number of records
* Column names

The raw data is then saved into a staging file.

---

### 2. Clean Data

The cleaning task performs the following operations:

* Removes duplicate rows
* Removes rows with missing important values
* Standardizes order status values to uppercase
* Removes records with invalid quantities

The cleaned data is then saved back into the staging file.

---

### 3. Validate Data

The validation task checks the cleaned dataset before allowing it to continue.

The following validations are performed:

* `order_id` should not contain missing values
* `quantity` should be greater than zero
* `sales_amount` should not be negative
* `order_status` should contain only allowed values

Allowed order statuses are:

```text
COMPLETED
CANCELLED
PENDING
```

If any validation fails, the Airflow task raises an error and the pipeline stops.

---

### 4. Save Processed Data

After successful validation, the cleaned data is saved as the final processed CSV file.

```text
processed_sales.csv
```

This file contains the final validated dataset.

---

## Airflow DAG

The DAG name used in this project is:

```text
sales_data_pipeline
```

The DAG contains four PythonOperator tasks:

```text
read_raw_data
clean_data
validate_data
save_data
```

Task dependencies are defined as:

```python
read_data >> clean >> validate >> save
```

---

## How to Run the Project

### 1. Clone the repository

```bash
git clone <your-repository-url>
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Apache Airflow

Initialize Airflow if required:

```bash
airflow db migrate
```

Start the Airflow scheduler:

```bash
airflow scheduler
```

Start the Airflow web server:

```bash
airflow webserver
```

### 4. Open Airflow UI

Open the Airflow interface in your browser and locate:

```text
sales_data_pipeline
```

Enable the DAG and trigger it manually.

---

## Key Concepts Demonstrated

This project demonstrates the following Data Engineering concepts:

* ETL pipeline development
* Data ingestion
* Data cleaning
* Data validation
* Data staging
* Pipeline orchestration
* Task dependencies
* Workflow scheduling
* Error handling
* Apache Airflow DAGs
* Python-based data processing

---

## Future Improvements

The pipeline can be enhanced further by adding:

* Database source and target systems
* Incremental data loading
* Airflow scheduling
* Retry handling
* Logging and monitoring
* Data quality checks
* Email alerts for pipeline failures
* Audit tables
* Cloud storage integration
* Docker support

