## 👨‍💻 Built with
<img src="https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue" /> <img src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white"/> <img src="https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white" /> <img src="https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white" /> <img src="https://img.shields.io/badge/Numpy-777BB4?style=for-the-badge&logo=numpy&logoColor=white" /> 
<img src="https://airflow.apache.org/images/feature-image.png" width="100" height="27,5" />
<img src="https://cdn-images-1.medium.com/max/1000/1*-7Ro7fO__wwWz0iL9tucHQ.png" width="100" height="27,5" />
<img src="https://www.devagroup.pl/blog/wp-content/uploads/2022/10/logo-Google-Looker-Studio.png" width="100" height="27,5" />
<img src="https://www.scitylana.com/wp-content/uploads/2019/01/Hello-BigQuery.png" width="100" height="27,5" />

##  Descripction about project

### ℹ️Project info

This project is created to catch information about issued building permits in precisely in territorial units: voivodships, poviats and cadastral units. 
Getting information from official website of GUNB (General Office of Building Supervision): [GUNB]( https://wyszukiwarka.gunb.gov.pl/).

The aim of the project was to use apache airflow to manage workflows. 
The workflow is launched every month to collect newly issued permits with detailed information about the permit (building permissions table), collect aggregates for individual units in the last month, 2 and 3 (aggregates table), collect updated data on existing units (unit info), create month-to-month increments (to send the report). Additionally, a task in airflow to send a monthly report to the e-mail address about the updated number of records, the current number of records in the database and data validation (great expectations) in the csv table in terms of: 
- registration number is formed properly,
- type of permissions in set,
- category values in categories set,
- voviodeship names in set,
- cadastral unit symbol is formed properly,
- date column is not empty,
- voivodeship number is in set,

Additionally there was created container with jupyter notebook to create geovisualisations based on folium (which are using a openstreetmap). Created geovisualised report for cadastral districts and also voivodeships for last X months (you can chose 1-3 months back).

### 📧Exemplary e-mail report with GE validation:
#### E-mail sent on e-mail:
![email](https://github.com/AJSTO/GUNB_building_permissions/blob/master/img/img_report.png)
#### Great Expectations report in json format (attached to e-mail):
![IMG REPORT1](https://github.com/AJSTO/GUNB_building_permissions/blob/master/img/REPORT_IMG1.jpeg) 
### Increase records report attached to e-mail:
![IMG REPORT2](https://github.com/AJSTO/GUNB_building_permissions/blob/master/img/REPORT_IMG2.jpeg) 

### Extensions:

#### An extension of this project is the repo with FastAPI - [GUNB_API](https://github.com/AJSTO/GUNB_API)

## 🗒️Database created in BIGQUERY contains tables:

#### Building permissions detailed info:
 
```bash
'id_uniq', 'numer_ewidencyjny_system', 'numer_ewidencyjny_urzad',
'data_wplywu_wniosku_do_urzedu', 'nazwa_organu', 'wojewodztwo_objekt',
'obiekt_kod_pocztowy', 'miasto', 'terc', 'cecha', 'ulica',
'ulica_dalej', 'nr_domu', 'kategoria', 'nazwa_zam_budowlanego',
'rodzaj_zam_budowlanego', 'kubatura', 'stan', 'jednostki_numer',
'obreb_numer', 'numer_dzialki', 'numer_arkusza_dzialki',
'nazwisko_projektanta', 'imie_projektanta',
'projektant_numer_uprawnien', 'projektant_pozostali'
```
Clustered by 'jednostki_numer', 'kategoria', 'rodzaj_zam_budowlanego'.

#### Building permissions aggregates:

```bash
'unit_id', 'injection_date' + 'columns for every possible variation of category, type of permissions for last 1, 2, 3 months'
```
Partitioning by parameter month of year.

#### Teritorial unit's info:

```bash
'unit_type', 'unit_name', 'unit_number'
```

Clustered by 'unit_type'.

#### Increase month to month:

```bash
'date_of_calc', 'last_1_m', 'last_2_m', 'last_3_m'
```

## ⏩ DAG contain tasks:
- create_dataset (creating Bigquery dataset if not created);
- Subtask Group **creating_bigquery_tables**:
  - create_table_units_info;
  - create_table_all_permissions;
  - create_table_aggregates;
  - create_table_increase_info;
- Subtask Group **downloading_and_validate_data**:
  - download_building_permissions_data;
  - data validation;
- Subtask Group **updating_project_tables** (updating and appending rows to Biquery tables):
  - updating_units_info_table;
  - appending_rows_to_table_of_permissions;
  - generate_aggregates;
  - count_increase_of_given_permissions;
- sending_mail (sending report via e-mail);
- removing_files;

![IMG DAG](https://github.com/AJSTO/GUNB_building_permissions/blob/master/img/DAG_IMG.jpeg)

In this project I used PythonOperators and EmailOperator.

## 📦This project using 6 Docker containers:
- **Container with airflow-init**
    - Created to initialize Apache Airflow;
- **Container with airflow-webserver**
    - Created GUI to use Apache Airflow;
- **Container with airflow-triggerer**
- **Container with airflow-scheduler**
    - Created to deal with DAGs;
- **Container with PosgreSQL**
    - Created for Airflow using;
- **Container with Jupyter Notebook**
    - Notebook created to visualise a data, used libraries: 
      - pandas
      - numpy
      - folium
      - sqlalchemy

## 🌲 Project tree
```bash
.
├── Dockerfile # Dockerfile to create image of airflow_extending 
├── README.md
├── dags
│   ├── Bigquery_dag.py # Python script with DAG
│   ├── config.yaml # config file
│   ├── variables.py # python module with project variables
│   ├── schemas.py # python module with bigquery table schemas
│   └── YOUR_JSON_KEY_FROM_BIGQUERY.json # JSON key should be here
├── docker-compose.yaml # Yaml file to create containers by Docker Compose
├── logs # Airflow logs
├── notebook # Folder with jupyter notebook files
│   ├── OPENSTREETMAP_HTML # Folder for saving generated maps
│   ├── POLYGON_SHAPES # Here will be downloaded shp files
│   ├── configuration_files
│   │   ├── config.yaml # config file
│   │   ├── YOUR_JSON_KEY_FROM_BIGQUERY.json # JSON key should be here
│   │   └── requirements.txt # Requirements to create jupyter notebook
│   └── spatial report.ipynb # Jupyter notebook with data visualisation
└──requirements.txt # Requirements to create airflow

```
## 🔑 Setup 
Set up your gmail account to allow connections from airflow for e-mail raport purpose.

Detalied info about: 

[Sending Emails using Airflow EmailOperator Simplified: 9 Easy Steps](https://hevodata.com/learn/airflow-emailoperator/)

To run properly this project you should set variables in files: 
### ./dags/config.yaml:
- MY_PROJECT: # Project name in BIGQUERY
- MY_DATASET: # Dataset name in BIGQUERY
- MY_TABLE: # Table name in BIGQUERY
- TABLE_AGG: # Table name in BIGQUERY with all captured aggregates
- UNITS_INFO: # Table with names and units id's
- INCREASE_TABLE: # Table with increase values
- JSON_KEY_BQ: # JSON key filename, also paste file with JSON key in folder ./dags

### ./docker-compose.yaml:
#### Environment:
- AIRFLOW__SMTP__SMTP_USER: # Your gmail account
- AIRFLOW__SMTP__SMTP_PASSWORD: # Generated password via info in link
- AIRFLOW__SMTP__SMTP_MAIL_FROM: # Your gmail account
- AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?key_path=%2Fopt%2Fairflow%2Fdags%2F**YOUR-JSON-KEY-IN-THIS-FOLDER**.json&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&project=**YOUR-PROJECT-ID**&num_retries=5'
#### Database:
- POSTGRES_USER: 
- POSTGRES_PASSWORD:
- POSTGRES_DB:
#### Jupyter:
- change 'token' name in command:
```bash
command: bash -c "conda install fiona pyproj six && pip install -r ./configuration_files/requirements.txt && jupyter lab build --minimize=False && jupyter lab --ServerApp.token=TOKEN_NAME"
```

## ⚙️ Run Locally
- Clone the project
- Go to the project directory:
Type in CLI:
```bash
  $ ls
```
You should see this:
```bash
Dockerfile logs README.md notebook dags plugins docker-compose.yaml requirements.txt
```
Now create image of needed airflow extension:
```bash
  $ docker build -t extending_airflow:latest .
```
When created, to initialize airflow type:
```bash
  $ docker-compose up airflow-init 
```
Next run build of all other images needed:
```bash
  $ docker-compose up -d
```
Now airflow will start working.
If you want to stop airflow:
```bash
  $ docker-compose down -v
```

## ⚙️ Open airflow
**When all containers running, open browser and type:**
```bash
  localhost:8080
```
Next type password and username.

##  📊Data visualisation with folium
**After run of DAG you should run jupyter notebook. To start notebook you should type in your browser:**
```bash
  localhost:8888
```

🚨In case the notebook requires a token pass a value which was assigned to TOKEN_NAME in ./docker-compose.yaml🚨

Next choose a file 🗒️spatial_report.ipynb and run all cells to see data analyse.
Example of geovisualisation with folium:

#### CADASTRAL UNITS in last 3 months:
![IMG CADASTRAL](https://github.com/AJSTO/GUNB_building_permissions/blob/master/img/img_cadastral.png)

#### VOIVODSHIPS in last 3 months:
![IMG VOIVO](https://github.com/AJSTO/GUNB_building_permissions/blob/master/img/img_voivo.png)

## 🔎 Looker Studio
Link to generated report in looker for last month:

[Building permissions report for last month](https://lookerstudio.google.com/reporting/1bf425fd-8d79-456d-a49e-6cac0973dab3)
![IMG LOOKER](https://github.com/AJSTO/GUNB_building_permissions/blob/master/img/gunb-looker.gif)
