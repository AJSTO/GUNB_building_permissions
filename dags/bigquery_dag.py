import os
import re
import shutil
import uuid
from datetime import datetime, timedelta, date

import pandas as pd
import pyarrow
import pandas_gbq

from google.cloud import bigquery
from google.oauth2 import service_account

import requests
from bs4 import BeautifulSoup as bs4
import wget
import zipfile

import yaml
import geopandas as gpd
import great_expectations as ge
from great_expectations.dataset import PandasDataset

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

from bigquery_dag.variables import URL, NOW, VOIVODESHIP_SYMBOL, CATEGORY_SYMBOL, TYPE_OF_PERM, VOIVODESHIP_NAMES, \
    LOCATION
from bigquery_dag.schemas import UNITS_INFO_SCHEMA, UNITS_CLUSTERING, ALL_PERMISSIONS_SCHEMA, \
    ALL_PERMISSIONS_CLUSTERING, ALL_PERMISSIONS_PARTITIONING, AGGREGATES_SCHEMA, AGGREGATES_PARTITIONING, \
    INCREASE_INFO_SCHEMA

default_args = {
    'owner': 'adam',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}
with open('dags/bigquery_dag/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
PROJECT_ID = config['PROJECT_ID']
DATASET_NAME = config['DATASET_NAME']
TABLE_PERMISSIONS = config['TABLE_PERMISSIONS']
TABLE_AGGREGATES = config['TABLE_AGGREGATES']
TABLE_UNITS_INFO = config['TABLE_UNITS_INFO']
TABLE_INCREASE = config['TABLE_INCREASE']
JSON_KEY_BQ = config['JSON_KEY_BQ']
KEY_PATH = f"dags/bigquery_dag/{JSON_KEY_BQ}"
CREDENTIALS = service_account.Credentials.from_service_account_file(
    KEY_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=CREDENTIALS.project_id,)


def download_building_permissions_data():
    """
    Creates a directory to save downloaded files and downloads a zip file containing building permission data.

    Parameters
        None.

    Returns:
        None.
    """

    os.makedirs('./download', exist_ok=True)
    wget.download(URL, f"./download/{NOW}.zip")
    with zipfile.ZipFile(f'./download/{NOW}.zip', 'r') as zip_ref:
        zip_ref.extractall('./download')


def updating_units_info_table():
    """
    Downloads the actual information about units from the GIS website, adds the information about unit_df name, type,
    and number to the BigQuery table, and saves the files to disk.

    The function scrapes the GIS website to find links to unit information. It then downloads each of the zip files
    containing the shapefiles for each unit type, extracts the shapefiles, and reads them into GeoDataFrames. For each
    unit type, it creates a list of dictionaries, with each dictionary containing the unit type, unit name, and unit
    number. It then saves the list of dictionaries as a Pandas DataFrame and uploads it to the specified BigQuery
    table.

    Parameters
        None.

    Returns:
        None.
    """
    unit_types = ['wojewodztwa', 'powiaty', 'jednostki_ewidencyjne']
    url = 'https://gis-support.pl/baza-wiedzy-2/dane-do-pobrania/granice-administracyjne/'
    units_url = {}
    # Scrape page
    r = requests.get(url)
    soup = bs4(r.content, 'html')
    scraped = soup.find_all('a', href=True)
    # Looping through captured links to find links with only unit_df types
    for url in scraped:
        for unit_type in unit_types:
            if '/' + unit_type + '.zip' in url['href']:
                units_url[unit_type] = url['href']
    units = []  # create to capture in list 3 dict with 3 types of units
    for unit_type, url in units_url.items():
        wget.download(url, f'./download/{unit_type}.zip')
        with zipfile.ZipFile(f'./download/{unit_type}.zip', 'r') as zip_ref:
            zip_ref.extractall('./download/units')
        unit_df = gpd.read_file(f'./download/units/{unit_type}.shp')
        unit_type_info = [
            {
                'unit_type': unit_type,
                'unit_name': name,
                'unit_number': number,
            }
            for name, number in zip(unit_df.JPT_NAZWA_, unit_df.JPT_KOD_JE)
        ]
        units += unit_type_info

    pandas_gbq.to_gbq(pd.DataFrame(units), f'{DATASET_NAME}.{TABLE_UNITS_INFO}',
                      project_id=f'{PROJECT_ID}', if_exists='replace',
                      credentials=CREDENTIALS, )


def data_validation(ti):
    """
    Validates crawled building permissions data using GreatExpectations.

    Checks the following criteria to ensure data validity:
    - Registration number is properly formatted.
    - Type of permission is in a given set of values.
    - Category values are in a given set of categories.
    - Voivodeship name is in a given set of names.
    - Cadastral unit symbol is properly formatted.
    - Surely date column is not empty.
    - Voivodeship number is in a given set of values.

    Parameters
        ti: Airflow task instance.

    Returns
        None.
    """
    registration_num = r'[A-Z]{2}-[A-Z]{2}'
    regex = re.compile(registration_num)
    cadastral_unit_form = r'\d{6}_\d'
    voivodeship_symbol_with_na = VOIVODESHIP_SYMBOL + ['na']
    build_perm_df = pd.read_csv('./download/wynik_zgloszenia.csv', sep='#')
    build_perm_df['data_wplywu_wniosku_do_urzedu'] = pd.to_datetime(
        build_perm_df.loc[:, 'data_wplywu_wniosku_do_urzedu']
    )
    # If table exists validate last 30 days if not validate all
    first_dag_run = ti.xcom_pull(task_ids='create_table_all_permissions', key='bigquery_table')
    if not first_dag_run:
        build_perm_df = build_perm_df[
            build_perm_df['data_wplywu_wniosku_do_urzedu'] >= pd.to_datetime(datetime.today() - timedelta(days=30))
        ]
    build_perm_df = build_perm_df[build_perm_df['numer_ewidencyjny_system'].apply(lambda x: bool(re.match(regex, x)))]
    build_perm_df['woj_num'] = build_perm_df['jednostki_numer'].apply(lambda x: str(x)[:2])
    # Build a Great Expectations suite
    dataset = PandasDataset(build_perm_df)
    dataset.expect_column_values_to_match_regex('numer_ewidencyjny_system', regex=registration_num, mostly=0.99)
    dataset.expect_column_values_to_be_in_set('rodzaj_zam_budowlanego', value_set=TYPE_OF_PERM, mostly=0.99)
    dataset.expect_column_values_to_be_in_set('kategoria', value_set=CATEGORY_SYMBOL, mostly=0.99)
    dataset.expect_column_values_to_be_in_set('wojewodztwo_objekt', value_set=VOIVODESHIP_NAMES, mostly=0.99)
    dataset.expect_column_values_to_match_regex('jednostki_numer', regex=cadastral_unit_form, mostly=0.99)
    dataset.expect_column_values_to_not_be_null('data_wplywu_wniosku_do_urzedu', mostly=0.99)
    dataset.expect_column_values_to_be_in_set('woj_num', value_set=voivodeship_symbol_with_na, mostly=0.99)

    # Save the expectation suite to json file
    dataset.save_expectation_suite(discard_failed_expectations=False,
                                   filepath=os.path.join(os.getcwd(), 'download', "validation_report.json"))


def appending_rows_to_table_of_permissions(ti):
    """
    Append all information about building permissions to the main table, validating data before pushing to table.

    If records younger than 30 days exist, append only those records. The function counts how many records are added to
    the table and how many records are in the table after the update.

    Parameters
        ti: Airflow task instance.

    Returns
        None.
    """
    first_dag_run = ti.xcom_pull(task_ids='create_table_all_permissions', key='bigquery_table')
    month_earlier = pd.to_datetime(datetime.today() - timedelta(days=30))
    checking_num_system = re.compile(r'[A-Z]{2}-[A-Z]{2}')  # to avoid rows with only designer info's
    checking_unit_cadastral = re.compile(r'\d{6}_\d')
    # Check num of total rows before update
    query = f"""
        SELECT 
            COUNT(*) AS total
        FROM 
            `{PROJECT_ID}.{DATASET_NAME}.{TABLE_PERMISSIONS}`
        ;
        """
    before_update = CLIENT.query(query)
    before_update = list(before_update)[0].total
    for chunk in pd.read_csv('./download/wynik_zgloszenia.csv', sep='#', chunksize=10000):
        chunk['data_wplywu_wniosku_do_urzedu'] = pd.to_datetime(chunk.loc[:, 'data_wplywu_wniosku_do_urzedu'])
        if not first_dag_run:
            chunk = chunk[chunk['data_wplywu_wniosku_do_urzedu'] >= month_earlier]
        if chunk.shape[0]:
            chunk = chunk[chunk['numer_ewidencyjny_system'].apply(
                lambda x: bool(re.match(checking_num_system, str(x))))]  # added to avoid rows with only designer info
            chunk = chunk[chunk['jednostki_numer'].apply(
                lambda x: bool(re.match(checking_unit_cadastral, str(x))))]  # validate units format
            chunk = chunk[chunk['jednostki_numer'].apply(lambda x: bool(str(x)[:2] in VOIVODESHIP_SYMBOL))]
            chunk = chunk[chunk['kategoria'].apply(lambda x: bool(str(x) in CATEGORY_SYMBOL))]
            chunk = chunk.drop(labels=['cecha.1'], axis=1)
            chunk = chunk.astype(str)
            chunk['uuid'] = [str(uuid.uuid4()) for _ in range(chunk.shape[0])]
            chunk = chunk.applymap(lambda x: x.replace("'", ''))  # to avoid errors because of '
            chunk['data_wplywu_wniosku_do_urzedu'] = pd.to_datetime(chunk.loc[:, 'data_wplywu_wniosku_do_urzedu'])
            pandas_gbq.to_gbq(
                chunk,
                f'{DATASET_NAME}.{TABLE_PERMISSIONS}',
                project_id=f'{PROJECT_ID}',
                if_exists='append',
                credentials=CREDENTIALS,
            )
    # Check num of total after updating
    query = f"""
    SELECT 
        COUNT(*) AS total
    FROM 
        `{PROJECT_ID}.{DATASET_NAME}.{TABLE_PERMISSIONS}`
    ;
    """
    total_rows = list(CLIENT.query(query))[0].total
    updated_rows = total_rows - before_update
    ti.xcom_push(key='num_of_rows', value=total_rows)
    ti.xcom_push(key='updated_rows', value=updated_rows)


def generate_aggregates(ti):
    """
    Generate aggregates from the updated BigQuery table of all permissions. Aggregate the number of permissions
    for each unit ID for the last 1, 2, and 3 months.

    Parameters
        ti: Airflow task instance.

    Returns
        None.
    """

    dates = [
        (date.today() - timedelta(days=90), 'last_3_m'),
        (date.today() - timedelta(days=60), 'last_2_m'),
        (date.today() - timedelta(days=30), 'last_1_m'),
    ]
    # Getting from bigquery all necessary columns and with date < 3 months to create aggregates
    query = f""" 
    SELECT 
        uuid, data_wplywu_wniosku_do_urzedu, jednostki_numer,
        kategoria, rodzaj_zam_budowlanego
    FROM 
        `{PROJECT_ID}.{DATASET_NAME}.{TABLE_PERMISSIONS}`
    WHERE
        data_wplywu_wniosku_do_urzedu > '{dates[0][0]}'
    ;
    """
    permissions_from_last_3_months = pandas_gbq.read_gbq(
        query, credentials=CREDENTIALS,
        progress_bar_type=None
    )
    # Extract substrings and add new columns
    permissions_from_last_3_months['powiat'] = permissions_from_last_3_months['jednostki_numer'].str[:4]
    permissions_from_last_3_months['wojewodztwo'] = permissions_from_last_3_months['jednostki_numer'].str[:2]
    permissions_from_last_3_months['zamierzenie'] = permissions_from_last_3_months['rodzaj_zam_budowlanego']\
        .str.split().str[0]
    permissions_from_last_3_months['count'] = 1
    permissions_from_last_3_months['data_wplywu_wniosku_do_urzedu'] = pd.to_datetime(
        permissions_from_last_3_months.loc[:, 'data_wplywu_wniosku_do_urzedu']
    ).dt.date
    # Create list of all possible unit numbers
    all_possible_units_ids = list(permissions_from_last_3_months['wojewodztwo'].unique()) + list(
        permissions_from_last_3_months['powiat'].unique()) + list(
        permissions_from_last_3_months['jednostki_numer'].unique())
    # Create table with all necessary columns in BQ
    column_names = sorted(
        list(
            set(
                [
                    f'cat_{category}_{type}_{time_distance}'
                    for category in CATEGORY_SYMBOL
                    for type in ['budowa', 'rozbudowa', 'odbudowa', 'nadbudowa']
                    for time_distance in ['last_1_m', 'last_2_m', 'last_3_m']
                ]
            )
        )
    )
    aggregate_df = pd.DataFrame(columns=column_names, index=all_possible_units_ids)
    aggregate_df['injection_date'] = pd.to_datetime(date.today())
    # Loop over date ranges and aggregate the data
    for date_, time_distance in dates:
        agg = permissions_from_last_3_months[
            permissions_from_last_3_months['data_wplywu_wniosku_do_urzedu'] > date_].groupby(
            ['wojewodztwo', 'powiat', 'jednostki_numer', 'kategoria', 'zamierzenie']
        )['count'].sum()
        for idx, count in agg.items():
            if count == 0:
                continue
            if idx[0]:
                unit_id = idx[0]
            elif idx[1]:
                unit_id = idx[1]
            else:
                unit_id = idx[2]
            col = f"cat_{idx[3]}_{idx[4]}_{time_distance}"
            aggregate_df.loc[unit_id, col] += count
    aggregate_df = aggregate_df.fillna(0)
    # Updating with all possible columns if table was created in this DAG run
    first_dag_run = ti.xcom_pull(task_ids='create_table_aggregates', key='bigquery_table')
    if first_dag_run:
        frame = pd.DataFrame(columns=['unit_id', 'injection_date'] + column_names)
        pandas_gbq.to_gbq(frame, f'{DATASET_NAME}.{TABLE_AGGREGATES}', project_id=PROJECT_ID, if_exists='replace',
                          credentials=CREDENTIALS)
    pandas_gbq.to_gbq(aggregate_df.reset_index().rename(
        columns={'index': 'unit_id'}).fillna(0),
        f'{DATASET_NAME}.{TABLE_AGGREGATES}',
        project_id=f'{PROJECT_ID}',
        if_exists='append',
        credentials=CREDENTIALS,
                      )


def count_increase_of_given_permissions():
    """
    Generate a report of the number of given permissions from the last 1, 2, and 3 months.

    Side Effects:
    - Saves the report to the specified Google BigQuery table.
    - Saves a CSV copy of the report to a local directory.

    Parameters
        None.

    Returns
        None.
    """
    dates = [
        (date.today() - timedelta(days=30), 'last_1_m'),
        (date.today() - timedelta(days=60), 'last_2_m'),
        (date.today() - timedelta(days=90), 'last_3_m'),
    ]
    query = f""" 
        SELECT 
            uuid, data_wplywu_wniosku_do_urzedu, jednostki_numer,
            kategoria, rodzaj_zam_budowlanego
        FROM 
            `{PROJECT_ID}.{DATASET_NAME}.{TABLE_PERMISSIONS}`
        WHERE
            data_wplywu_wniosku_do_urzedu > '{dates[0][0]}'
        ;
        """
    permissions_from_last_3_months = pandas_gbq.read_gbq(
        query, credentials=CREDENTIALS,
        progress_bar_type=None
    )
    # Count number of permissions in each date range
    counts = {}
    for start_date, label in dates:
        count = len(
            permissions_from_last_3_months[
                permissions_from_last_3_months['data_wplywu_wniosku_do_urzedu'].dt.date > start_date
            ]
        )
        counts[label] = [count]
    increase_info = {'date_of_calc': [pd.Timestamp(date.today())], **counts}
    increase_info_df = pd.DataFrame(increase_info)
    pandas_gbq.to_gbq(increase_info_df, f'{DATASET_NAME}.{TABLE_INCREASE}',
                      project_id=f'{PROJECT_ID}',
                      if_exists='append',
                      credentials=CREDENTIALS,
                      )
    query = f"""
            SELECT 
                *
            FROM 
                `{PROJECT_ID}.{DATASET_NAME}.{TABLE_INCREASE}`
            ;
            """
    increase_info_df = pandas_gbq.read_gbq(query, credentials=CREDENTIALS, progress_bar_type=None)
    increase_info_df.to_csv(f"./download/increase_raport-{date.today()}.csv")


def removing_downloaded_files():
    """
    Remove all downloaded and created files.

    Parameters
        None.

    Returns:
        None.
    """
    shutil.rmtree('download')


with DAG(
        default_args=default_args,
        dag_id='GUNB_building_permissions',
        description='Updating Bigquery with building permissions',
        start_date=datetime(2022, 12, 1),
        schedule_interval='@monthly',
        catchup=False,
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME
    )
    with TaskGroup(group_id='creating_bigquery_tables') as creating_bigquery_tables:
        create_table_units_info = BigQueryCreateEmptyTableOperator(
            task_id='create_table_units_info',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_UNITS_INFO,
            schema_fields=UNITS_INFO_SCHEMA,
            cluster_fields=UNITS_CLUSTERING,
        )
        create_table_all_permissions = BigQueryCreateEmptyTableOperator(
            task_id='create_table_all_permissions',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_PERMISSIONS,
            schema_fields=ALL_PERMISSIONS_SCHEMA,
            cluster_fields=ALL_PERMISSIONS_CLUSTERING,
            time_partitioning=ALL_PERMISSIONS_PARTITIONING
        )
        create_table_aggregates = BigQueryCreateEmptyTableOperator(
            task_id='create_table_aggregates',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_AGGREGATES,
        )
        create_table_increase_info = BigQueryCreateEmptyTableOperator(
            task_id='create_table_increase_info',
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_INCREASE,
            schema_fields=INCREASE_INFO_SCHEMA
        )
        [create_table_units_info, create_table_all_permissions, create_table_aggregates, create_table_increase_info]
    with TaskGroup(group_id='download_and_validate_data') as download_and_validate_data:
        download_building_permissions_data = PythonOperator(
            task_id='download_building_permissions_data',
            python_callable=download_building_permissions_data,
        )
        data_validation = PythonOperator(
            task_id='data_validation',
            python_callable=data_validation,
        )
        download_building_permissions_data >> data_validation
    with TaskGroup(group_id='updating_project_tables') as updating_project_tables:
        updating_units_info_table = PythonOperator(
            task_id='updating_units_info_table',
            python_callable=updating_units_info_table,
        )
        appending_rows_to_table_of_permissions = PythonOperator(
            task_id='appending_rows_to_table_of_permissions',
            python_callable=appending_rows_to_table_of_permissions,
        )
        generate_aggregates = PythonOperator(
            task_id='generate_aggregates',
            python_callable=generate_aggregates,
        )
        count_increase_of_given_permissions = PythonOperator(
            task_id='count_increase_of_given_permissions',
            python_callable=count_increase_of_given_permissions
        )
        updating_units_info_table >> appending_rows_to_table_of_permissions >> generate_aggregates >> \
            count_increase_of_given_permissions
    sending_email_report = EmailOperator(
        task_id='sending_email_report',
        to='adam100larczyk@gmail.com',
        subject=f"Database updated - {datetime.today()}",
        html_content="<b><h1>Database updated with <b>{{ti.xcom_pull(key='updated_rows')}}</b> rows, now contain"
                     " <b>{{ti.xcom_pull(key='num_of_rows')}}</b> rows.</h1></b>",
        files=[
            f"/opt/airflow/download/increase_raport-{date.today()}.csv",
            "/opt/airflow/download/validation_report.json",
               ],
    )
    removing_downloaded_files = PythonOperator(
        task_id='removing_downloaded_files',
        python_callable=removing_downloaded_files,
    )

    create_dataset >> creating_bigquery_tables >> download_and_validate_data >> updating_project_tables >> \
        sending_email_report >> removing_downloaded_files
