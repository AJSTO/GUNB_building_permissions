# TO CREATE UNITS INFO TABLE
UNITS_INFO_SCHEMA = [
    {"name": "unit_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "unit_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "unit_number", "type": "STRING", "mode": "REQUIRED"},
]
UNITS_CLUSTERING = ["unit_type", ]

# TO CREATE ALL PERMISSIONS TABLE
ALL_PERMISSIONS_SCHEMA = [
    {"name": "uuid", "type": "STRING", "mode": "REQUIRED"},
    {"name": "numer_ewidencyjny_system", "type": "STRING", "mode": "REQUIRED"},
    {"name": "numer_ewidencyjny_urzad", "type": "STRING"},
    {"name": "data_wplywu_wniosku_do_urzedu", "type": "TIMESTAMP"},
    {"name": "nazwa_organu", "type": "STRING"},
    {"name": "wojewodztwo_objekt", "type": "STRING"},
    {"name": "obiekt_kod_pocztowy", "type": "STRING"},
    {"name": "miasto", "type": "STRING"},
    {"name": "terc", "type": "STRING"},
    {"name": "cecha", "type": "STRING"},
    {"name": "ulica", "type": "STRING"},
    {"name": "ulica_dalej", "type": "STRING"},
    {"name": "nr_domu", "type": "STRING"},
    {"name": "kategoria", "type": "STRING"},
    {"name": "nazwa_zam_budowlanego", "type": "STRING"},
    {"name": "rodzaj_zam_budowlanego", "type": "STRING"},
    {"name": "kubatura", "type": "STRING"},
    {"name": "stan", "type": "STRING"},
    {"name": "jednostki_numer", "type": "STRING"},
    {"name": "obreb_numer", "type": "STRING"},
    {"name": "numer_dzialki", "type": "STRING"},
    {"name": "numer_arkusza_dzialki", "type": "STRING"},
    {"name": "nazwisko_projektanta", "type": "STRING"},
    {"name": "imie_projektanta", "type": "STRING"},
    {"name": "projektant_numer_uprawnien", "type": "STRING"},
    {"name": "projektant_pozostali", "type": "STRING"},
]
ALL_PERMISSIONS_CLUSTERING = ["jednostki_numer", "kategoria", "rodzaj_zam_budowlanego", ]
ALL_PERMISSIONS_PARTITIONING = {'type': 'MONTH'}

# TO CREATE AGGREGATES TABLE
AGGREGATES_SCHEMA = [
    {"name": "unit_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "injection_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
]
AGGREGATES_PARTITIONING = {'type':'MONTH'}

# TO CREATE INCREASE INFO TABLE
INCREASE_INFO_SCHEMA = [
    {"name": "date_of_calc", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "last_1_m", "type": "INT64"},
    {"name": "last_2_m", "type": "INT64"},
    {"name": "last_3_m", "type": "INT64"},
]
