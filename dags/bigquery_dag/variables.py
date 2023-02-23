from datetime import datetime

# Location of dataset
LOCATION = 'EU'
# Today date
NOW = datetime.today().strftime('%Y-%m-%d')
# URL to zip file of building permissions
URL = 'https://wyszukiwarka.gunb.gov.pl/pliki_pobranie/wynik_zgloszenia.zip'
# Symbols for voivodeship validation in cadastral unit
VOIVODESHIP_SYMBOL = [
    '02', '04', '06', '08', '10', '12',
    '14', '16', '18', '20', '22', '24',
    '26', '28', '30', '32'
]
# Names of voivodeships
VOIVODESHIP_NAMES = [
    'podlaskie', 'kujawsko-pomorskie', 'dolnośląskie', 'łódzkie', 'lubuskie', 'pomorskie',
    'małopolskie', 'lubelskie', 'warmińsko-mazurskie', 'opolskie', 'wielkopolskie', 'podkarpackie',
    'śląskie', 'świętokrzyskie', 'mazowieckie', 'zachodniopomorskie',
]
# Symbols from category validation
CATEGORY_SYMBOL = [
    'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X'
    'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII',
    'XIX', 'XX', 'XXI', 'XXII', 'XXIII', 'XXIV', 'XXV', 'XXVI',
    'XXVII', 'XXVIII', 'XXIX', 'XXX',
]
# Types of building permissions
TYPE_OF_PERM = [
    'budowa nowego/nowych obiektów budowlanych',
    'rozbudowa istniejącego/istniejących obiektów budowlanych',
    'odbudowa istniejącego/istniejących obiektów budowlanych',
    'nadbudowa istniejącego/istniejących obiektów budowlanych',
]

