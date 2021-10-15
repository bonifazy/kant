
#            for: 'main.py', 'parser.py', 'db.py'
#
# Additional print() extra info about parsing or working with database
DEBUG = True

# Starting rating (updating) for items, this prices and this availability (sizes) of
# 'products', 'prices', 'instock_...' tables
# Normal state rating: 1
# Not in stock rating: 0
# Updated status rating: increment of current
RATING = 1

#            for: 'main.py'
#
# Similar recording format as in the size upload from AVAILABLE request
SHOPS = ('Nagornaya', 'Timiryazevskaya', 'TeplyStan', 'Altufevo')

# Brand names to distribute main description items to starting addition to 'products' table and availability to
# 'instock' tables more correctly
BRANDS = ['Asics', 'Saucony', 'Mizuno', 'Hoka', 'Adidas', 'Salomon', 'Brooks', 'On', '361°', 'Raidlight']

# Links to append and update items, prices and avalaibility items-- running shoes
BRANDS_URLS = [
    'https://www.kant.ru/catalog/shoes/running-shoes/',
    'https://www.kant.ru/catalog/shoes/running-shoes/brand-asics/',
    'https://www.kant.ru/catalog/shoes/running-shoes/brand-saucony/',
    'https://www.kant.ru/catalog/shoes/running-shoes/brand-mizuno/',
    'https://www.kant.ru/catalog/shoes/running-shoes/brand-hoka/',
    'https://www.kant.ru/catalog/shoes/running-shoes/brand-adidas/',
    'https://www.kant.ru/catalog/shoes/running-shoes/krossovki/brand-salomon/',
    'https://www.kant.ru/brand/brooks/products/',
    'https://www.kant.ru/brand/on/products/',
    'https://www.kant.ru/brand/361/products/',
    'https://www.kant.ru/brand/raidlight/products/'
]

# json file to export card description (an optional)
JSON_FILE = 'card.json'

# csv file to export card description (as default in 'to' parameter from export() method)
CSV_FILE = 'card.csv'

# csv file to export card description (as default in 'to' parameter from export() method)
XML_FILE = 'card.xml'

#
#            for: 'db.py'
#
# Database file. Starting tables structure from Django project, so 'id' is optional (not used explicitly from this proj)
DB_NAME = 'db.sqlite3'

#
#            for: 'parser.py'
#
# Timeout between parallel page loads, sec: as usual from 0.2 to 1 sec.
TIMEOUT = 1

# Count of parallel loads per one async working request, urls to parallel work: as usual from 5 to 30.
CHUNK = 20

# Link to get size, this count of items on each running shoes by this code
# response return json of all departments of kant.ru local shops with size, count and id of offline shop by unic id
# which depends on unic 'code'
AVAILABLE = "http://www.kant.ru/ajax/loadTableAvailability.php"
