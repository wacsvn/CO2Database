import re
import sqlite3
from bs4 import BeautifulSoup

# Read in data files
with open('Co2.html') as f:
    co2_data = f.read()
with open('SeaLevel.csv') as f:
    sealevel_data = f.read()

# Parse CO2 data with BeautifulSoup
soup = BeautifulSoup(co2_data, 'html.parser')
co2_table = soup.find('table', summary="csv2html program output")
co2_dict = {}

for row in co2_table.find_all('tr')[3:]:
    columns = row.find_all('td')
    if len(columns) >= 7:
        year = int(columns[0].text)
        month = int(columns[1].text)
        decimal = float(columns[2].text)
        avg = float(columns[3].text)
        inter = float(columns[4].text)
        trend = float(columns[5].text)
        days = int(columns[6].text)
        co2_dict[(year, month)] = (decimal, avg, inter, trend, days)


# Print CO2 data for debugging
print("CO2 Data:")
if not co2_dict:
    print("No CO2 data found.")
else:
    print("CO2 data successfully found.")
    print(len(co2_dict))

# Parse sea level data
sealevel_dict = {}
for line in sealevel_data.splitlines()[4:]:
    parts = line.split(',')
    if len(parts) >= 2:
        date = parts[0]
        sea_level = float(parts[1])

        j1 = None
        if len(parts) >= 3:
            try:
                j1 = float(parts[2])
            except ValueError:
                j1 = None
        j2 = None
        j3 = None
        sealevel_dict[date] = (sea_level, j1, j2, j3)

class Database:

    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()

    def create_tables(self):
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS co2 (
                year INTEGER, 
                month INTEGER,
                decimal REAL,
                average REAL,
                inter REAL,
                trend REAL,
                days INTEGER
            )
        ''')
        print("CO2 Table created")  # Debug print

        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS sealevel (
                date TEXT, 
                sea_level REAL,
                j1 REAL,
                j2 REAL,
                j3 REAL
            )
        ''')
        print("Sea Level Table created")  # Debug print

    def insert_co2(self, data):
        sql = 'INSERT INTO co2 (year, month, decimal, average, inter, trend, days) VALUES (?, ?, ?, ?, ?, ?, ?)'
        for key, val in data.items():
            values = (key[0], key[1], *val)
            self.cur.execute(sql, values)
        print("CO2 Data inserted")  # Debug print

    def insert_sealevel(self, data):
        sql = 'INSERT INTO sealevel (date, sea_level, j1, j2, j3) VALUES (?, ?, ?, ?, ?)'
        for key, val in data.items():
            values = (key, *val)
            self.cur.execute(sql, values)
        print("Sea Level Data inserted")  # Debug print

    def search_co2(self, year, month):
        sql = 'SELECT * FROM co2 WHERE year = ? AND month = ?'
        self.cur.execute(sql, (year, month))
        result = self.cur.fetchone()
        return result

    def search_sealevel(self, date):
        sql = 'SELECT * FROM sealevel WHERE date = ?'
        self.cur.execute(sql, (date,))
        result = self.cur.fetchone()
        return result

    def delete_co2(self, date):
        sql = 'DELETE FROM co2 WHERE year = ? AND month = ?'
        year, month = date.split('-')
        self.cur.execute(sql, (year, month))
        self.conn.commit()
        print(f"CO2 data for {date} deleted")

    def delete_sealevel(self, date):
        sql = 'DELETE FROM sealevel WHERE date = ?'
        self.cur.execute(sql, (date,))
        self.conn.commit()
        print(f"Sea level data for {date} deleted")

    def commit(self):
        self.conn.commit()

# Create database and tables
db = Database('climate_data4.db')
db.create_tables()

# Insert data
db.insert_co2(co2_dict)
db.insert_sealevel(sealevel_dict)

# Commit changes
db.commit()

print('\nData imported to database!')

# Search Testing
# For co2
result = db.search_co2(1959, 1)
if result:
    print("CO2 data found:")
    for row in result:
        print(row)
else:
    print("No matching CO2 data found.")

# For sealevel
result = db.search_sealevel('1992.9614')
if result:
    print("Sea level data found:")
    print(result)
else:
    print("No matching sea level data found.")

# Delete Testing
db.delete_co2('1959-01')
db.delete_sealevel('1992.9614')

# ^ Deletes first rows of each table