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

    def QueryBuilder(self, Data_Base, Query_Type, Query_Tuple):
        '''
        Build a query string based on the given parameters and execute it in the database.

        Args:
            Data_Base (str): The name of the database table. (really this should've been called table)
            Query_Type (str): The type of query (e.g., "INSERT", "DELETE", "SELECT", "TABLE", "CREATE").
            Query_Tuple (tuple): A tuple containing the data for the query.

        Returns:
            str: The constructed query string.
        '''
        if Query_Type == "INSERT":
            query_string = f"INSERT INTO {Data_Base} VALUES {Query_Tuple}"
        elif Query_Type == "DELETE":
            query_string = f"DELETE FROM {Data_Base} WHERE {Query_Tuple[0]} = {Query_Tuple[1]}"
        elif Query_Type == "SELECT":
            query_string = f"SELECT * FROM {Data_Base} WHERE {Query_Tuple[0]} = {Query_Tuple[1]}"
        elif Query_Type == "SEARCH":
            query_string = f"SELECT * FROM {Data_Base} WHERE {Query_Tuple[0]} = {Query_Tuple[1]}'"
        elif Query_Type == "TABLE":
            # Assume Query_Tuple is a tuple of attribute names and data types
            attributes = ", ".join(f"{attr} {data_type}" for attr, data_type in Query_Tuple)
            query_string = f"CREATE TABLE IF NOT EXISTS {Data_Base} ({attributes})"
        elif Query_Type == "CREATE":
            # Assume Query_Tuple is a tuple of attribute names and data types
            attributes = ", ".join(f"{attr} {data_type}" for attr, data_type in Query_Tuple)
            query_string = f"CREATE TABLE IF NOT EXISTS {Data_Base} ({attributes})"
        else:
            # Invalid query type, raise an exception
            raise ValueError(f"Unsupported query type: {Query_Type}")

        # Execute the query
        self.cur.execute(query_string)

        # Return the constructed query string
        return query_string


# TESTING
# Create database and tables for testing
db = Database('climate_data.db')
db.create_tables()

# Insert data
db.insert_co2(co2_dict)
db.insert_sealevel(sealevel_dict)

# Commit changes
db.commit()
print('\nData imported to database!')

# Test cases for the updated QueryBuilder method within the Database class
def test_query_builder():
    # Test INSERT query
    insert_query = db.QueryBuilder("co2", "INSERT", (2023, 9, 408.99, 409.21, 408.65, 408.82, 30))
    print("INSERT Query:", insert_query)

    # Test DELETE query
    delete_query = db.QueryBuilder("co2", "DELETE", ("year", 2000))
    print("DELETE Query:", delete_query)

    # Test SELECT query
    select_query = db.QueryBuilder("co2", "SELECT", ("year", 2003))
    print("SELECT Query:", select_query)

    # Test CREATE query
    table_attributes = (
        ("id", "INTEGER PRIMARY KEY"),
        ("name", "TEXT NOT NULL"),
        ("age", "INTEGER")
    )
    create_query = db.QueryBuilder("my_table", "CREATE", table_attributes)
    print("CREATE Query:", create_query)

    # To ensure that this code works, this CREATE query should have resulted in the climate_data.db file containing a third table alongside co2 and sealevel, called my_table.

    db.commit()

# Run the test cases
test_query_builder()



