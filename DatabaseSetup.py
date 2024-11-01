# database_setup.py
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
from pymongo import MongoClient
import datetime

# Database configuration - Replace with actual credentials
MYSQL_USER = "root"
MYSQL_PASSWORD = "root@111"
mysql_password_encoded = MYSQL_PASSWORD.replace('@', '%40')
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "coffee_shop"
MONGO_URI = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.3.2"
MONGO_DATABASE = "coffee_shop"
COLLECTION_NAME='sales'

# pd.set_option('display.max_colwidth', None)  # Show full content of columns
# pd.set_option('display.max_rows', None)      # Show all rows for debugging

# Connect to MySQL
def connect_mysql():
    try:
        st = f"mysql+mysqlconnector://{MYSQL_USER}:{mysql_password_encoded}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        # print(st)
        engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{mysql_password_encoded}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        print("Connected to MySQL")
        return engine
    except Exception as e:
        print("MySQL connection error:", e)
        return None

# Connect to MongoDB
def connect_mongo():
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        print("Connected to MongoDB")
        return db
    except Exception as e:
        print("MongoDB connection error:", e)
        return None
    

def import_data_to_mysql(engine, excel_file):
    # df = pd.read_csv(csv_file, encoding='ISO-8859-1')
    # df = pd.read_csv(csv_file, encoding='ISO-8859-1', low_memory=False, on_bad_lines='skip')
    df = pd.read_excel(excel_file, engine='openpyxl')

    print(df.head())
    metadata = MetaData()
    # coffee_sales_table = Table('coffee_sales', metadata,
    #                            Column('id', Integer, primary_key=True, autoincrement=True),
    #                            Column('product', String(50)),
    #                            Column('quantity', Integer),
    #                            Column('price', Float),
    #                            Column('date', String(20))
    #                            )
    
    # # Create table in MySQL
    # metadata.create_all(engine)
     # Insert data into MySQL table, replacing existing data
    df.to_sql(name="coffee_sales_table", con=engine, if_exists='replace', index=False)

    print("Table created in MySQL")

    # Insert data
    df.to_sql('coffee_sales', con=engine, if_exists='replace', index=False)
    print("Data imported into MySQL")

#function coz mongodb does not support given date format
def convert_time_columns(df):
    # Convert any datetime or time objects in the DataFrame to datetime.datetime
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')  # Convert datetime to string
        else:
            # If the column has time data, convert it to datetime with the minimum date
            df[col] = df[col].apply(
                lambda x: datetime.datetime.combine(datetime.date.today(), x) if isinstance(x, datetime.time) else x
            )
    return df

def import_data_to_mongo(db, excel_file):

    df = pd.read_excel(excel_file, engine='openpyxl')
    print(df.head())
    df = convert_time_columns(df)

    data = df.to_dict(orient='records')

    collection = db[COLLECTION_NAME]
    collection.insert_many(data)
    print(f"Inserted {len(data)} records into {COLLECTION_NAME} collection.")


def main():
    mysql_engine = connect_mysql()
    mongo_db = connect_mongo()

    csv_file = 'coffee.xlsx'

    if mysql_engine:
        import_data_to_mysql(mysql_engine, csv_file)

    if mongo_db is not None:
        import_data_to_mongo(mongo_db, csv_file)

if __name__ == "__main__":
    main()
