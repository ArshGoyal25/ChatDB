# database_setup.py
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
from pymongo import MongoClient
import datetime
import os

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
        #st = f"mysql+mysqlconnector://{MYSQL_USER}:{mysql_password_encoded}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
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
    

def insert_data_to_mysql(engine, df, table_name=None):

    try:
        table_name = table_name or "temp"
        # print(df.head())
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Data inserted into MySQL table '{table_name}'")
        return {"message": f"Data inserted into SQL table '{table_name}' successfully"}
    
    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")
        return {"error": str(e)}


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

def insert_data_to_mongo(db, df, collection_name=None):
    try:
        collection_name = collection_name or COLLECTION_NAME
        df = convert_time_columns(df)
        # print(df.head())
        data = df.to_dict(orient='records')
        data = [{str(k): v for k, v in record.items()} for record in data]

        collection = db[collection_name]
        collection.insert_many(data)
        print(f"Inserted {len(data)} records into {COLLECTION_NAME} collection.")
        return {"message": f"Data inserted into MongoDB collection '{collection_name}' successfully"}
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")
        return {"error": str(e)}


# General import function to load data from a file and insert into both databases
def import_data(df, to_sql=True, to_nosql=True,table_name=None):
    # print("HERE2")
    results = {}
    if to_sql:
        engine = connect_mysql()
        if engine:
            results['sql'] = insert_data_to_mysql(engine, df, table_name)
    
    if to_nosql:
        db = connect_mongo()
        if db is not None:
            print(db)
            results['nosql'] = insert_data_to_mongo(db, df, table_name)

    return results



def main():
    file_path = 'coffee.xlsx'
    results = import_data(file_path, to_sql=True, to_nosql=True)
    print("Import results:", results)

if __name__ == "__main__":
    main()
