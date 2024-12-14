# database_setup.py
import json
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, text
from pymongo import MongoClient
import datetime
import random
import ast
from bson import json_util
from Config import *

# Connect to MySQL
def connect_mysql():
    try:
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
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Data inserted into MySQL table '{table_name}'")
        return {"message": f"Data inserted into SQL table '{table_name}' successfully"}
    
    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")
        return {"error": str(e)}


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

def insert_data_to_mongo(db, data, collection_name=None):
    try:
        collection_name = collection_name or COLLECTION_NAME
        collection = db[collection_name]

        # Check if the input is a list of records or a single record
        if isinstance(data, list):
            collection.insert_many(data)  # Insert multiple records
            print(f"Inserted {len(data)} records into the '{collection_name}' collection.")
        elif isinstance(data, dict):
            collection.insert_one(data)  # Insert a single record
            print(f"Inserted 1 record into the '{collection_name}' collection.")
        else:
            raise ValueError("Data must be a dictionary or a list of dictionaries.")

        return {"message": f"Data inserted into MongoDB collection '{collection_name}' successfully"}
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")
        return {"error": str(e)}


def get_mysql_table_names(engine):
    query = "SHOW TABLES"
    df = pd.read_sql(query, engine)
    return df.iloc[:, 0].tolist()

def get_mongo_collection_names(db):
    return db.list_collection_names()

def get_columns_from_mysql(engine, table_name):
    with engine.connect() as connection:
        query = text(f"DESCRIBE {table_name}")
        result = connection.execute(query)
        columns = {}
        for row in result:
            print(row)
            columns[row[0]] = row[1]
    return columns

def get_fields_from_mongodb(db, collection_name):
    collection = db[collection_name]
    document = collection.find().limit(5)
    document = list(document)
    if document:
        return json.loads(json_util.dumps(document))
    return []


# Retrieve columns and their types from MySQL
def get_columns_and_types_from_mysql(engine, table_name):
    # Get a connection object from the engine
    with engine.connect() as connection:
        query = text(f"DESCRIBE {table_name}")
        result = connection.execute(query)
        columns = {}
        for row in result:
            print(row)
            columns[row[0]] = row[1]
    return columns

# Retrieve field types from a sample document in MongoDB
def get_field_types_from_mongodb(db, table_name):
    sample_document = db[table_name].find_one()
    if not sample_document:
        return {}
    return {key: type(value).__name__ for key, value in sample_document.items()}

def get_sample_document_mongodb(db, table_name):
    sample_document = db[table_name].aggregate([{'$sample': {'size': 1}}]).next()
    if sample_document:
        return sample_document
    return {}

# Retrieve a sample row from MySQL
def get_sample_row_mysql(engine, table_name):
    query = text(f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT 1")
    df = pd.read_sql(query, engine)
    for col in df.select_dtypes(include=['timedelta']):
                    df[col] = df[col].apply(
                        lambda x: str(x).split(" ")[-1] if pd.notnull(x) else None
                    )

    data = df.to_dict(orient="records")
    return data

# General import function to load data from a file and insert into both databases
def import_data(df, to_sql=True, to_nosql=True,table_name=None):
    results = {}
    if to_sql:
        engine = connect_mysql()
        if engine:
            results['sql'] = insert_data_to_mysql(engine, df, table_name)
            print(results)
    
    if to_nosql:
        db = connect_mongo()
        if db is not None:
            print(db)
            results['nosql'] = insert_data_to_mongo(db, df, table_name)

    return results

# def main():
#     file_path = 'coffee_shop.xlsx'
#     df = pd.read_excel(file_path)
#     results = import_data(df, to_sql=True, to_nosql=True)
#     print("Import results:", results)

# if __name__ == "__main__":
#     main()
