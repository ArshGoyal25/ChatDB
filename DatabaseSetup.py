# database_setup.py
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
from pymongo import MongoClient
import datetime
import random

# Database configuration - Replace with actual credentials
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
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


def get_mysql_table_names(engine):
    query = "SHOW TABLES"
    df = pd.read_sql(query, engine)
    return df.iloc[:, 0].tolist() # get fisrt col which contains tbl names

def get_mongo_collection_names(db):
    print("temp")
    return db.list_collection_names()

def get_columns_from_mysql(engine, table_name):
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}'"
    df = pd.read_sql(query, engine)
    return df['COLUMN_NAME'].tolist()

def get_fields_from_mongodb(db, collection_name):
    collection = db[collection_name]
    document = collection.find_one()

    if document is not None:
        return list(document.keys())
    return []

def get_random_aggregation_function():
    """Select a random aggregation function."""
    aggregation_functions = ['AVG', 'SUM', 'COUNT', 'MAX', 'MIN']
    return random.choice(aggregation_functions)

def get_random_column(columns):
    """Select a random column from a list of columns."""
    return random.choice(columns)

def generate_select_query(table_name, columns):
    return f"SELECT * FROM {table_name} LIMIT 5"


def generate_group_by_query(db_type, table_name, columns, fields):
    """Generate a GROUP BY query with dynamic aggregation."""
    aggregation_function = get_random_aggregation_function()
    
    # For MySQL
    if db_type == 'mysql':
        column_for_aggregation = get_random_column(columns)
        group_by_column = get_random_column(columns)
        return f"SELECT {group_by_column}, {aggregation_function}({column_for_aggregation}) FROM {table_name} GROUP BY {group_by_column}"
    
    # For MongoDB
    elif db_type == 'nosql':
        group_by_column = get_random_column(fields)
        aggregation_field = get_random_column(fields)
        
        if aggregation_function == "AVG":
            return f"db.{table_name}.aggregate([{{ $group: {{ _id: '${group_by_column}', avg: {{ $avg: '${aggregation_field}' }} }} }}])"
        elif aggregation_function == "SUM":
            return f"db.{table_name}.aggregate([{{ $group: {{ _id: '${group_by_column}', sum: {{ $sum: '${aggregation_field}' }} }} }}])"
        elif aggregation_function == "COUNT":
            return f"db.{table_name}.aggregate([{{ $group: {{ _id: '${group_by_column}', count: {{ $sum: 1 }} }} }}])"
        elif aggregation_function == "MAX":
            return f"db.{table_name}.aggregate([{{ $group: {{ _id: '${group_by_column}', max: {{ $max: '${aggregation_field}' }} }} }}])"
        elif aggregation_function == "MIN":
            return f"db.{table_name}.aggregate([{{ $group: {{ _id: '${group_by_column}', min: {{ $min: '${aggregation_field}' }} }} }}])"
    return None

def generate_example_queries(table_name, user_input, db_type = 'mysql'):
    print(table_name)
    if db_type == 'mysql':
        engine = connect_mysql()
        columns = get_columns_from_mysql(engine, table_name)
    elif db_type == 'nosql':
        db = connect_mongo()
        fields = get_fields_from_mongodb(db, table_name)

    aggregation_functions = ['AVG', 'SUM', 'COUNT', 'MAX', 'MIN']
    queries = []

    if "group by" in user_input.lower():
        # Aggregation query with GROUP BY
        if db_type == 'mysql':
            aggregation_function = random.choice(aggregation_functions) 
            column_for_aggregation = random.choice(columns)  # Select a column for aggregation
            group_by_column = random.choice(columns)  # Select a column for GROUP BY
            query = f"SELECT {group_by_column}, {aggregation_function}({column_for_aggregation}) FROM {table_name} GROUP BY {group_by_column}"
            queries.append(query)

        elif db_type == 'nosql':
            aggregation_function = random.choice(aggregation_functions)  # Select a random aggregation function
            # Dynamically select the fields for aggregation and grouping
            group_by_column = random.choice(fields)  # Select a column for GROUP BY
            aggregation_field = random.choice(fields)  # Select a field for aggregation
            
            # Build MongoDB aggregation query based on the selected aggregation function
            if aggregation_function == "AVG":
                query = f"db.{table_name}.aggregate([{{ '$group': {{ '_id': '${group_by_column}', 'avg': {{ '$avg': '${aggregation_field}' }} }} }}])"
            elif aggregation_function == "SUM":
                query = f"db.{table_name}.aggregate([{{ '$group': {{ '_id': '${group_by_column}', 'sum': {{ '$sum': '${aggregation_field}' }} }} }}])"
            elif aggregation_function == "COUNT":
                query = f"db.{table_name}.aggregate([{{ '$group': {{ '_id': '${group_by_column}', 'count': {{ '$sum': 1 }} }} }}])"
            elif aggregation_function == "MAX":
                query = f"db.{table_name}.aggregate([{{ '$group': {{ '_id': '${group_by_column}', 'max': {{ '$max': '${aggregation_field}' }} }} }}])"
            elif aggregation_function == "MIN":
                query = f"db.{table_name}.aggregate([{{ '$group': {{ '_id': '${group_by_column}', 'min': {{ '$min': '${aggregation_field}' }} }} }}])"
            queries.append(query)

    elif "select" in user_input.lower():
        # SELECT query with specific columns
        if db_type == 'mysql':
            query = f"SELECT {', '.join(random.sample(columns, 2))} FROM {table_name} LIMIT 5"
            queries.append(query)
        elif db_type == 'nosql':
            random_fields = random.sample(fields, 2)  # Select two random fields
            projection = {field: 1 for field in random_fields}
            query = f"db.{table_name}.find().project({projection}).limit(5)"
            queries.append(query)
            print("HETERTERTE")

    if not queries:
        if db_type == 'mysql':
            query1 = f"SELECT * FROM {table_name} LIMIT 5"
            query2= f"SELECT {', '.join(random.sample(columns, 2))} FROM {table_name} LIMIT 5"
            query3 = f"SELECT *, {random.choice(aggregation_functions)}({random.choice(columns)}) FROM {table_name} GROUP BY {random.choice(columns)}"
            queries = [query1, query2, query3]
        
        elif db_type=='nosql':
            random_fields = random.sample(fields, 2)  # Select two random fields
            projection = {field: 1 for field in random_fields}
            query1 = f"db.{table_name}.find().limit(5)"
            query2 = f"db.{table_name}.find().project({projection}).limit(5)"
            query3 = f"db.{table_name}.aggregate([{{ '$group': {{ '_id': '$category', 'avg': {{ '$avg': '$price' }} }} }}])" # HAVE TO DYNAMICALLY GENERATE AGGREGATION FUNC TOO!
            queries = [query1, query2, query3]
    return queries

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
    file_path = 'coffee_shop.xlsx'
    df = pd.read_excel(file_path)
    results = import_data(df, to_sql=True, to_nosql=True)
    print("Import results:", results)

if __name__ == "__main__":
    main()
