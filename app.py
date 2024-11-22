# app.py

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import random
from DatabaseSetup import connect_mysql, connect_mongo, import_data, get_mongo_collection_names, get_mysql_table_names, generate_example_queries
import re
import ast
from bson import ObjectId

app = Flask(__name__)
CORS(app)



# API endpoint to insert data into either SQL or MongoDB
@app.route("/api/insert", methods=["POST", "OPTIONS"])
def insert_data():
    if request.method == 'OPTIONS':
        # Handle preflight request
        return '', 200  # No content but successful status
    print(request)
    print(request.files)
    print(request.form)

    if 'file' not in request.files or 'target_db' not in request.form:
        return jsonify({"error": "CSV file and database type are required"}), 400
    
    try:
        # print("HEREE")
        file = request.files['file']
        db_type = request.form['target_db']
        table_name=request.form.get('table_name')

        if table_name is None:
            print(file.filename[len(file.filename)-4: len(file.filename)])
            if file.filename[len(file.filename)-4: len(file.filename)]=="xlsx":
                file_name = file.filename[0: len(file.filename)-5]
            else:
                file_name = file.filename[0: len(file.filename)-4]
            table_name = file_name.replace(" ", "_")
            print(table_name)

        try:
            excel_data = pd.ExcelFile(file)
            # print(excel_data)
            df = pd.read_excel(excel_data)
            # print(df.head())
        except Exception as e:
            return jsonify({"error": f"Error reading Excel file: {str(e)}"}), 400

        to_sql = db_type.lower() == "sql"
        to_nosql = db_type.lower() == "nosql"
        result = import_data(df, to_sql=to_sql, to_nosql=to_nosql, table_name=table_name)

        return jsonify(result), 200
        # return jsonify("success"), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

def get_table_columns(engine, table_name):
    try:
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        df = pd.read_sql(query, engine)
        result = df.to_dict(orient="records")
        for row in result:
            print(row)
        columns = [row['COLUMN_NAME'] for row in result]
        return columns
    except Exception as e:
        raise ValueError(f"Error fetching columns for table {table_name}: {e}")

# def detect_natural_language_query(user_input):
#     patterns = [
#         r"total (?P<A>\w+) by (?P<B>\w+)",
#         r"find (?P<A>\w+) grouped by (?P<B>\w+)",
#         r"highest (?P<A>\w+) by (?P<B>\w+)",
#         r"average (?P<A>\w+) by (?P<B>\w+)",
#         r"minimum (?P<A>\w+) by (?P<B>\w+)",
#         r"maximum (?P<A>\w+) by (?P<B>\w+)"
#     ]
#     where_pattern = r"where (?P<condition>.+)"
#     for pattern in patterns:
#         match = re.search(pattern, user_input, re.IGNORECASE)
#         if match:
#             condition_match = re.search(where_pattern, user_input, re.IGNORECASE)
#             condition = condition_match.group('condition') if condition_match else None
#             return match.groupdict(), condition
#     return None, None

# def handle_natural_language_query(db_type, table_name, user_input):
#     matched_query, condition = detect_natural_language_query(user_input)
#     if not matched_query:
#         return jsonify({"error": "Could not detect a valid query pattern"}), 400

#     if not table_name:
#         return jsonify({"error": "Natural Language Query Detected. For this kind of query, a table name is required"}), 400

#     aggregate_methods = {
#         "total": "SUM",
#         "highest": "MAX",
#         "average": "AVG",
#         "minimum": "MIN",
#         "maximum": "MAX"
#     }
#     aggregate_function = None
#     for key, func in aggregate_methods.items():
#         if key in user_input.lower():
#             aggregate_function = func
#             break

#     if not aggregate_function:
#         return jsonify({"error": "Could not detect a valid aggregate function"}), 400

#     if db_type == 'mysql':
#         return handle_mysql_query(table_name, matched_query, aggregate_function, condition)
#     elif db_type == 'mongodb':
#         return handle_mongodb_query(table_name, matched_query, aggregate_function, condition)
#     else:
#         return jsonify({"error": f"Unsupported database type: {db_type}"}), 400

# def handle_mysql_query(table_name, matched_query, aggregate_function, condition):
#     engine = connect_mysql()
#     table_columns = get_table_columns(engine, table_name)
#     if matched_query['A'] not in table_columns or matched_query['B'] not in table_columns:
#         return jsonify({"error": f"Invalid columns. '{matched_query['A']}' or '{matched_query['B']}' not found in table '{table_name}'"}), 400

#     where_clause = f"WHERE {condition}" if condition else ""
#     query = f"SELECT {matched_query['B']}, {aggregate_function}({matched_query['A']}) AS {matched_query['B']}_{matched_query['A']} FROM {table_name} {where_clause} GROUP BY {matched_query['B']} ORDER BY {matched_query['B']}_{matched_query['A']} DESC"
    
#     return jsonify({"queries": [query]}), 200

# def handle_mongodb_query(table_name, matched_query, aggregate_function, condition):
#     pipeline = []    
#     match_stage = {}
#     if condition:
#         match_stage["$match"] = {key: value for key, value in parse_condition(condition).items()}
    
#     group_stage = {
#         "$group": {
#             "_id": f"${matched_query['B']}",
#             f"{matched_query['B']}_{matched_query['A']}": {f"${aggregate_function}": f"${matched_query['A']}"}
#         }
#     }
    
#     sort_stage = {
#         "$sort": {f"{matched_query['B']}_{matched_query['A']}": -1}  # descending order
#     }
    
#     pipeline.append(match_stage)
#     pipeline.append(group_stage)
#     pipeline.append(sort_stage)
    
#     return jsonify({"queries": pipeline}), 200

# def parse_condition(condition):
#     condition_dict = {}
#     conditions = condition.split('and')
#     for cond in conditions:
#         key, value = cond.split('=')
#         key = key.strip()
#         value = value.strip().strip('"')
#         condition_dict[key] = value
#     return condition_dict


def detect_natural_language_query(user_input):
    # Patterns for aggregate queries (with GROUP BY)
    patterns = [
        r"total (?P<A>\w+) by (?P<B>\w+)",
        r"find (?P<A>\w+) grouped by (?P<B>\w+)",
        r"highest (?P<A>\w+) by (?P<B>\w+)",
        r"average (?P<A>\w+) by (?P<B>\w+)",
        r"minimum (?P<A>\w+) by (?P<B>\w+)",
        r"maximum (?P<A>\w+) by (?P<B>\w+)"
    ]
    
    # Pattern for simple queries with WHERE clause
    where_pattern = r"where (?P<condition>.+)"
    
    # Check for aggregate queries
    for pattern in patterns:
        match = re.search(pattern, user_input, re.IGNORECASE)
        if match:
            condition_match = re.search(where_pattern, user_input, re.IGNORECASE)
            condition = condition_match.group('condition') if condition_match else None
            return match.groupdict(), condition, "aggregate"
    
    # Check for simple queries with WHERE clause
    match = re.search(where_pattern, user_input, re.IGNORECASE)
    if match:
        condition = match.group('condition')
        return match.groupdict(), condition, "simple"
    
    return None, None, None

def handle_natural_language_query(db_type, table_name, user_input):
    
    matched_query, condition, query_type = detect_natural_language_query(user_input)
    
    if not matched_query:
        return jsonify({"error": "Could not detect a valid query pattern"}), 400

    if not table_name:
        return jsonify({"error": "A table name is required for this kind of query"}), 400

    if db_type == 'mysql':
        return handle_mysql_query(table_name, matched_query, condition, query_type, user_input)
    elif db_type == 'mongodb':
        return handle_mongodb_query(table_name, matched_query, condition, query_type)
    else:
        return jsonify({"error": f"Unsupported database type: {db_type}"}), 400

def handle_mysql_query(table_name, matched_query, condition, query_type, user_input):
    engine = connect_mysql()
    table_columns = get_table_columns(engine, table_name)

    if matched_query['A'] not in table_columns or matched_query['B'] not in table_columns:
        return jsonify({"error": f"Invalid columns. '{matched_query['A']}' or '{matched_query['B']}' not found in table '{table_name}'"}), 400

    if condition:
        condition_dict = parse_condition(condition, table_columns)
        if not condition_dict:
            return jsonify({"error": "Invalid condition or column name in the WHERE clause"}), 400
        where_clause = f"WHERE {format_condition(condition_dict)}"
    else:
        where_clause = ""

    if query_type == "aggregate":
        aggregate_methods = {
            "total": "SUM",
            "highest": "MAX",
            "average": "AVG",
            "minimum": "MIN",
            "maximum": "MAX"
        }
        aggregate_function = None
        for key, func in aggregate_methods.items():
            if key in user_input.lower():
                aggregate_function = func
                break

        if not aggregate_function:
            return jsonify({"error": "Could not detect a valid aggregate function"}), 400
        
        query = f"SELECT {matched_query['B']}, {aggregate_function}({matched_query['A']}) AS {matched_query['B']}_{matched_query['A']} FROM {table_name} {where_clause} GROUP BY {matched_query['B']} ORDER BY {matched_query['B']}_{matched_query['A']} DESC"
    else:
        # Simple SELECT query with WHERE clause
        query = f"SELECT {matched_query['A']} FROM {table_name} {where_clause}"

    return jsonify({"queries": [query]}), 200

def handle_mongodb_query(table_name, matched_query, condition, query_type):
    # MongoDB-specific code
    engine = connect_mongo()
    table_columns = get_mongo_collection_columns(engine, table_name)
    
    if matched_query['A'] not in table_columns:
        return jsonify({"error": f"Invalid column '{matched_query['A']}' not found in collection '{table_name}'"}), 400

    pipeline = []
    
    if condition:
        condition_dict = parse_condition(condition, table_columns)
        if not condition_dict:
            return jsonify({"error": "Invalid condition or column name in the WHERE clause"}), 400
        pipeline.append({"$match": condition_dict})

    if query_type == "aggregate":
        # Aggregate query (with GROUP BY)
        group_stage = {
            "$group": {
                "_id": f"${matched_query['B']}",
                f"{matched_query['B']}_{matched_query['A']}": {f"${aggregate_function}": f"${matched_query['A']}"}
            }
        }
        sort_stage = {
            "$sort": {f"{matched_query['B']}_{matched_query['A']}": -1}  # descending order
        }
        
        pipeline.append(group_stage)
        pipeline.append(sort_stage)
    else:
        # Simple SELECT query with WHERE clause
        pipeline.append({"$project": {matched_query['A']: 1}})

    return jsonify({"queries": pipeline}), 200

def parse_condition(condition, valid_columns):
    condition_dict = {}
    condition = condition.strip().lower()

    # Handle phrases like "is less than", "is greater than"
    operators = {
        "is equal to": "=",
        "is less than": "<",
        "is greater than": ">",
        "is not equal to": "!=",
        "is less than or equal to": "<=",
        "is greater than or equal to": ">="
    }

    # Replace "is less than" with "<", etc.
    for phrase, operator in operators.items():
        if phrase in condition:
            condition = condition.replace(phrase, operator)

    # Now check for the operator
    for operator in operators.values():
        if operator in condition:
            left, right = condition.split(operator)
            left = left.strip()
            right = right.strip()
            if left in valid_columns:
                # Handling string values by stripping quotes (e.g., "New York" -> New York)
                if right.startswith('"') and right.endswith('"'):
                    right = right[1:-1]
                condition_dict[left] = format_value(right, operator)
            else:
                return None  # Invalid column name
    return condition_dict

def format_condition(condition_dict):
    formatted_condition = []
    for column, value in condition_dict.items():
        if isinstance(value, str):
            formatted_condition.append(f"{column} = '{value}'")
        else:
            formatted_condition.append(f"{column} {value[0]} {value[1]}")
    return " AND ".join(formatted_condition)

def format_value(value, operator):
    if operator in ['=', '!=']:
        return f"{value}"  # String values need quotes
    return value  # Numeric values are returned as is


def get_mongo_collection_columns(db, table_name):
    # Get column names (keys) from the first document in the specified collection
    collection = db[table_name]
    
    # Fetch the first document from the collection
    first_document = collection.find_one()
    
    if not first_document:
        return []  # If no documents are found, return an empty list
    
    # Return the list of keys (column names) from the first document
    return list(first_document.keys())


# API endpoint to generate example queries based on user input
@app.route("/api/generate_query", methods=["POST"])
def generate_query():
    try:
        data = request.get_json()
        table_name = data.get("table_name")
        user_input = data.get("user_input")
        db_type = data.get("db_type", "mysql").lower() #defaulting to mysql if no dbtype given by user
        print(table_name, user_input, db_type)

        if not user_input:
            return jsonify({"error": "User input for query example is required"}), 400
        
        example_synonyms = ["example", "sample", "instance"]
        if any(word in user_input.lower() for word in example_synonyms):
            if not table_name:
                if db_type == "mysql":
                    engine = connect_mysql()
                    table_names = get_mysql_table_names(engine)
                    table_name = random.choice(table_names) #randomly generating table name

                elif db_type == "nosql":
                    db = connect_mongo()
                    collection_names = get_mongo_collection_names(db)
                    print(collection_names)
                    table_name = random.choice(collection_names)  # Select a random collection
            
            queries = generate_example_queries(table_name, user_input, db_type)
            return jsonify({"queries": queries}), 200
        
        else:
            return handle_natural_language_query(db_type, table_name, user_input)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API endpoint for SQL data retrieval
@app.route("/api/sql/query", methods=["GET"])
def get_sql_coffee_sales():
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({"error": "table_name parameter is required"}), 400
    try:
        engine = connect_mysql()
        query = f"SELECT * FROM {table_name} limit 5"
        df = pd.read_sql(query, engine)
        print(df)
        # Convert Timedelta columns to string format
        for col in df.select_dtypes(include=['timedelta']):
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)

        data = df.to_dict(orient="records")
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API endpoint for MongoDB data retrieval
@app.route("/api/mongo/query", methods=["GET"])
def get_mongo_coffee_sales():
    collection_name = request.args.get("collection_name")
    if not collection_name:
        return jsonify({"error": "collection_name parameter is required"}), 400

    try:
        db = connect_mongo()  # Correct usage here
        collection = db[collection_name]
        data = list(collection.find({}, {"_id": 0}).limit(5)) # Exclude '_id' field
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Additional route for SQL querying with filter
@app.route("/api/sql/coffee_sales/filter", methods=["GET"])
def filter_sql_coffee_sales():
    product = request.args.get("product")
    print(product)
    try:
        engine = connect_mysql()
        query = f'SELECT * FROM coffee_sales WHERE product_category = "{product}"'
        
        #print(query)
        df = pd.read_sql(query, engine, params={"product": product})
        # Convert Timedelta columns to string format
        for col in df.select_dtypes(include=['timedelta']):
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)

        data = df.to_dict(orient="records")
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def parse_query(query):
    collection_match = re.search(r"db\.(\w+)", query)
    if not collection_match:
        raise ValueError("Collection name not found in query")
    collection_name = collection_match.group(1)

    # Extract query type (find, aggregate, etc.)
    if '.find(' in query:
        query_type = 'find'
    elif '.aggregate(' in query:
        query_type = 'aggregate'
    else:
        query_type = 'unknown'

    return collection_name, query_type

#Convert MongoDB ObjectId to strings for JSON serialization
def serialize_mongo_docs(docs):
    for doc in docs:
        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])
    return docs


# API endpoint to execute a query on the appropriate database
@app.route("/api/execute_query", methods=["POST"])
def execute_query():
    try:
        # Parse the incoming request
        data = request.get_json()
        query = data.get("query")
        db_type = data.get("db_type", "").lower()

        if not query or not db_type:
            return jsonify({"error": "Both 'query' and 'db_type' are required"}), 400

        # Handle SQL queries
        if db_type == "mysql":
            try:
                engine = connect_mysql()
                df = pd.read_sql(query, engine)
                print(df)
                # Convert Timedelta columns to string format if any
                for col in df.select_dtypes(include=['timedelta']):
                    df[col] = df[col].apply(
                        lambda x: str(x).split(" ")[-1] if pd.notnull(x) else None
                    )


                data = df.to_dict(orient="records")
                return jsonify(data), 200

            except Exception as e:
                return jsonify({"error": f"SQL execution error: {str(e)}"}), 500

        # Handle NoSQL queries (MongoDB)
        elif db_type == "nosql":
            try:
                db = connect_mongo()
                collection_str, query_type = parse_query(query)

                collection = db[collection_str]
                if query_type == "find":
                    results = list(eval(query))
                    serialized_results = serialize_mongo_docs(results)
                    return jsonify(serialized_results), 200

                elif query_type == "aggregate":
                    query_str = query.split('aggregate(')[-1].split('])')[0] + "]"
                    aggregation_query = ast.literal_eval(query_str)
                    results = list(collection.aggregate(aggregation_query))
                    serialized_results = serialize_mongo_docs(results)
                    return jsonify(serialized_results), 200

                else:
                    return jsonify({"error": f"NoSQL execution error"}), 500

            except Exception as e:
                return jsonify({"error": f"NoSQL execution error: {str(e)}"}), 500

        else:
            return jsonify({"error": f"Unsupported database type: {db_type}"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
