# app.py

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import random
from DatabaseSetup import connect_mysql, connect_mongo, import_data, get_mongo_collection_names, get_fields_from_mongodb, get_columns_from_mysql, get_collection_details, get_mysql_table_names, generate_example_queries
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


AGGREGATE_FUNCTIONS = {
    'total': 'SUM',
    'highest': 'MAX',
    'average': 'AVG',
    'minimum': 'MIN',
    'maximum': 'MAX'
}

def detect_natural_language_query(user_input):
    # Patterns for different clauses
    aggregate_patterns = [
        r"total (?P<A>\w+)",
        r"find (?P<A>\w+)",
        r"highest (?P<A>\w+)",
        r"average (?P<A>\w+)",
        r"minimum (?P<A>\w+)",
        r"maximum (?P<A>\w+)"
    ]

    aggregate_group_by_patterns = [
        r"total (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"find (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"highest (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"average (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"minimum (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"maximum (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)"
    ]
    
    where_pattern = r"where (?P<condition>.+?)(?= having| order by| top| limit|$)"
    having_pattern = r"having (?P<condition>.+?)(?= where| order by| top| limit|$)"    
    order_by_pattern = r"(order by|sort by) (?P<columns>.+?)( asc| desc| ascending| descending|$)"
    top_pattern = r"(top\s*\d+|limit\s*\d+)"

    # Initialize result structure
    result = {
        "type": None,
        "columns": None,
        "aggregate_function": None,
        "where_condition": None,
        "having_condition": None,
        "group_by": False,
        "order_by": None,
        "order_direction": "ASC",  # Default order direction
        "top": None,
        "project_columns": []
    }

    # Extract columns at the start of the query before any aggregate function or clauses
    column_pattern = r"^([\w\s,]+?)(?=\s*(total|highest|average|minimum|maximum|where|grouped by|group by|order by|sort by|asc|desc|ascending|descending|top\s*\d+|limit\s*\d+))"    
    column_match = re.search(column_pattern, user_input, re.IGNORECASE)
    
    if column_match:
        for col in column_match.group(1).split(','):
            match_found = False
            for pattern in aggregate_patterns:
                match = re.search(pattern, user_input, re.IGNORECASE)
                if match:
                    match_found = True
            if not match_found:
                result["project_columns"].append(col.strip())


    for pattern in aggregate_group_by_patterns:
        match = re.search(pattern, user_input, re.IGNORECASE)
        if match:
            result["type"] = "aggregate"
            result["columns"] = match.groupdict()
            result["aggregate_function"] = next(
                (AGGREGATE_FUNCTIONS[key] for key in AGGREGATE_FUNCTIONS if key in match.group(0).lower()), None
            )
            result["group_by"] = True
            break

    
    if result["group_by"] == False:
        # Check for aggregate queries and capture columns for aggregation
        for pattern in aggregate_patterns:
            match = re.search(pattern, user_input, re.IGNORECASE)
            if match:
                result["type"] = "aggregate"
                result["columns"] = match.groupdict()
                result["aggregate_function"] = next(
                    (AGGREGATE_FUNCTIONS[key] for key in AGGREGATE_FUNCTIONS if key in match.group(0).lower()), None
                )
                break

    top_match = re.search(top_pattern, user_input, re.IGNORECASE)
    if top_match:
        result["top"] = top_match.group().strip()

    # If no aggregate pattern matched, set type to "simple"
    if not result["type"]:
        result["type"] = "simple"
    
    # Extract WHERE clause (if any)
    where_match = re.search(where_pattern, user_input, re.IGNORECASE)
    if where_match:
        result["where_condition"] = where_match.group("condition").strip()
    
    # Extract HAVING clause (if any)
    having_match = re.search(having_pattern, user_input, re.IGNORECASE)
    if having_match:
        result["having_condition"] = having_match.group("condition").strip()
    
    # Extract ORDER BY clause (if any) with ascending or descending
    order_by_match = re.search(order_by_pattern, user_input, re.IGNORECASE)
    if order_by_match:
        result["order_by"] = order_by_match.group("columns").strip()
        # Check for ASC or DESC or variations like ascending/descending
        if "desc" in user_input.lower() or "descending" in user_input.lower():
            result["order_direction"] = "DESC"
        elif "asc" in user_input.lower() or "ascending" in user_input.lower():
            result["order_direction"] = "ASC"

    return result


def handle_natural_language_query(db_type, table_name, user_input):
    if not table_name:
        return jsonify({"error": "A table name is required for this kind of query"}), 400
    
    result = detect_natural_language_query(user_input)
    for key, value in result.items():
        print(f"{key}: {value}")

    if not result:
        return jsonify({"error": "Could not detect a valid query pattern"}), 400

    if db_type == 'mysql':
        return handle_mysql_query(table_name, result)
    elif db_type == 'mongodb':
        return handle_mongodb_query(table_name, result)
    else:
        return jsonify({"error": f"Unsupported database type: {db_type}"}), 400

def handle_mysql_query(table_name, result):
    engine = connect_mysql()
    table_columns = get_table_columns(engine, table_name)

    project_columns = result["project_columns"]
    for col in project_columns:
        if col not in table_columns:
            return jsonify({"error": f"Invalid columns. '{col}' not found in table '{table_name}'. It Looks like the columns you wish to project are not in the table. Please update them. To understand more about which columns are present in the table you choose, run the command 'show columns from {table_name}'"}), 400

    aggregate_columns = result["columns"]
    if aggregate_columns is not None:
        if 'A' in aggregate_columns.keys():
            if aggregate_columns['A'] not in table_columns:
                return jsonify({"error": f"Invalid columns. '{aggregate_columns['A']}' not found in table '{table_name}'. It looks like the columns you are using to aggregate are not in the table. Please update them. To understand more about which columns are present in the table you choose, run the command 'show columns from {table_name}'"}), 400
        if 'B' in aggregate_columns.keys():
            if aggregate_columns['B'] not in table_columns:
                return jsonify({"error": f"Invalid columns. '{aggregate_columns['B']}' not found in table '{table_name}'. It looks like the columns you are using to aggregate are not in the table. Please update them. To understand more about which columns are present in the table you choose, run the command 'show columns from {table_name}'"}), 400
    
    query_type = result["type"]
    group_by_clause = result["group_by"]
    where_condition = result["where_condition"]
    having_condition = result["having_condition"]
    aggregate_function = result["aggregate_function"]
    order_by_clause = result["order_by"]
    order_direction = result["order_direction"]
    limit_clause = result["top"]

    missing_clauses = []

    if having_condition:
        if group_by_clause is False:
            return jsonify({"error": "HAVING clause cannot be used without GROUP BY clause"}), 400
        
    if group_by_clause:
        if aggregate_function is False:
            return jsonify({"error": "GROUP BY clause cannot be used without aggregate function"}), 400

    if where_condition:
        condition_dict, operator = parse_condition(where_condition, table_columns)
        if not condition_dict:
            return jsonify({"error": "Invalid condition or column name in the WHERE clause"}), 400
        where_clause = f"WHERE {format_condition(condition_dict, operator)}"
    else:
        where_clause = ""
        missing_clauses.append("WHERE clause")

    if having_condition:
        having_dict, operator = parse_condition(having_condition, table_columns)
        if not having_dict:
            return jsonify({"error": "Invalid condition or column name in the HAVING clause"}), 400
        having_clause = f"HAVING {format_condition(having_dict, operator)}"
    else:
        having_clause = ""
        if group_by_clause:
            missing_clauses.append("HAVING clause")

    if order_by_clause:
        if order_by_clause not in table_columns:
            return jsonify({"error": f"Invalid column '{order_by_clause}' for ORDER BY. Please choose a valid column."}), 400
        order_clause = f"ORDER BY {order_by_clause} {order_direction if order_direction else 'ASC'}"
    else:
        order_clause = ""
        missing_clauses.append("ORDER BY clause")

    if not limit_clause:
        limit_clause = "LIMIT 10"
        missing_clauses.append("LIMIT clause")
    
    # Construct Aggregate query
    if query_type == "aggregate":
        if not aggregate_function:
            return jsonify({"error": "Aggregate function not specified."}), 400

        # Build the aggregate query
        if group_by_clause:
            query = f"SELECT {aggregate_columns['B']}, {aggregate_function}({aggregate_columns['A']}) AS {aggregate_columns['B']}_{aggregate_columns['A']} FROM {table_name} {where_clause} GROUP BY {aggregate_columns['B']} {having_clause} {order_clause} {limit_clause}"
        else:
            query = f"SELECT {aggregate_function}({aggregate_columns['A']}) AS {aggregate_columns['A']}_{aggregate_function} FROM {table_name} {where_clause} {having_clause} {order_clause} {limit_clause}"
    
    # Construct Simple SELECT query
    else:
        if not project_columns:
            project_columns = ["*"]
        query = f"SELECT {', '.join(project_columns)} FROM {table_name} {where_clause} {order_clause} {limit_clause}"

    message = "Here is your query:"
    if missing_clauses:
        message += "\n\nIt looks like you may have missed the following clauses. Here’s how you can use them:\n"
        if "WHERE clause" in missing_clauses:
            message += "1. WHERE clause: You can filter results based on certain conditions, e.g., 'WHERE age > 30'.\n"
        if "HAVING clause" in missing_clauses:
            message += "2. HAVING clause: Use this for filtering grouped results, e.g., 'HAVING COUNT(*) > 5'.\n"
        if "ORDER BY clause" in missing_clauses:
            message += "3. ORDER BY clause: You can sort the results, e.g., 'ORDER BY age DESC'.\n"
        if "LIMIT clause" in missing_clauses:
            message += "4. LIMIT clause: You can limit the number of results returned, e.g., 'LIMIT 10'.\n"

    return jsonify({"queries": [query], "message": message}), 200

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
            print(phrase)
            condition = condition.replace(phrase, operator)

    print("condition: ", condition)
    for operator in operators.values():
        print(operator)
        if operator in condition:
            left, right = condition.split(operator)
            left = left.strip()
            right = right.strip()

            if left in valid_columns:
                if right.isdigit():  # Numeric condition
                    condition_dict[left] = int(right)
                else:
                    if right.startswith('"') and right.endswith('"'):  # If quotes are already there, keep them
                        condition_dict[left] = right
                    else:  # If no quotes, add them
                        condition_dict[left] = f'"{right}"'
            else:
                return None
            break
    print(condition_dict, operator)
    return condition_dict, operator

def format_condition(condition_dict, operator):
    print(f"Using operator: {operator}")
    formatted_condition = []
    
    for column, value in condition_dict.items():
        if isinstance(value, (int, float)):
            formatted_condition.append(f"{column} {operator} {value}")
        elif isinstance(value, str):
            if value.startswith('"') and value.endswith('"'):
                formatted_condition.append(f"{column} {operator} {value}")
            else:
                formatted_condition.append(f"{column} {operator} '{value}'")
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

# API to get table names based on the database type (MySQL or NoSQL)
@app.route('/api/get_table_names', methods=['GET'])
def get_table_names():
    db_type = request.args.get('db_type')  # Get db_type from query params
    if db_type == 'mysql':
        try:
            engine = connect_mysql()
            dets = get_mysql_table_names(engine)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    elif db_type == 'nosql':
        try:
            db = connect_mongo()
            dets = get_mongo_collection_names(db)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
    return jsonify({"table_names": dets}), 200

# API endpoint to generate example queries based on user input
@app.route("/api/generate_query", methods=["POST"])
def generate_query():
    try:
        data = request.get_json()
        table_name = data.get("table_name")
        user_input = data.get("user_input").lower()
        db_type = data.get("db_type", "mysql").lower() #defaulting to mysql if no dbtype given by user
        print(table_name, user_input, db_type)

        if not user_input:
            return jsonify({"error": "User input for query example is required"}), 400

        example_synonyms = ["example", "sample", "instance"]
        describe_synonyms = ["describe", "outline"]

        #conditions for example query
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
        
        #conditions for describe <db_name> or describe table <table_name>

        elif any(word in user_input for word in describe_synonyms):
            
            # if table not in user inp, then it means describe db
            if "table" not in user_input:
                db = user_input.split(" ")[1]
                print(db)
                if db_type == "mysql":
                    engine = connect_mysql()
                    dets = get_mysql_table_names(engine)
                    print(dets)

                else:
                    db = connect_mongo()
                    dets = get_mongo_collection_names(db)

                return jsonify({"database_details": dets}), 200
            
            # else if table in user input and user has not given tbl name in prompt above and has neiter given it in the input
            elif not table_name and "table" in user_input and len(user_input.split(" "))<3 :
                return jsonify({"error": "Please enter a table name in the box above"}), 400
            
            else:
                #else give precendence to tble name given in query
                if len(user_input.split(" "))==3:
                    table_name = user_input.split(" ")[2]
                print(table_name)
                if db_type == "mysql":
                    engine = connect_mysql()
                    res = get_columns_from_mysql(engine, table_name)
                    print(res)

                else:
                    engine = connect_mongo()
                    res = get_fields_from_mongodb(engine, table_name)
                return jsonify({"table_details": res}), 200
        
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
