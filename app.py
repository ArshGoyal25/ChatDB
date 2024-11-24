# app.py

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import random
from DatabaseSetup import connect_mysql, connect_mongo, import_data, get_mongo_collection_names, get_mysql_table_names, generate_example_queries
from QueryDetection import handle_natural_language_query
from DescribeQuery import handle_describe_query
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
        
        elif any(word in user_input for word in describe_synonyms):
            return handle_describe_query(db_type, table_name, user_input)
        
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
