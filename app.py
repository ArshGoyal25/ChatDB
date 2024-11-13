# app.py

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
from DatabaseSetup import connect_mysql, connect_mongo, insert_data_to_mysql, insert_data_to_mongo, import_data

app = Flask(__name__)
CORS(app)

# API endpoint to insert data into either SQL or MongoDB
@app.route("/api/insert", methods=["POST"])
def insert_data():

    if 'file' not in request.files or 'target_db' not in request.form:
        return jsonify({"error": "CSV file and database type are required"}), 400
    
    try:
        # print("HEREE")
        file = request.files['file']
        db_type = request.form['target_db']
        table_name=request.form.get('table_name')

        if table_name is None:
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



# API endpoint for SQL data retrieval
@app.route("/api/sql/query", methods=["GET"])
def get_sql_coffee_sales():
    table_name = request.args.get('table_name')
    if not table_name:
        return jsonify({"error": "table_name parameter is required"}), 400
    try:
        engine = connect_mysql()
        query = f"SELECT * FROM {table_name} limit 10"
        df = pd.read_sql(query, engine)

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
        data = list(collection.find({}, {"_id": 0}).limit(20)) # Exclude '_id' field
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

if __name__ == "__main__":
    app.run(debug=True)
