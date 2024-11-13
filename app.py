# app.py

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
from DatabaseSetup import connect_mysql, connect_mongo

app = Flask(__name__)
CORS(app)

# API endpoint for SQL data retrieval
@app.route("/api/sql/coffee_sales", methods=["GET"])
def get_sql_coffee_sales():
    try:
        engine = connect_mysql()
        query = "SELECT * FROM coffee_sales"
        df = pd.read_sql(query, engine)

        # Convert Timedelta columns to string format
        for col in df.select_dtypes(include=['timedelta']):
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)

        data = df.to_dict(orient="records")
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API endpoint for MongoDB data retrieval
@app.route("/api/mongo/coffee_sales", methods=["GET"])
def get_mongo_coffee_sales():
    try:
        db = connect_mongo()  # Correct usage here
        collection = db["sales"]
        data = list(collection.find({}, {"_id": 0}))  # Exclude '_id' field
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
