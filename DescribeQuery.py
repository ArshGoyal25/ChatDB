import re
from flask import Flask, jsonify, request
import pandas as pd
from DatabaseSetup import connect_mysql, connect_mongo, get_columns_from_mysql, get_fields_from_mongodb, get_mysql_table_names, get_mongo_collection_names

def handle_describe_query(db_type, table_name, user_input):
    # print("table" in user_input and db_type=="mysql")
    if "table" in user_input and db_type=="mysql":
        engine = connect_mysql()
        res = get_columns_from_mysql(engine, table_name)
        return jsonify({"table_details": res}), 200

    elif "collection" in user_input and db_type=="nosql":
        engine = connect_mongo()
        res = get_fields_from_mongodb(engine, table_name)
        return jsonify({"table_details": res}), 200
    else:
        return jsonify({"error": "Please give query appropriate to the Target Database selected "}), 400
