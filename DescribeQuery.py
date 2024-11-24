import re
from flask import Flask, jsonify, request
import pandas as pd
from DatabaseSetup import connect_mysql, connect_mongo, get_columns_from_mysql, get_fields_from_mongodb, get_mysql_table_names, get_mongo_collection_names

def handle_describe_query(db_type, table_name, user_input):
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