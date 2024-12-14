import random
import re
from flask import Flask, jsonify, request
import pandas as pd
from DatabaseSetup import connect_mysql, connect_mongo, get_columns_from_mysql, get_fields_from_mongodb, get_numeric_cols_sql, get_numeric_fields_nosql, get_sample_document_mongodb, get_field_types_from_mongodb

AGGREGATE_FUNCTIONS = {
    'total': 'SUM',
    'highest': 'MAX',
    'average': 'AVG',
    'minimum': 'MIN',
    'maximum': 'MAX',
    'find': 'COUNT',
    'lowest': 'MIN',
    'count': 'COUNT',
    'max': 'MAX',
    'min': 'MIN',
    'sum': 'SUM',
    'avg': 'AVG'
}

def get_table_columns(engine, table_name):
    return get_columns_from_mysql(engine, table_name)

def get_mongo_collection_columns(db, table_name):
    return get_fields_from_mongodb(db, table_name)

def parse_condition_sql(condition, valid_columns, agg_col = None, grp_col = None, having_condition = False):
    condition = condition.strip().lower()

    operators = {
        "!=": "!=",
        "<": "<",
        "<=": "<=",
        ">": ">",
        ">=": ">=",
        "=": "=",
        "is greater than equal to": ">=",
        "is less than equal to": "<=",
        "is equal to": "=",
        "is less than": "<",
        "is greater than": ">",
        "is not equal to": "!="
    }

    ops = ("!=", "<=", "<", ">=", ">", "=")

    for phrase, operator in operators.items():
        if phrase in condition:
            condition = condition.replace(phrase, operator)

    conditions_with_operators = re.split(r'(\s+and\s+|\s+or\s+)', condition, flags=re.IGNORECASE)
    
    conditions = []
    operators_in_condition = []
    
    for _, item in enumerate(conditions_with_operators):
        item = item.strip()
        if item.lower() in ['and', 'or']:  # If it's an operator
            operators_in_condition.append(item.lower())  # Store the operator (and/or)
        elif item:  # Non-empty items are conditions
            conditions.append(item)
    # print(conditions)
    # print(operators_in_condition)
    sql_query = ""
    for i, cond in enumerate(conditions):
        for operator in ops:
            if operator in cond:
                left, right = cond.split(operator)
                left = left.strip()
                right = right.strip()

                if(having_condition):
                    vals = left.split(" ")
                    if(len(vals) == 2):
                        agg_func_obtained = None
                        agg_col_obtained = vals[1]
                        if(agg_col_obtained != agg_col and agg_col_obtained != grp_col):
                            return False, "Only the aggregate column or the group by column can be used in having clause"
                        for key in AGGREGATE_FUNCTIONS:
                            if(key == vals[0]):
                                agg_func_obtained = AGGREGATE_FUNCTIONS[key]
                        if(agg_func_obtained == None):
                            return False,"Invalid aggregate function used in having clause"
                        left = f"{agg_func_obtained}({agg_col_obtained})"
                    else:
                        agg_col_obtained = left
                        if(agg_col_obtained == agg_col):
                            return False, "The aggregate column can be used in having clause only with an aggregate function"
                        if(agg_col_obtained != grp_col):
                            return False, "Only the group by column can be used in having clause without an aggregate function"
                else:
                    if left not in valid_columns:
                        return False, "Invalid column {left} used in condition statement"

                if right.isdigit():  # Numeric condition
                    right_value = right
                else:
                    if right.startswith('"') and right.endswith('"'):  # If quotes are already there, keep them
                        right_value = right
                    else:
                        right_value = f'"{right}"'
                if i > 0:
                    sql_query += f" {operators_in_condition[i-1].upper()} "  # Add operator (AND/OR)
                sql_query += f"{left} {operator} {right_value}"
                break

    return True, sql_query

def handle_mysql_query(table_name, result):
    engine = connect_mysql()
    table_columns = get_table_columns(engine, table_name)
    random_col1 = random.choice(list(table_columns.keys()))
    random_col2 = random.choice(list(table_columns.keys()))
    random_col3 = random.choice(list(table_columns.keys()))
    numeric_cols = get_numeric_cols_sql(table_columns)

    errors = result["errors"]
    suggestions = result["suggestions"]
    if errors:
        return jsonify({"error": errors[0], "suggestions": suggestions}), 400
    
    project_columns = result["project_columns"]
    for col in project_columns:
        if col == '*' or col == "all":
            project_columns = ['*']
            continue
        if col not in table_columns:
            return jsonify({"error": f"Invalid columns. '{col}' not found in table '{table_name}'. It Looks like the columns you wish to project are not in the table. Please update them. To understand more about which columns are present in the table you choose, run the command 'describe table' or 'describe collection' depending on the database type"}), 400

    aggregate_columns = result["columns"]
    if aggregate_columns is not None:
        suggestions = [f"Suggested Column Names: {random_col1}, {random_col2}, {random_col3}"]
        if 'A' in aggregate_columns.keys():
            if aggregate_columns['A'] not in table_columns:
                error = f"Invalid columns. '{aggregate_columns['A']}' not found in table '{table_name}'. It looks like the columns you are using to aggregate are not in the table. Please update them. To understand more about which columns are present in the table you choose, run the command 'describe table' or 'describe collection' depending on the database type"
                return jsonify({"error": error, "suggestions": suggestions}), 400
        if 'B' in aggregate_columns.keys():
            if aggregate_columns['B'] not in table_columns:
                error = f"Invalid columns. '{aggregate_columns['B']}' not found in table '{table_name}'. It looks like the columns you are using to aggregate are not in the table. Please update them. To understand more about which columns are present in the table you choose, run the command 'describe table' or 'describe collection' depending on the database type"
                return jsonify({"error": error, "suggestions": suggestions}), 400
    
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
        status, where_query = parse_condition_sql(where_condition, table_columns)
        if not status:
            return jsonify({"error": where_query}), 400
        where_clause = f"WHERE {where_query}"
    else:
        where_clause = ""
        missing_clauses.append("WHERE clause")

    if having_condition:
        status, having_query = parse_condition_sql(having_condition, table_columns, aggregate_columns['A'], aggregate_columns['B'], True)
        if not status:
            return jsonify({"error": having_query}), 400
        having_clause = f"HAVING {having_query}"
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
            query = f"SELECT {aggregate_columns['B']}, {aggregate_function}({aggregate_columns['A']}) AS {aggregate_function.lower()}_{aggregate_columns['A']} FROM {table_name} {where_clause} GROUP BY {aggregate_columns['B']} {having_clause} {order_clause} {limit_clause}"
        else:
            query = f"SELECT {aggregate_function}({aggregate_columns['A']}) AS {aggregate_columns['A']}_{aggregate_function} FROM {table_name} {where_clause} {having_clause} {order_clause} {limit_clause}"
    
    # Construct Simple SELECT query
    else:
        if not project_columns:
            project_columns = ["*"]
        query = f"SELECT {', '.join(project_columns)} FROM {table_name} {where_clause} {order_clause} {limit_clause}"

    message = "Here is your query:"
    suggestions = []
    if missing_clauses:
        message += "\n\nIt looks like you may have missed the following clauses. Here’s how you can use them:"
        if "WHERE clause" in missing_clauses:
            suggestions.append(f"You can filter results based on certain conditions, try - 'where {numeric_cols[0]} is less than {random.randint(1, 10)}' '")            
        if "HAVING clause" in missing_clauses:
            suggestions.append(f"You can filter grouped results, try - 'having COUNT({numeric_cols[0]}) is greater than {random.randint(1, 10)}'")
        if "ORDER BY clause" in missing_clauses:
            suggestions.append(f"You can sort the results, try - 'order by {random_col1}'")
        if "LIMIT clause" in missing_clauses:
            suggestions.append(f"You can limit the number of results returned, try - 'limit {random.randint(1, 10)}'")

    return jsonify({"queries": [query],"suggestions": suggestions, "message": message}), 200

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def parse_condition_nosql(condition, valid_columns, agg_col = None, grp_col = None, having_condition = False):
        
    operators = {
       ">=": "$gte",
       "<=": "$lte",
        "==": "$eq",
        "!=": "$ne",
        "<>": "$ne",
        "<": "$lt",
        ">": "$gt",
         "=": "$eq",
        "is greater than equal to": "$gte",
        "is equal to": "$eq",
        "is not equal to": "$ne",
        "is less than equal to": "$lte",
        "is less than": "$lt",
        "is greater than": "$gt"
        
    }

    # Split conditions by logical operators (e.g., AND, OR) while keeping the operators
    conditions = re.split(r'\s+and\s+|\s+or\s+', condition, flags=re.IGNORECASE)
    # print(conditions)
    mongo_query = []

    for cond in conditions:
        condition_dict = {}
        for operator, mongo_op in operators.items():
            # print(operator)
            if operator.lower() in cond.lower():
                left, right = cond.split(operator, 1)
                left = left.strip()
                right = right.strip()
                # print(operator, left, right)
                if(having_condition):
                    vals = left.split(" ")
                    if(len(vals) == 2):
                        agg_func_obtained = None
                        agg_col_obtained = vals[1]
                        if(agg_col_obtained != agg_col and agg_col_obtained != grp_col):
                            return False, "Only the aggregate column or the group by column can be used in having clause"
                        for key in AGGREGATE_FUNCTIONS:
                            if(key == vals[0]):
                                agg_func_obtained = AGGREGATE_FUNCTIONS[key]
                        if(agg_func_obtained == None):
                            return False,"Invalid aggregate function used in having clause"
                        left = f"{agg_func_obtained.lower()}_{agg_col_obtained}"
                    else:
                        agg_col_obtained = left
                        if(agg_col_obtained == agg_col):
                            return False, "The aggregate column can be used in having clause only with an aggregate function"
                        if(agg_col_obtained != grp_col):
                            return False, "Only the group by column can be used in having clause without an aggregate function"

                # Handle right side values based on type (string, number, float)
                if right.startswith('"') and right.endswith('"'):
                    value = right[1:-1]
                    condition_dict = {left: {mongo_op: value}}
                elif right.isdigit():
                    value = int(right)  # Convert to integer
                    condition_dict = {left: {mongo_op: value}}
                elif is_float(right):
                    value = float(right)  # Convert to float
                    condition_dict = {left: {mongo_op: value}}
                else:
                    # Case-insensitive string matching using $regex
                    value = {"$regex": f"{right}", "$options": "i"}
                    condition_dict = {left: value}

                mongo_query.append(condition_dict)
                break
        else:
            return False, "Invalid Operators Used in condition statement"

    if len(mongo_query) > 1:
        if re.search(r'\bor\b', condition):
            return True, {"$or": mongo_query}
        else:
            return True, {"$and": mongo_query}

    return True, mongo_query[0]

def handle_mongo_query(collection_name, result):
    engine = connect_mongo()
    table_columns = get_sample_document_mongodb(engine, collection_name)
    table_columns = list(table_columns.keys())
    fields = get_field_types_from_mongodb(engine, collection_name)
    numeric_cols = get_numeric_fields_nosql(fields)

    random_col1 = random.choice(table_columns)
    random_col2 = random.choice(table_columns)
    random_col3 = random.choice(table_columns)
    numeric_cols1 = random.choice(numeric_cols)
    numeric_cols2 = random.choice(numeric_cols)

    errors = result["errors"]
    suggestions = result["suggestions"]
    if errors:
        return jsonify({"error": errors[0], "suggestions": suggestions}), 400
    
    project_columns = result["project_columns"]
    aggregate_columns = result["columns"]
    query_type = result["type"]
    group_by_column = result["group_by"]
    where_condition = result["where_condition"]
    having_condition = result["having_condition"]
    aggregate_function = result["aggregate_function"]
    order_by_column = result["order_by"]
    order_direction = result["order_direction"]
    limit_clause = result["top"]

    # Initialize missing clauses message
    missing_clauses = []

    # Construct WHERE clause
    if where_condition:
        status, condition_dict = parse_condition_nosql(where_condition, table_columns)
        if not status:
            return jsonify({"error": condition_dict}), 400
        match_stage = {"$match": condition_dict}
    else:
        match_stage = {}
        missing_clauses.append("WHERE clause")

    # Construct HAVING clause
    if having_condition:
        status, having_query = parse_condition_nosql(having_condition, table_columns, aggregate_columns['A'], aggregate_columns['B'], True)
        if not status:
            return jsonify({"error": having_query}), 400
        having_stage = {"$match": having_query}
    else:
        having_stage = {}
        if group_by_column:
            missing_clauses.append("HAVING clause")

    # Construct ORDER BY clause
    if order_by_column:
        sort_stage = {"$sort": {order_by_column: 1 if order_direction == "ASC" else -1}}
    else:
        sort_stage = {}
        missing_clauses.append("ORDER BY clause")

    # Construct LIMIT clause
    if limit_clause:
        limit_clause = ''.join([char if char.isdigit() else '' for char in limit_clause]).strip()
        try:
            limit_clause = int(limit_clause)
        except ValueError:
            limit_clause = 10
    else:
        limit_clause = 10  # Default limit if not specified
        missing_clauses.append("LIMIT clause")

    # Build aggregate query
    if query_type == "aggregate":
        if not aggregate_function:
            return jsonify({"error": "Aggregate function not specified."}), 400
        pipeline = []
        if match_stage:
            pipeline.append(match_stage)
        
        if group_by_column:
            pipeline.append(
                {"$group": {
                    "_id": f"${aggregate_columns['B']}",
                    f"{aggregate_function.lower()}_{aggregate_columns['A']}": {f"${aggregate_function.lower()}": f"${aggregate_columns['A']}"}
                }}
            )
        else:
            pipeline.append(
                {"$group": {
                    "_id": "null",
                    f"{aggregate_function.lower()}_{aggregate_columns['A']}": {f"${aggregate_function.lower()}": f"${aggregate_columns['A']}"}
                }}
            )

        if having_stage:
            pipeline.append(having_stage)
        
        if sort_stage:
            pipeline.append(sort_stage)
        
        if limit_clause:
            pipeline.append({"$limit": limit_clause})
        query = f"db.{collection_name}.aggregate({pipeline})"

    else:  # Build Simple SELECT query
        if not project_columns or project_columns == ["*"] or project_columns == ["all"]:
            project_columns = {"_id": 0}  # Exclude `_id` by default for MongoDB projections
        else:
            project_columns = {col: 1 for col in project_columns}
        query_filter = match_stage.get("$match", {})
        if sort_stage:
            query = f"db.{collection_name}.find({query_filter}, {project_columns}).sort({sort_stage['$sort']}).limit({limit_clause or 10})"
        else:
            query = f"db.{collection_name}.find({query_filter}, {project_columns}).limit({limit_clause or 10})"
        print(query)

    # Build helpful message for missing clauses
    message = "Here is your query pipeline:"
    suggestions = []
    if missing_clauses:
        message += "\n\nIt looks like you may have missed the following clauses. Here’s how you can use them:"
        if "WHERE clause" in missing_clauses:
            suggestions.append(f"You can match results based on certain conditions, try - 'where {numeric_cols[0]} is less than {random.randint(1, 10)}' '")            
        if "HAVING clause" in missing_clauses:
            suggestions.append(f"You can match grouped results, try - 'having COUNT({numeric_cols[0]}) is greater than {random.randint(1, 10)}'")
        if "ORDER BY clause" in missing_clauses:
            suggestions.append(f"You can sort the results, try - 'order by {random_col1}'")
        if "LIMIT clause" in missing_clauses:
            suggestions.append(f"You can limit the number of results returned, try - 'limit {random.randint(1, 10)}'")
    print("Pipeline:", query)
    return jsonify({"queries": [query],"suggestions": suggestions,"message": message}), 200

def detect_natural_language_query(user_input, table_name=None, db_type = None):
    
    if(db_type == "mysql"):
        engine = connect_mysql()  
        table_columns = get_table_columns(engine, table_name)
        numeric_cols = get_numeric_cols_sql(table_columns)
        table_columns = list(table_columns.keys())
    else:
        engine = connect_mongo()
        table_columns = get_sample_document_mongodb(engine, table_name)
        table_columns = list(table_columns.keys())
        fields = get_field_types_from_mongodb(engine, table_name)
        numeric_cols = get_numeric_fields_nosql(fields)

    random_key1 = random.choice(list(AGGREGATE_FUNCTIONS.keys()))
    random_key2 = random.choice(list(AGGREGATE_FUNCTIONS.keys()))
    random_col1 = random.choice(table_columns)
    random_col2 = random.choice(table_columns)
    random_col3 = random.choice(table_columns)
    random_numeric_col1 = random.choice(numeric_cols)
    random_numeric_col2 = random.choice(numeric_cols)
    while(random_numeric_col1 == random_col1):
        random_numeric_col1 = random.choice(numeric_cols)

    while(random_numeric_col2 == random_col2):
        random_numeric_col2 = random.choice(numeric_cols)

    while(random_col1 == random_col2):
        random_col2 = random.choice(table_columns)
    while(random_col2 == random_col3):
        random_col3 = random.choice(table_columns)
    conditions = ["is equal to", "is not equal to", "is less than", "is less than equal to", "is greater than", "is greater than equal to"]
    random_cond = random.choice(conditions)

    user_input = user_input.strip()
    select_present = False
    if user_input.lower().startswith("select"):
        user_input = user_input[6:].strip()
        select_present = True
        print("jdfnjdnf")

    # Patterns for different clauses
    aggregate_patterns = [
        r"(total|sum) (?P<A>\w+)",
        r"(find|count) (?P<A>\w+)",
        r"(highest|max|maximum) (?P<A>\w+)",
        r"(average|avg) (?P<A>\w+)",
        r"(minimum|min|lowest) (?P<A>\w+)",
    ]

    aggregate_group_by_patterns = [
        r"(total|sum) (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"(find|count) (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"(highest|max|maximum) (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"(average|avg) (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
        r"(minimum|min|lowest) (?P<A>\w+) (by|grouped by|group by) (?P<B>\w+)",
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
        "project_columns": [],
        "errors": [],
        "suggestions": []
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
                if 'where' not in col:
                    result["project_columns"].append(col.strip())
    if not column_match and select_present:
        print("select present")
        for col in user_input.split(','):
            result["project_columns"].append(col.strip())
        if result["project_columns"] == ['']:
            result["project_columns"] = ["*"]

    
    for pattern in aggregate_group_by_patterns:
        match = re.search(pattern, user_input.lower(), re.IGNORECASE)
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

    if ("group by" in user_input.lower() or "grouped by" in user_input.lower()) and not result["aggregate_function"]:
        result["errors"].append("A GROUP BY Query cannot be generated without an Aggregate Function. Please include a word correspondng to an aggregate function before the aggregate column. Words such as maximum, lowest, total, etc. apart from direct aggregates are also are supported")
        result["suggestions"].append(f"{random_key1} {user_input}")
        return result
    
    if ("group by" in user_input.lower() or "grouped by" in user_input.lower()) and result["group_by"] == False:
        result["errors"].append("Invalid Group By Prompt Detected. Make sure you have specified both an aggregate column name and group column name")
        result["suggestions"].append(f"Suggested Columns: {random_col1}, {random_col2}, {random_numeric_col1}, {random_numeric_col2}")
        return result

    top_match = re.search(top_pattern, user_input, re.IGNORECASE)
    if top_match:
        result["top"] = top_match.group().strip()

    # If no aggregate pattern matched, set type to "simple"
    if not result["type"]:
        result["type"] = "simple"
    
    # Extract WHERE clause (if any)
    where_match = re.search(where_pattern, user_input.lower(), re.IGNORECASE)
    if where_match:
        result["where_condition"] = where_match.group("condition").strip()

    if "where" in user_input.lower() and not result["where_condition"]:
        result["errors"].append("The keyword 'where' is used in the input provided. Please note that the WHERE clause cannot be used without a condition. Please include a condition in your query.")
        result["suggestions"].append(f"where {random_col2} {random_cond} {random.randint(1, 50)}")
        return result
    
    # Extract HAVING clause (if any)
    having_match = re.search(having_pattern, user_input, re.IGNORECASE)
    if having_match:
        result["having_condition"] = having_match.group("condition").strip()

    if ("having" in user_input.lower() or result["having_condition"]) and not result["group_by"]:
        result["errors"].append("The keyword 'having' is used in the input provided. Please note that the HAVING clause cannot be used without GROUP BY. Please include a GROUP BY clause before the HAVING clause in your query.")
        result["suggestions"].append(f"{random_key1} {random_numeric_col1} group by {random_col1} {user_input} ... ")
        result["suggestions"].append(f"{random_key2} {random_numeric_col2} group by {random_col2} {user_input} ... ")
        return result

    if "having" in user_input.lower() and not result["having_condition"]:
        result["errors"].append("The keyword 'having' is used in the input provided. Please note that the HAVING clause cannot be used without a condition. Please include a condition in your query.")
        result["suggestions"].append(f"having {random_key1}({random_col2}) {random_cond} {random.randint(1, 50)}")
        return result

    # Extract ORDER BY clause (if any) with ascending or descending
    order_by_match = re.search(order_by_pattern, user_input, re.IGNORECASE)
    if order_by_match:
        result["order_by"] = order_by_match.group("columns").strip()
        # Check for ASC or DESC or variations like ascending/descending
        if "desc" in user_input.lower() or "descending" in user_input.lower():
            result["order_direction"] = "DESC"
        elif "asc" in user_input.lower() or "ascending" in user_input.lower():
            result["order_direction"] = "ASC"

    if "order by" in user_input.lower() and not result["order_by"]:
        result["errors"].append("The keyword 'order by' is used in the input provided but no columns are specified. Please note that the ORDER BY clause must be used with the column(s) to be sorted.")
        result["suggestions"].append(f"order by {random_col1}")
        return result

    if not result["columns"] and not result["where_condition"] and not result["having_condition"] and not result["order_by"] and not result["top"] and not result["project_columns"]:
        result["errors"].append("Query does not match any recognizable patterns. Please check your syntax.")
        result["suggestions"].append(f"{random_key1} {random_numeric_col1} group by {random_col2}")
        result["suggestions"].append(f" where {random_numeric_col1} {random_cond} {random.randint(1, 50)} ")
        result["suggestions"].append(f" {random_key2} {random_col3} order by {random_col1} asc")
        result["suggestions"].append(f" {random_col2}, {random_col3} limit {random.randint(1, 10)}" )
        return result

    return result

def handle_natural_language_query(db_type, table_name, user_input):
    if not table_name:
        return jsonify({"error": "A table name is required for this kind of query"}), 400
    
    result = detect_natural_language_query(user_input, table_name, db_type)
    for key, value in result.items():
        print(f"{key}: {value}")

    if not result:
        return jsonify({"error": "Could not detect a valid query pattern"}), 400

    if db_type == 'mysql':
        return handle_mysql_query(table_name, result)
    elif db_type == 'nosql':
        return handle_mongo_query(table_name, result)
    else:
        return jsonify({"error": f"Unsupported database type: {db_type}"}), 400