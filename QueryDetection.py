import re
from flask import Flask, jsonify, request
import pandas as pd
from DatabaseSetup import connect_mysql, connect_mongo, get_columns_from_mysql, get_fields_from_mongodb

def get_table_columns(engine, table_name):
    return get_columns_from_mysql(engine, table_name)

def get_mongo_collection_columns(db, table_name):
    return get_fields_from_mongodb(db, table_name)

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

def handle_mongo_query(collection_name, result):
    engine = connect_mongo()
    table_columns = get_mongo_collection_columns(engine, collection_name)

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
        condition_dict = parse_condition(where_condition, table_columns)
        if not condition_dict:
            return jsonify({"error": "Invalid condition or column name in the WHERE clause."}), 400
        match_stage = {"$match": condition_dict}
    else:
        match_stage = {}
        missing_clauses.append("WHERE clause")

    # Construct HAVING clause
    if having_condition:
        having_dict = parse_condition(having_condition, table_columns)
        if not having_dict:
            return jsonify({"error": "Invalid condition or column name in the HAVING clause."}), 400
        having_stage = {"$match": having_dict}
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
        limit_stage = {"$limit": limit_clause}
    else:
        limit_stage = {"$limit": 10}  # Default limit if not specified
        missing_clauses.append("LIMIT clause")

    # Build aggregate query
    if query_type == "aggregate":
        if not aggregate_function:
            return jsonify({"error": "Aggregate function not specified."}), 400

        if group_by_column:
            pipeline = [
                match_stage,
                {"$group": {
                    "_id": f"${group_by_column}",
                    f"{aggregate_function}_{aggregate_columns['A']}": {f"${aggregate_function}": f"${aggregate_columns['A']}"}
                }},
                having_stage,
                sort_stage,
                limit_stage
            ]
        else:
            pipeline = [
                match_stage,
                {"$group": {
                    "_id": None,
                    f"{aggregate_function}_{aggregate_columns['A']}": {f"${aggregate_function}": f"${aggregate_columns['A']}"}
                }},
                having_stage,
                sort_stage,
                limit_stage
            ]
    else:  # Build Simple SELECT query
        if not project_columns:
            project_columns = {"_id": 0}  # Exclude `_id` by default for MongoDB projections
        else:
            project_columns = {col: 1 for col in project_columns}

        pipeline = [
            match_stage,
            {"$project": project_columns},
            sort_stage,
            limit_stage
        ]

    # Build helpful message for missing clauses
    message = "Here is your query pipeline:"
    if missing_clauses:
        message += "\n\nIt looks like you may have missed the following stages. Here’s how you can use them:\n"
        if "WHERE clause" in missing_clauses:
            message += "1. WHERE stage: Use `$match` to filter documents, e.g., `{'age': {'$gt': 30}}`.\n"
        if "HAVING clause" in missing_clauses:
            message += "2. HAVING stage: Use `$match` after `$group` to filter grouped results, e.g., `{'count': {'$gt': 5}}`.\n"
        if "ORDER BY clause" in missing_clauses:
            message += "3. ORDER BY stage: Use `$sort` to sort results, e.g., `{'age': 1}` for ascending.\n"
        if "LIMIT clause" in missing_clauses:
            message += "4. LIMIT stage: Use `$limit` to restrict the number of results, e.g., `10`.\n"
    print("Pipeline:", pipeline)
    return jsonify({"queries": [str(pipeline)], "message": message}), 200

def detect_natural_language_query(user_input):
    AGGREGATE_FUNCTIONS = {
        'total': 'SUM',
        'highest': 'MAX',
        'average': 'AVG',
        'minimum': 'MIN',
        'maximum': 'MAX'
    }
    
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
    elif db_type == 'nosql':
        return handle_mongo_query(table_name, result)
    else:
        return jsonify({"error": f"Unsupported database type: {db_type}"}), 400