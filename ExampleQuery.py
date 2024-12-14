import random
import ast
from DatabaseSetup import  connect_mongo, connect_mysql, get_columns_and_types_from_mysql, get_sample_row_mysql, get_field_types_from_mongodb, get_sample_document_mongodb

def get_random_aggregation_function():
    """Select a random aggregation function."""
    aggregation_functions = ['AVG', 'SUM', 'COUNT', 'MAX', 'MIN']
    return random.choice(aggregation_functions)

def get_random_column(columns):
    """Select a random column from a list of columns."""
    return random.choice(columns)

def get_random_operation(db_type):
    if db_type == "mysql":
        operators = ["=", "<", ">", "!=", ">=", "<="]
    elif db_type == "nosql":
        operators = ["$eq", "$lt", "$gt", "$ne", "$gte", "$lte"]
    return random.choice(operators)

def get_numeric_cols_sql(columns):
    numeric_columns = [
    col for col, col_type in columns.items()
    if any(keyword in col_type.lower() for keyword in ['int', 'float', 'double', 'decimal'])
    ]
    return numeric_columns

def generate_sql_select(columns):
    project_columns = random.sample(list(columns.keys()),k=2)  # Select 2 random columns
    return project_columns

def generate_sql_where_clause(columns, sample_row):
    project_columns = random.sample(list(columns.keys()),k=2)  # Select 2 random columns

    column = random.choice(list(columns.keys()))
    numeric_columns = get_numeric_cols_sql(columns)        
    value = sample_row[0][column]

    if column in numeric_columns:
        operator = get_random_operation("mysql")
    else:
        operator = random.choice(["=", "!="])
        value = f"'{value}'"  # Wrap non numeric values in quotes

    where_clause = f"{column} {operator} {value}"
    return where_clause, project_columns

def generate_sql_group_by(columns):
    numeric_columns = get_numeric_cols_sql(columns)
    if not numeric_columns:
        return None
    agg_func = get_random_aggregation_function()
    agg_column = random.choice(numeric_columns)
    group_column = random.choice(list(columns.keys()))

    project_cols = [group_column, f"{agg_func}({agg_column})"]

    return agg_func, agg_column, group_column, project_cols

def generate_sql_having(columns):
    agg_func, agg_column, group_column, project_columns = generate_sql_group_by(columns)
    having_op = get_random_operation("mysql")
    return agg_func, agg_column, having_op, group_column, project_columns

def generate_sql_order_by(columns):
    orderby_column = random.choice(columns)
    order_direction = random.choice(["ASC", "DESC"])
    return orderby_column, order_direction

def generate_sql_aggregate(columns):
    numeric_columns = get_numeric_cols_sql(columns)
    if not numeric_columns:
        return None
    agg_func = get_random_aggregation_function()
    agg_column = random.choice(numeric_columns)

    project_cols = [f"{agg_func}({agg_column})"]
    return project_cols

def sql_generate_example_queries(table_name, columns, sample_row, user_input):
    query = ""
    limit_columns=None
    project_columns = where_clause = agg_func = agg_column = group_column = orderby_column = order_direction = having_op = None
    if "select" in user_input.lower():
        project_columns = generate_sql_select(columns)
    if "limit" in user_input.lower():
        if "select" not in user_input.lower():
            user_input+="select "
            return sql_generate_example_queries(table_name, columns, sample_row, user_input)
        limit_columns = "Limit"
    if "where" in user_input.lower():
        where_clause, project_columns = generate_sql_where_clause(columns, sample_row)
    if "aggregate" in user_input.lower():
        project_columns = generate_sql_aggregate(columns)
    if "group by" in user_input.lower():
        agg_func, agg_column, group_column, project_columns = generate_sql_group_by(columns)
    if "having" in user_input.lower():
        agg_func, agg_column, having_op, group_column, project_columns = generate_sql_having(columns)
    if "order by" in user_input.lower():
        if "group by" in user_input.lower() or "having" in user_input.lower():
            orderby_column, order_direction = generate_sql_order_by(project_columns)
        else:
            orderby_column, order_direction = generate_sql_order_by(list(columns.keys()))
            project_columns = random.sample(list(columns.keys()),k=2)

    if project_columns is not None:
        query += f"SELECT {', '.join(project_columns)} FROM {table_name}"
    if where_clause is not None:
        query += f" WHERE {where_clause}"
        if limit_columns is not None and group_column is None and having_op is None and orderby_column is None:
            query += f" LIMIT {10}"
    if group_column is not None:
        query += f" GROUP BY {group_column}"
        if limit_columns is not None and having_op is None and orderby_column is None:
            query += f" LIMIT {10}"
    if having_op is not None:
        query += f" HAVING {agg_func}({agg_column}) {having_op} {random.randint(1, 10)}"
        if limit_columns is not None and orderby_column is None:
            query += f" LIMIT {10}"
    if orderby_column is not None:
        query += f" ORDER BY {orderby_column} {order_direction}"
        if limit_columns is not None:    
            query += f" LIMIT {10}"
    if group_column is None and having_op is None and "aggregate" not in user_input.lower() and project_columns is not None:
        if limit_columns is not None:
            query += f" LIMIT {10}"

    if query == "":
        return None
    
    return query



def get_numeric_fields_nosql(fields):
    numeric_types = ['int', 'long', 'double', 'decimal128']
    numeric_fields = [field for field, field_type in fields.items() if any(num_type in field_type for num_type in numeric_types)]
    return numeric_fields


def generate_nosql_project_query(fields):
    projected_fields = {field: 1 for field in random.sample(list(fields.keys()), k=2)}
    return projected_fields

def generate_nosql_match_query(columns, sample_row):
    column = random.choice(list(columns.keys()))
    numeric_columns = get_numeric_fields_nosql(columns)        
    value = sample_row[column]

    if column in numeric_columns:
        operator = get_random_operation("nosql")
    else:
        operator = random.choice(["", "$ne"])
    
    if isinstance(value, str):
        value = f"'{value}'"  # Wrap string values in quotes

    if operator:
        match_clause = f"{{'{column}': {{'{operator}': {value}}}}}"
    else:
        match_clause = f"{{'{column}': {value}}}"
    return match_clause

def generate_nosql_group_query(columns):
    # Filter INT type fields
    numeric_fields = get_numeric_fields_nosql(columns)
    if not numeric_fields:
        return "No suitable INT fields for aggregation"
    agg_func = get_random_aggregation_function()
    group_field = random.choice(list(columns.keys()))
    agg_field = random.choice(numeric_fields)
    agg_func = agg_func.lower()
    return agg_func, agg_field, group_field

def generate_nosql_group_with_match_query(columns):

    agg_func, agg_field, group_field = generate_nosql_group_query(columns)
    op = get_random_operation("nosql")
    
    aggregate_condition = f"{{'$match': {{'{agg_func}_col': {{'{op}': {random.randint(1, 10)}}}}}}}"
    return aggregate_condition, agg_func, agg_field, group_field

def generate_nosql_sort_query(fields):
    orderby_field = random.choice(fields)
    order_direction = random.choice([1, -1])
    
    sort_query = f"{{ '$sort': {{ '{orderby_field}': {order_direction} }} }}"
    return sort_query


def mongodb_generate_example_queries(collection_name, fields, sample_document, user_input):
    query = ""

    # Keywords to identify user intent
    keywords = ["project", "match", "sort", "group", "aggregate", "limit"]

    if any(keyword in user_input.lower() for keyword in keywords):

        limit_value = 5  # Default limit value
        # Extract limit value from the user input if present
        if "limit" in user_input.lower():
            try:
                limit_value = int([word for word in user_input.split() if word.isdigit()][0])
            except (IndexError, ValueError):
                pass  # Use default if parsing fails

        if "group" not in user_input.lower() and "aggregate" not in user_input.lower() and "sort" not in user_input.lower():
            project_fields = {}
            match_clause = {}
            if "project" in user_input.lower():
                project_fields = generate_nosql_project_query(fields)
            if "match" in user_input.lower():
                match_clause = generate_nosql_match_query(fields, sample_document)
            query = f"db.{collection_name}.find({match_clause}, {project_fields}).limit({limit_value})"
        elif "group" not in user_input.lower() and "aggregate" not in user_input.lower():
            stages = []
            if "match" in user_input.lower():
                match_clause = generate_nosql_match_query(fields, sample_document)
                match_clause = ast.literal_eval(match_clause)  # Converts string to dictionary
                stages.append({'$match': match_clause})
            if "project" in user_input.lower():
                project_fields = generate_nosql_project_query(fields)
                stages.append({'$project': project_fields})
            if "sort" in user_input.lower():
                sort_query = generate_nosql_sort_query(list(fields.keys()))
                sort_query = ast.literal_eval(sort_query)
                stages.append(sort_query)
            stages.append({'$limit': limit_value})
            query = f"db.{collection_name}.aggregate({stages})"
        else:
            stages = []
            project_clause = ""
            if "group" in user_input.lower() and "aggregate" not in user_input.lower():
                agg_func, agg_field, group_field = generate_nosql_group_query(fields)
                if agg_func == "count":
                    stages.append(ast.literal_eval(f"{{'$group': {{'_id': '${group_field}', '{agg_func}_col': {{'$sum': 1}}}}}}"))
                else:
                    stages.append(ast.literal_eval(f"{{'$group': {{'_id': '${group_field}', '{agg_func}_col': {{'${agg_func}': '${agg_field}'}}}}}}"))
                project_clause = f"{{'$project': {{'_id': 0, '{group_field}': '$_id', '{agg_func}_col': 1}}}}"
            if "group" in user_input.lower() and "aggregate" in user_input.lower():
                aggregate_condition, agg_func, agg_field, group_field = generate_nosql_group_with_match_query(fields)
                if agg_func == "count":
                    stages.append(ast.literal_eval(f"{{'$group': {{'_id': '${group_field}', '{agg_func}_col': {{'$sum': 1}}}}}}"))
                else:
                    stages.append(ast.literal_eval(f"{{'$group': {{'_id': '${group_field}', '{agg_func}_col': {{'${agg_func}': '${agg_field}'}}}}}}"))
                stages.append(ast.literal_eval(aggregate_condition))
                project_clause = f"{{'$project': {{'_id': 0, '{group_field}': '$_id', '{agg_func}_col': 1}}}}"
            if "sort" in user_input.lower():
                sort_query = generate_nosql_sort_query([f"_id", f"{agg_func}_col"])
                sort_query = ast.literal_eval(sort_query)
                stages.append(sort_query)
            if "project" in user_input.lower() and project_clause != "":
                stages.append(ast.literal_eval(project_clause))
            stages.append({'$limit': limit_value})
            query = f"db.{collection_name}.aggregate({stages})"

    if query == "":
        return None
    return query


def generate_example_queries(table_name, user_input, db_type='mysql'):
    queries = []
    if db_type == 'mysql':
        engine = connect_mysql()
        columns = get_columns_and_types_from_mysql(engine, table_name)
        sample_row = get_sample_row_mysql(engine, table_name)
        query = sql_generate_example_queries(table_name, columns, sample_row, user_input)
        
        # No constructs specified. pick 3 examples at random
        if query is None:
            sample_input = ["select", "select where", "group by", "having", "order by", "where order by", "group by order by", "aggregate", "limit"]
            example_queries = random.sample(sample_input, 3)
            for example in example_queries:
                query = sql_generate_example_queries(table_name, columns, sample_row, example)
                queries.append(query)
        else:
            queries.append(query)
    elif db_type == 'nosql':
        db = connect_mongo()
        fields = get_field_types_from_mongodb(db, table_name)
        sample_documnent = get_sample_document_mongodb(db, table_name)
        query = mongodb_generate_example_queries(table_name, fields, sample_documnent, user_input)
        # No constructs specified. pick 3 examples at random
        if query is None:
            sample_input = ["project", "project match", "project sort", "group", "group aggregate", "group sort", "group aggregate sort", "group project", "group sort project"]
            example_queries = random.sample(sample_input, 3)
            for example in example_queries:
                query = mongodb_generate_example_queries(table_name, fields, sample_documnent, example)
                queries.append(query)
        else:
            queries.append(query)

    return queries