
import re
def parse_condition(condition, valid_columns):
    condition = condition.strip().lower()

    # Handle phrases like "is equal to", "is less than", etc.
    operators = {
        "!=": "!=",
        "<": "<",
        "<=": "<=",
        ">": ">",
        ">=": ">=",
        "=": "=",
        "is greater than or equal to": ">=",
        "is less than or equal to": "<=",
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
    
    # print("Conditions:", conditions)
    # print("Operators:", operators_in_condition)

    sql_query = ""
    for i, cond in enumerate(conditions):
        for operator in ops:
            if operator in cond:
                # print("operator: ", operator)
                left, right = cond.split(operator)
                left = left.strip()
                right = right.strip()

                if left not in valid_columns:
                    return None

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

    return sql_query

a = f"product_type is equal to premium beans or store_id >= 5 and store_id is less than 5"
valid_columns = ['transaction_id', 'transaction_date', 'transaction_time', 'transaction_qty', 'store_id', 'store_location', 'product_id', 'unit_price', 'product_category', 'product_type', 'product_detail']

print(parse_condition(a, valid_columns))