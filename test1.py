def parse_condition_nosql(condition: str, valid_columns: list) -> dict:
    operators = {
        "=": "$eq",
        "==": "$eq",
        "!=": "$ne",
        "<>": "$ne",
        "<": "$lt",
        "<=": "$lte",
        ">": "$gt",
        ">=": "$gte",
        "is equal to": "$eq",
        "is not equal to": "$ne",
        "is less than": "$lt",
        "is less than or equal to": "$lte",
        "is greater than": "$gt",
        "is greater than or equal to": "$gte",
    }

    # Split conditions by logical operators (e.g., AND, OR) while keeping the operators
    conditions = re.split(r'\s+and\s+|\s+or\s+', condition, flags=re.IGNORECASE)

    mongo_query = []

    for cond in conditions:
        condition_dict = {}
        for operator, mongo_op in operators.items():
            if operator.lower() in cond.lower():
                left, right = cond.split(operator, 1)
                left = left.strip()
                right = right.strip()

                # Ensure the left side is a valid column
                if left not in valid_columns:
                    return None

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
            return None

    if len(mongo_query) > 1:
        if re.search(r'\bor\b', condition):
            return {"$or": mongo_query}
        else:
            return {"$and": mongo_query}

    return mongo_query[0]