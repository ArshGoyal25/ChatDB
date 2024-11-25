import re

# Define aggregate functions and their corresponding keywords
AGGREGATE_FUNCTIONS = {
    'total': 'SUM',
    'highest': 'MAX',
    'average': 'AVG',
    'minimum': 'MIN',
    'maximum': 'MAX'
}

def detect_natural_language_query(user_input):
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
    
    # Modify ORDER BY pattern to handle variations like "sort by", "ascending", and "descending"
    order_by_pattern = r"(order by|sort by) (?P<columns>.+?)( asc| desc| ascending| descending|$)"
    

    # Patterns for 'TOP' and other special cases like 'LIMIT'
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
    column_pattern = r"^([\w\s,]+?)(?=\s*(total|highest|average|minimum|maximum|where|order|grouped by|group by|sort by|asc|desc|ascending|descending|top\s*\d+|limit\s*\d+))"
    
    # Check for columns at the start of the query
    column_match = re.search(column_pattern, user_input, re.IGNORECASE)
    
    if column_match:
        # If columns are found, split them by commas and clean any extra spaces
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

# Test Cases
queries = [
    "store_id where product_type is equal to premium beans",
    "product_category, store_location where store_location is equal to lower manhattan",
    "total transaction_qty where store_location is equal to Lower Manhattan order by transaction_qty desc",
    "product_id, product_type, transaction_qty where store_location is equal to Lower Manhattan order by transaction_qty desc",
    "highest unit_price group by product_category where product_category is equal to Coffee",
    "average unit_price where product_type is equal to Drip coffee",
    "maximum transaction_qty order by transaction_qty asc",
    "total transaction_qty where product_type is equal to Gourmet brewed coffee order by unit_price descending",
    "maximum transaction_qty group by store_location where transaction_qty is equal to 4 order by transaction_qty ascending",
    "transaction_qty where product_type is equal to 'Gadgets' and product_detail is equal to 'Red'",
    "transaction_qty where store_location is equal to 'West' order by transaction_qty desc top 10",
    "product_type, transaction_qty limit 5 where store_location is equal to lower manhattan order by transaction_qty desc",
    "product_type, transaction_qty where store_location is equal to lower manhattan order by transaction_qty desc top 10"
]


for query in queries:
    print(f"Input: {query}")
    result = detect_natural_language_query(query)
    for key, value in result.items():
        print(f"{key}: {value}")
    print()
