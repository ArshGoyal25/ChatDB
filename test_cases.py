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


# queries1 = 
# [
#     "where transaction_qty is equal to 7",
#     "max product_id group by product_type",
#     "minimum product_id order by transaction_date asc",
#     "product_type, product_id limit 9",
#     "total transaction_qty where product_type is equal to premium beans",,
#     "product_category, store_location where store_location is equal to lower manhattan",
#     "average unit_price where product_type is equal to Drip coffee",
#     "find transaction_qty group by transaction_date"
# ]