queries = [
    "store_id where product_type is equal to Hot chocolate",
    "product_category, store_location where store_location is equal to lower manhattan",
    "where store_location is equal to Lower Manhattan order by transaction_qty desc",
    "total transaction_qty where store_location is equal to Lower Manhattan"
    "total unit_price group by product_category",
    "highest unit_price group by product_category where product_category is equal to Coffee",
    "average unit_price where product_type is equal to Drip coffee",
    "where store_location is equal to Lower Manhattan and product_category is equal to branded and unit_price is less than 20 order by transaction_qty desc",


    "maximum transaction_qty group by store_location where transaction_qty is equal to 4 order by transaction_qty ascending",
    "transaction_qty where product_type is equal to 'Gadgets' and product_detail is equal to 'Red'",
    "transaction_qty where store_location is equal to 'West' order by transaction_qty desc top 10",
    "product_type, transaction_qty limit 5 where store_location is equal to lower manhattan order by transaction_qty desc",
    "product_type, transaction_qty where store_location is equal to lower manhattan order by transaction_qty desc top 10"
]

queriesProductMongo=[
"where price is not equal to 11",
"highest warranty_years group by brand",
"warranty_years group by brand", #to show error and suggestion. Use suggestion
"total price group by brand order by brand desc",
"total price group by brand order by brand desc having price is greater than 20", #group by error show
"total price group by brand order by brand desc having total price is greater than 20", #fix for above
"total price group by brand order by brand desc having total price is greater than 20 where brand is equal to Apple ",
"total price order by brand desc having total price is greater than 20 where brand is equal to Apple ", #having without group by 
"where rating is less than 4", #for int
"where rating is less than equal to 4 and color is equal to red", #and
"where rating <= 4 or color = red or price > 20"
]

queriesClothes=[
    "maximum sales group by brand",
    "maximum sales group by brand having max sales is greater than 1000",
    "having max sales is greater than 1000", #- error,
    "where sales is greater than 1000 order by stock descending",
    "brand, name where sales is greater than 1000 order by stock descending",
    " brand, name where sales is greater than 1000 order by stock descending limit 1",
    " brand, name limit 1 where sales is greater than 1000 order by stock descending",
    " brand, name, sales, stock where sales is greater than 1000 and stock is less than 100 order by stock descending",
    " brand, name, sales, stock order by stock descending where sales is greater than 1000 and stock is less than 100 ",
    "max rating_score group by year_published",
    ""




]


queriesBooks= [
    "where rating_votes is equal to 161942",
    "where rating_votes is equal to 161942 or rating_score is less than 4",
    "where rating_votes is equal to 161942 or rating_score is less than 4 and review_number is equal to 16092",
    "where rating_votes is equal to 161942 or rating_score < 4 order by author_name",
    "where year_published is not equal to 2000",
    "max rating_score group by year_published having year_published = 2012",
    "max rating_score group by year_published having max rating_score < 4.4",
    "max rating_score group by year_published having max rating_score <= 4.4 where year_published is equal to 1999",
    "max rating_score group by year_published limit 3 having max rating_score <= 4.4 order by year_published"


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