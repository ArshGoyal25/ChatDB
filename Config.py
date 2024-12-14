# Database configuration - Replace with actual credentials
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
mysql_password_encoded = MYSQL_PASSWORD.replace('@', '%40')
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "coffee_shop"

MONGO_URI = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.3.2"
MONGO_DATABASE = "coffee_shop"
COLLECTION_NAME='sales'