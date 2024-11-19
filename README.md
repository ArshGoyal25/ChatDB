# ChatDB
# setup virtual env:
python3 -m venv project

# Activate virtual env:
source ./project/bin/activate 

# install required packages:
# pymongo for MongoDB
# sqlalchemy and mysqlconnector or mysql connection via sqlalchemy
# pandas
pip3 install pymongo sqlalchemy mysql-connector-python pandas

# create db in mysql:
mysql -u root -p
CREATE DATABASE coffee_shop;
SHOW DATABASES; -- should have coffee_shop listed
EXIt -- to exit mysql

# DatabaseSetup.py
Replace sql and mongodb connection string with local system string
Ensure coffee_shop.xlsx is in the same directory

run: python3 DatabaseSetup.py
