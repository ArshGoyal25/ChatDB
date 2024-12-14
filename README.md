# Welcome to ChatDB

ChatDB is an interactive application designed to help users learn how to query database systems like SQL and NoSQL effectively. The tool simplifies database exploration by:  
- Suggesting and executing SQL and NoSQL queries.  
- Providing a natural language interface to interact with databases.  
- Supporting MySQL, and MongoDB integration.  
- Allowing seamless management and visualization of data via a React-based frontend.  

---

## Features  
- **SQL/NoSQL Query Learning**: Generate and execute /NoSQLSQL queries with guidance.  
- **Generate Sample Queries**: View Suggestions on queries to execute in database
- **Natural Language Interface**: Use plain English to interact with databases.
- **React Frontend**: Intuitive user interface for smooth interaction.  

---
## Project Structure 

1. **Backend**:    
   - `DatabaseSetup.py` - Sets up, interacts and executes queries in the MySQL and MongoDB databases.  
   - `DescribeQuery.py` - Contains logic to describe the contents of the tables.  
   - `ExampleQuery.py` - Contains logic to generate example queries.  
   - `QueryDetection.py` - Detects and analyzes database query patterns, and generates queries based on natural language input.  
   - `Config.py` - Centralized configuration for database connection strings.  
   - `app.py` - Flask Server with RestAPI endpoints for processing user requests.  


2. **Frontend** (React):  
   - `database-ui/` - Contains the React components, routes, and services for the user interface.  


3. **Other Files**:  
   - `Data_Files` - Contains xlsx and json data used to populate the MySQL database.  
   - `requirements.txt` - Python dependencies for the backend.  
   - `package.json` - Node.js dependencies for the React frontend.  
   - `README.md` - Project documentation.  


---

## Setup Guide  

### Prerequisites 
- Python 3.x  
- Node.js and npm  
- MySQL and MongoDB installed locally  

### 1. Backend Setup  

#### Create and Activate a Virtual Environment  
```bash
python3 -m venv project
source ./project/bin/activate
pip3 install -r requirements.txt
```

#### Install the requirements
```bash
pip3 install -r requirements.txt
```

### 2. Set Up MySQL Database
```bash
mysql -u root -p  
CREATE DATABASE coffee_shop;  
SHOW DATABASES; -- Verify that `coffee_shop` is listed  
EXIT; -- Exit MySQL console 
```

### 3. Set Up MongoDB Collection

```bash
mongod --dbpath=<path_to_your_data_directory>  
mongosh  
use coffee_shop;  -- Switch to the database - it will be created if it doesn't exist  
```

### 4.Configure Database Connections

Update the connection details in Config.py as per your setup.

#### MySQL Configuration
```bash
MYSQL_USER = "root"  
MYSQL_PASSWORD = "your_password"  
MYSQL_HOST = "localhost"  
MYSQL_PORT = 3306  
MYSQL_DB = "coffee_shop"  
```

#### MongoDB Configuration  
```bash
MONGO_URI = "mongodb://localhost:27017/"  
MONGO_DB = "coffee_shop"
```

- Replace the MySQL connection string with your local MySQL configuration.
- Replace the MongoDB connection string with your MongoDB URI.


### 5. Frontend Setup 

#### Navigate to the Frontend Directory
```bash
cd data 
```

#### Install Node.js Packages
```bash
npm install
```

### 6. Running the Application
Two terminals are required to run the application

#### In the first terminal, start the backend server
```bash
python3 app.py 
```

#### In the second terminal, start the frotend React Application
```bash
cd database-ui
npm start 
```

#### Open a browser and navigate to:
```bash
http://localhost:3000  
```