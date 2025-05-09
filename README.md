# ChatDB: A Serverless System for Natural Language to SQL Translation

A system that allows users to interact with AWS RDS for PostgreSQL databases using natural language. The system supports CRUD operations and complex queries including SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET clauses. Powered by grok-3-beta model for natural language understanding.

## CLI Usage

The CLI provides an interactive interface for converting natural language to SQL and executing queries.

### Installation
```bash
pip install -r requirements.txt
```

### Running the CLI
```bash
python cli.py [options]
```

### Options
- `--host`: Database host address
- `--dbname`: Database name
- `--port`: Database port (default: 5432)
- `--db_user`: Database username
- `--db_password`: Database password

If no database parameters are provided, the CLI will use an example database for demonstration.

### Example Usage
```bash
# Using example database
python cli.py

# Using custom database
python cli.py --host mydb.example.com --dbname mydb --port 5432 --db_user admin --db_password secret
```

## API Usage

The system provides two REST API endpoints:

### 1. Natural Language to SQL Conversion
```
POST https://u1gds316me.execute-api.us-east-2.amazonaws.com/v1/nl2sql
```

Request Body:
```json
{
    "query": "Find all customers with last name Smith",
    "host": "mydb.example.com",
    "dbname": "mydb",
    "port": 5432,
    "db_user": "admin",
    "db_password": "secret"
}
```

Response:
```json
{
    "sql_query": "SELECT * FROM customers WHERE lastname = 'Smith';",
    "error_reason": ""
}
```

### 2. SQL Execution
```
POST https://u1gds316me.execute-api.us-east-2.amazonaws.com/v1/exec_sql
```

Request Body:
```json
{
    "query": "SELECT * FROM customers WHERE lastname = 'Smith';",
    "host": "mydb.example.com",
    "dbname": "mydb",
    "port": 5432,
    "db_user": "admin",
    "db_password": "secret"
}
```

Response:
```json
[
    {
        "customerid": "C001",
        "firstname": "John",
        "lastname": "Smith",
        "email": "john.smith@example.com"
    }
]
```

## API Redeployment

This project is built on a serverless architecture, where the backend logic is deployed as AWS Lambda functions. Each Lambda function is exposed through API Gateway endpoints, creating a fully serverless REST API. The system consists of two main components:

1. **Natural Language to SQL Conversion Lambda** (`nl2sql`)
   - Converts natural language queries to SQL
   - Exposed through API Gateway endpoint `/v1/nl2sql`
   - Runtime: Python 3.13

2. **SQL Execution Lambda** (`exec_sql`)
   - Executes SQL queries against PostgreSQL database
   - Exposed through API Gateway endpoint `/v1/exec_sql`
   - Runtime: Python 3.12

The layers required for Lambda function dependency is included in the file tree, and the Grok API KEY shall be stored in the Lambda environment variable. 
