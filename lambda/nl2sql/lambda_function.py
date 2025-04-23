import os
import json
import asyncio
import logging
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

XAI_API_KEY = os.environ.get("XAI_API_KEY")
XAI_API_URL = os.environ.get("XAI_API_URL", "https://api.x.ai/v1/chat/completions")
XAI_MODEL = os.environ.get("XAI_MODEL", "grok-3-beta")
API_TIMEOUT = float(os.environ.get("API_TIMEOUT", 10.0))
SCHEMA_API_URL = "https://u1gds316me.execute-api.us-east-2.amazonaws.com/v1/exec_sql"

DEFAULT_DB_HOST = "chatdb.cxcuaw08ibd5.us-east-2.rds.amazonaws.com"
DEFAULT_DB_NAME = "banking"
DEFAULT_DB_PORT = 5432

async def fetch_table_structure(host, dbname, port, user, password):
    headers = {"Content-Type": "application/json"}
    payload = {
        "query": "SELECT table_name, column_name, data_type, ordinal_position\nFROM information_schema.columns\nWHERE table_schema = 'public'\nORDER BY table_name, ordinal_position;",
        "host": host,
        "dbname": dbname,
        "port": port,
        "db_user": user,
        "db_password": password
    }
    async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
        try:
            logger.info(f"Fetching table structure from schema API with host={host}, dbname={dbname}, port={port}")
            r = await client.post(SCHEMA_API_URL, headers=headers, json=payload)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Schema API HTTP error: {str(e)}, Response: {e.response.text}")
            raise Exception("Failed to fetch table structure")
        except httpx.RequestError as e:
            logger.error(f"Schema API request error: {str(e)}")
            raise Exception("Failed to fetch table structure")
        except ValueError as e:
            logger.error(f"Schema API response parsing error: {str(e)}")
            raise Exception("Invalid schema API response format")

async def get_valid_tables(host, dbname, port, user, password):
    table_structure = await fetch_table_structure(host, dbname, port, user, password)
    logger.info(f"Schema: {json.dumps(table_structure, indent=2)}")
    return {entry["table_name"].lower() for entry in table_structure}, table_structure

SYSTEM_PROMPT_TEMPLATE = """
You are a SQL generation assistant. Given a natural language instruction, generate an executable SQL query (PostgreSQL dialect) as a plain string based on the following database schema:
{}

Rules:
- Generate queries for CRUD operations (CREATE: INSERT, READ: SELECT, UPDATE, DELETE) using only the tables and columns listed in the schema.
- If the instruction references tables or columns not in the schema, return exactly the single character "X" with no additional text.
- If the instruction is ambiguous, unsafe, or cannot be converted to a valid SQL query, return exactly the single character "X" with no additional text.
- Handle relationships between tables (e.g., joins) when the schema implies connections via matching column names (e.g., branches.managerid links to employees.employeeid).
- For INSERT, include all required columns unless specified; use reasonable defaults if needed.
- For UPDATE and DELETE, include WHERE clauses to avoid affecting unintended rows.
- If asked to explain why a query could not be generated, return a non-empty string explaining the specific reason (e.g., "Table 'orders' does not exist", "Column 'age' does not exist in table 'employees'", "Instruction is too vague").
- Do not include explanations, metadata, or comments in SQL queries, just the SQL query string or exactly "X".
- Ensure queries are safe and executable in PostgreSQL.
- Examples:
  - Input: "Get all customers with lastname Smith" -> Output: "SELECT * FROM customers WHERE lastname = 'Smith';"
  - Input: "Add a new customer with first name John, last name Doe, and email john.doe@example.com" -> Output: "INSERT INTO customers (customerid, firstname, lastname, email, phonenumber, address) VALUES ('CUST001', 'John', 'Doe', 'john.doe@example.com', NULL, NULL);"
  - Input: "Update the salary of employee with employeeid E001 to 60000" -> Output: "UPDATE employees SET salary = 60000 WHERE employeeid = 'E001';"
  - Input: "Delete the account with accountid A001" -> Output: "DELETE FROM accounts WHERE accountid = 'A001';"
  - Input: "Get all orders with price > 100" -> Output: "X"
  - Input: "You failed to generate an SQL query for the instruction: 'Get all orders with price > 100'. Please explain why the query could not be generated (e.g., non-existent table, invalid column, ambiguous instruction)." -> Output: "Table 'orders' does not exist"
  - Input: "You failed to generate an SQL query for the instruction: 'Get all employees with age > 30'. Please explain why the query could not be generated (e.g., non-existent table, invalid column, ambiguous instruction)." -> Output: "Column 'age' does not exist in table 'employees'"
  - Input: "You failed to generate an SQL query for the instruction: 'Get all data from the database'. Please explain why the query could not be generated (e.g., non-existent table, invalid column, ambiguous instruction)." -> Output: "Instruction is too vague"
  - Input: "Find the name of the manager who manages the Downtown branch. Return the name in the format of First Name" -> Output: "SELECT e.firstname FROM branches b JOIN employees e ON b.managerid = e.employeeid WHERE b.branchname = 'Downtown';"
  - Input: "List employees who are not managers, ordered by name" -> Output: "SELECT e.firstname || ' ' || e.lastname AS name, e.jobtitle FROM employees e WHERE e.employeeid NOT IN (SELECT managerid FROM branches) ORDER BY name ASC;"
"""

class XAIClient:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("XAI_API_KEY is missing")
        self.api_key = api_key
        self.base_url = XAI_API_URL
        self.model = XAI_MODEL

    async def generate_sql(self, nl_instruction: str, host: str, dbname: str, port: int, user: str, password: str) -> tuple[str, str]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        try:
            _, table_structure = await get_valid_tables(host, dbname, port, user, password)
            system_prompt = SYSTEM_PROMPT_TEMPLATE.format(json.dumps(table_structure, indent=2))
        except Exception as e:
            logger.error(f"Failed to initialize schema: {str(e)}")
            return "", "Failed to fetch table structure"

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": nl_instruction}
            ],
            "max_tokens": 512,
            "temperature": 0.2
        }
        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            try:
                logger.info(f"Sending initial request to xAI API for instruction: {nl_instruction}")
                r = await client.post(self.base_url, headers=headers, json=payload)
                r.raise_for_status()
                
                raw_response = r.text
                logger.debug(f"Raw API response: {raw_response}")
                
                response_data = r.json()
                choices = response_data.get("choices", [])
                if not choices or not isinstance(choices, list) or not choices[0].get("message"):
                    logger.error("Invalid response structure: empty or malformed 'choices'")
                    return "", "Invalid API response structure"
                
                content = choices[0]["message"].get("content", "")
                logger.info(f"Received initial response: {content}")
                
                # Strip quotes and backslashes to handle cases like "\"X\"" or "X, ..."
                sql_query = content.strip().strip('"').replace('\\"', '')
                logger.info(f"Cleaned response: {sql_query}")
                
                error_reason = ""
                
                # If sql_query is "X" or starts with "X", ask Grok for an error explanation
                if sql_query == "X" or sql_query.startswith("X"):
                    logger.info(f"Empty query indicator 'X' detected for: {nl_instruction}. Requesting error explanation.")
                    follow_up_prompt = (
                        f"You failed to generate an SQL query for the instruction: '{nl_instruction}'. "
                        "Please explain why the query could not be generated "
                        "(e.g., non-existent table, invalid column, ambiguous instruction). "
                        "Provide a specific, non-empty explanation."
                    )
                    follow_up_payload = {
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": nl_instruction},
                            {"role": "assistant", "content": content},
                            {"role": "user", "content": follow_up_prompt}
                        ],
                        "max_tokens": 512,
                        "temperature": 0.2
                    }
                    logger.info(f"Sending follow-up request for error explanation")
                    follow_up_r = await client.post(self.base_url, headers=headers, json=follow_up_payload)
                    follow_up_r.raise_for_status()
                    
                    follow_up_response = follow_up_r.json()
                    follow_up_choices = follow_up_response.get("choices", [])
                    if not follow_up_choices or not follow_up_choices[0].get("message"):
                        logger.error("Invalid follow-up response structure")
                        return "", "Failed to determine error reason"
                    
                    follow_up_content = follow_up_choices[0]["message"].get("content", "")
                    logger.info(f"Received follow-up response: {follow_up_content}")
                    
                    error_reason = follow_up_content.strip()
                    sql_query = ""  # Reset sql_query to empty for invalid queries
                    if not error_reason:
                        logger.warning("Follow-up response was empty")
                        error_reason = "Failed to determine error reason"
                
                return sql_query, error_reason
                
            except httpx.HTTPStatusError as e:
                logger.error(f"API HTTP error: {str(e)}, Response: {e.response.text}")
                return "", f"API error: {e.response.status_code}"
            except httpx.RequestError as e:
                logger.error(f"API request error: {str(e)}")
                return "", "API request failed"
            except ValueError as e:
                logger.error(f"Response parsing error: {str(e)}")
                return "", "Invalid API response format"

async def _handle(event, context):
    logger.info(f"Received event with request ID: {context.aws_request_id}")

    try:
        body = event.get("body") or "{}"
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return {
            "statusCode": 400,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "Invalid JSON in request body"})
        }

    nl = data.get("query")
    if not isinstance(nl, str) or not nl.strip():
        logger.error("Missing or invalid 'query' field")
        return {
            "statusCode": 400,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "Missing or invalid 'query' field"})
        }

    host = data.get("host", DEFAULT_DB_HOST)
    dbname = data.get("dbname", DEFAULT_DB_NAME)
    port = int(data.get("port", DEFAULT_DB_PORT))
    user = data.get("db_user", os.environ.get("DB_USER"))
    password = data.get("db_password", os.environ.get("DB_PASSWORD"))
    logger.info(f"Using connection parameters: host={host}, dbname={dbname}, port={port}")

    try:
        client = XAIClient(XAI_API_KEY)
        sql_query, error_reason = await client.generate_sql(nl.strip(), host, dbname, port, user, password)
        if not sql_query:
            logger.warning(f"No SQL query generated: {error_reason}")
            return {
                "statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"sql_query": "", "error_reason": error_reason})
            }
        logger.info(f"Generated SQL: {sql_query}")
        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"sql_query": sql_query, "error_reason": ""})
        }
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            "statusCode": 502,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)})
        }

def lambda_handler(event, context):
    return asyncio.run(_handle(event, context))