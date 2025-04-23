import os
import json
import psycopg
from psycopg.rows import dict_row

DEFAULT_DB_HOST = "chatdb.cxcuaw08ibd5.us-east-2.rds.amazonaws.com"
DEFAULT_DB_NAME = "banking"
DEFAULT_DB_PORT = 5432

def lambda_handler(event, context):
    """
    Lambda entrypoint. Expects a JSON body with:
      - query (str): the SQL to execute (required)
      - host (str): optional override for DB_HOST
      - dbname (str): optional override for DB_NAME
      - port (int): optional override for DB_PORT
      - db_user (str): optional override for DB_USER env var
      - db_password (str): optional override for DB_PASSWORD env var

    Runs the SQL on RDS PostgreSQL, commits if needed, and returns:
      - For SELECT: an array of rows
      - For INSERT/UPDATE/DELETE: {"rowcount": N}
    """
    try:
        # 1) Parse incoming JSON body
        body = event.get("body") or "{}"
        data = json.loads(body)

        # 2) Extract SQL
        sql = data.get("query")
        if not sql:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": 'Missing "query" field'})
            }

        # 3) Determine connection parameters (allow overrides)
        host     = data.get("host",     DEFAULT_DB_HOST)
        dbname   = data.get("dbname",   DEFAULT_DB_NAME)
        port     = int(data.get("port", DEFAULT_DB_PORT))
        user     = data.get("db_user",     os.environ.get("DB_USER"))
        password = data.get("db_password", os.environ.get("DB_PASSWORD"))

        if not user or not password:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Database credentials not provided"})
            }

        # 4) Connect to PostgreSQL with dict_row factory
        conn = psycopg.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port,
            row_factory=dict_row
        )
        cur = conn.cursor()

        # 5) Execute the SQL
        cur.execute(sql)

        # 6) Commit if it's not a SELECT
        if not sql.lstrip().lower().startswith("select"):
            conn.commit()

        # 7) Fetch results for SELECT, or rowcount for DML
        if cur.description:
            result = cur.fetchall()
        else:
            result = [{"rowcount": cur.rowcount}]

        # 8) Clean up
        cur.close()
        conn.close()

        # 9) Return JSON, serializing Decimals as strings
        return {
            "statusCode": 200,
            "body": json.dumps(result, default=str)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
