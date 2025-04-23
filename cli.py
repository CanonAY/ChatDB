#!/usr/bin/env python3
import argparse
import os
import time
import requests
import sys
from tabulate import tabulate
from typing import Dict, List
import threading

TITLE_ART = """
.██████╗██╗  ██╗ █████╗ ████████╗██████╗ ██████╗.
██╔════╝██║  ██║██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗
██║     ███████║███████║   ██║   ██║  ██║██████╔╝
██║     ██╔══██║██╔══██║   ██║   ██║  ██║██╔══██╗
╚██████╗██║  ██║██║  ██║   ██║   ██████╔╝██████╔╝
.╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═════╝ ╚═════╝.
"""

def clear_screen():
    """Clear the terminal screen based on the operating system."""
    os.system('cls' if os.name == 'nt' else 'clear')

def display_animated_title():
    """Display the title art with a line-by-line animation effect."""
    lines = TITLE_ART.strip().split('\n')
    clear_screen()
    for i in range(0, len(lines), 2):
        clear_screen()
        for j in range(i + 2):
            print(lines[j])
        time.sleep(0.15)
    print("\nWelcome to the Natural Language to SQL Query CLI!")
    print("Type 'q' to exit the program.\n")

class SQLQueryCLI:
    """Command Line Interface for Natural Language to SQL Query System."""
    
    def __init__(self):
        """Initialize the CLI with API endpoints and headers."""
        self.nl2sql_url = "https://u1gds316me.execute-api.us-east-2.amazonaws.com/v1/nl2sql"
        self.exec_sql_url = "https://u1gds316me.execute-api.us-east-2.amazonaws.com/v1/exec_sql"
        self.headers = {"Content-Type": "application/json"}
        self.db_params = {}
        self.loading = False

    def animate_loading(self, prefix_message: str):
        """
        Display an animated loading indicator with dots.
        
        Args:
            prefix_message: The message to display before the loading dots
        """
        while self.loading:
            for i in range(13):
                if not self.loading:
                    break
                dots = '.' * i
                spaces = ' ' * (12 - i)
                print(f"\r{prefix_message}{dots}{spaces}", end='', flush=True)
                time.sleep(0.12)
            
            for i in range(12, -1, -1):
                if not self.loading:
                    break
                dots = '.' * i
                spaces = ' ' * (12 - i)
                print(f"\r{prefix_message}{dots}{spaces}", end='', flush=True)
                time.sleep(0.04)
        
        print("\r" + " " * (len(prefix_message) + 12), end='\r')

    def with_loading_animation(self, operation, message: str):
        """
        Execute an operation while displaying a loading animation.
        
        Args:
            operation: The function to execute
            message: The message to display during loading
            
        Returns:
            The result of the operation
        """
        self.loading = True
        animation_thread = threading.Thread(target=self.animate_loading, args=(message,))
        animation_thread.start()
        
        try:
            result = operation()
        finally:
            self.loading = False
            animation_thread.join()
        
        return result

    def get_sql_query(self, natural_language_query: str) -> Dict:
        """
        Convert a natural language query to SQL.
        
        Args:
            natural_language_query: The natural language query to convert
            
        Returns:
            Dictionary containing the SQL query and any error messages
            
        Raises:
            SystemExit: If the API request fails
        """
        try:
            payload = {"query": natural_language_query}
            payload.update(self.db_params)
            
            def make_request():
                response = requests.post(
                    self.nl2sql_url,
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                return response.json()
            
            return self.with_loading_animation(make_request, "Converting to SQL")
        except requests.exceptions.RequestException as e:
            print(f"\nError: Failed to convert query to SQL - {str(e)}", file=sys.stderr)
            sys.exit(1)

    def execute_sql(self, sql_query: str) -> List[Dict]:
        """
        Execute a SQL query and return the results.
        
        Args:
            sql_query: The SQL query to execute
            
        Returns:
            List of dictionaries containing the query results
            
        Raises:
            SystemExit: If the API request fails
        """
        try:
            payload = {"query": sql_query}
            payload.update(self.db_params)
            
            def make_request():
                response = requests.post(
                    self.exec_sql_url,
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                result = response.json()
                
                if "error" in result:
                    error_msg = result["error"].split("\n")[0]
                    print(f"\nError executing SQL: {error_msg}", file=sys.stderr)
                    return []
                
                return result
            
            return self.with_loading_animation(make_request, "Executing SQL instruction")
        except requests.exceptions.RequestException as e:
            print(f"\nError: Failed to execute SQL query - {str(e)}", file=sys.stderr)
            sys.exit(1)

    def format_results(self, results: List[Dict]) -> str:
        """
        Format query results as a table.
        
        Args:
            results: List of dictionaries containing query results
            
        Returns:
            Formatted table string
        """
        if not results:
            return "No results found."
        formatted_results = []
        for row in results:
            formatted_row = {k: str(v) for k, v in row.items()}
            formatted_results.append(formatted_row)
        
        return tabulate(formatted_results, headers="keys", tablefmt="grid")

    def run(self):
        """Main CLI workflow."""
        quit_commands = {'quit', 'exit', 'q', '\\q'}
        
        while True:
            print("\nEnter your natural language instruction (or 'q' to exit):")
            query = input("> ").strip()
            
            if query.lower() in quit_commands:
                break
            
            if not query:
                print("Please enter a valid instruction.")
                continue
            
            nl2sql_result = self.get_sql_query(query)
            sql_query = nl2sql_result["sql_query"].strip('"')
            error_reason = nl2sql_result.get("error_reason", "")
            
            if sql_query == "":
                print(f"\nUnable to generate SQL instruction. {error_reason}")
                print("Please try refining your instructions.")
                continue
            
            print("\nGenerated SQL:")
            print("-------------------")
            print(sql_query)
            
            print("\nDo you want to execute this SQL instruction? (yes/no/refine)")
            confirmation = input("> ").lower().strip()
            
            if confirmation == 'no':
                continue
            elif confirmation == 'refine':
                continue
            elif confirmation != 'yes':
                print("Invalid input. Please enter 'yes', 'no', or 'refine'.")
                continue
            
            results = self.execute_sql(sql_query)
            
            print("\nResult:")
            print("-------------")
            print(self.format_results(results))

def main():
    """Entry point for the CLI application."""
    parser = argparse.ArgumentParser(
        description="Natural Language to SQL Query CLI"
    )
    parser.add_argument('--host', help='Database host')
    parser.add_argument('--dbname', help='Database name')
    parser.add_argument('--port', type=int, help='Database port')
    parser.add_argument('--db_user', help='Database username')
    parser.add_argument('--db_password', help='Database password')
    
    args = parser.parse_args()
    
    display_animated_title()
    cli = SQLQueryCLI()
    
    db_params = {
        'host': args.host,
        'dbname': args.dbname,
        'port': args.port,
        'db_user': args.db_user,
        'db_password': args.db_password
    }
    
    cli.db_params = {k: v for k, v in db_params.items() if v is not None}
    
    if not cli.db_params:
        print("Note: No database connection parameters provided.")
        print("Using example database for demonstration purposes.\n")
    
    cli.run()

if __name__ == "__main__":
    main() 