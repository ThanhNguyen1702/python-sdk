from mcp.server.fastmcp import FastMCP
import os
import psycopg2
import psycopg2.extras
from typing import List, Dict, Any
import json
# Create an MCP server
mcp = FastMCP("Demo")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "agency"),
    "user": os.getenv("DB_USER", "thanhnd"),
    "password": os.getenv("DB_PASSWORD", "thanh")
}

def get_db_connection():
    """Create a database connection"""
    try:
        # Option 1: Use connection string
        conn_string = f"host={DB_CONFIG['host']} port={DB_CONFIG['port']} dbname={DB_CONFIG['database']} user={DB_CONFIG['user']} password={DB_CONFIG['password']}"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute("SET search_path TO app, public")
        cursor.close()
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")
    

@mcp.tool()
def execute_query(query: str) -> str:
    """
    Execute a SQL query (SELECT, UPDATE, INSERT, DELETE) and return results or status.
    SELECT returns JSON results.
    Other queries return a success message with affected row count.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Check query type
        command = query.strip().split()[0].upper()

        if command == 'SELECT':
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            # Convert to list of dictionaries for JSON serialization
            results_list = [dict(row) for row in results]
            return json.dumps(results_list, indent=2, default=str)

        elif command in ('UPDATE', 'INSERT', 'DELETE'):
            cursor.execute(query)
            affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            return f"Success: {affected} row(s) affected."

        else:
            return "Error: Only SELECT, INSERT, UPDATE, DELETE queries are allowed."

    except Exception as e:
        return f"Error executing query: {str(e)}"

# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b

@mcp.tool()
def greet(name: str) -> str:
    """Get a personalized greeting for a person"""
    return f"Hello, {name}!"

@mcp.tool()
def list_tables() -> str:
    """List all tables in the database"""
    query = """
    SELECT table_name, table_type 
    FROM information_schema.tables 
    WHERE table_schema = 'app' OR table_schema = 'public'
    ORDER BY table_name;
    """
    return execute_query(query)


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"


@mcp.tool()
def count_rows(table_name: str) -> str:
    """Count rows in a specific table"""
    # Basic SQL injection protection
    if not table_name.replace('_', '').replace('-', '').isalnum():
        return "Error: Invalid table name"
    
    query = f"SELECT COUNT(*) as row_count FROM {table_name};"
    return execute_query(query)

@mcp.tool()
def get_table_sample(table_name: str, limit: int = 5) -> str:
    """Get a sample of rows from a table"""
    # Basic SQL injection protection
    if not table_name.replace('_', '').replace('-', '').isalnum():
        return "Error: Invalid table name"
    
    if limit > 100:
        limit = 100  # Prevent huge queries
    
    query = f"SELECT * FROM {table_name} LIMIT {limit};"
    return execute_query(query)

@mcp.tool()
def search_table(table_name: str, column: str, search_term: str, limit: int = 10) -> str:
    """Search for records in a table where a column contains a term"""
    # Basic SQL injection protection
    if not table_name.replace('_', '').replace('-', '').isalnum():
        return "Error: Invalid table name"
    if not column.replace('_', '').replace('-', '').isalnum():
        return "Error: Invalid column name"
    
    if limit > 100:
        limit = 100
    
    query = f"""
    SELECT * FROM {table_name} 
    WHERE {column}::text ILIKE '%{search_term}%' 
    LIMIT {limit};
    """
    return execute_query(query)

@mcp.tool()
def describe_table(table_name: str) -> str:
    """Describe the structure of a specific table"""
    query = f"""
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length
    FROM information_schema.columns 
    WHERE table_name = '{table_name}' AND table_schema = 'public'
    ORDER BY ordinal_position;
    """
    return execute_query(query)


@mcp.resource("schema://{table_name}")
def get_table_schema(table_name: str) -> str:
    """Get detailed schema information for a table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get table structure
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not columns:
            return f"Table '{table_name}' not found"
        
        schema_info = {
            "table_name": table_name,
            "columns": [dict(col) for col in columns]
        }
        
        return json.dumps(schema_info, indent=2, default=str)
    except Exception as e:
        return f"Error getting schema: {str(e)}"



if __name__ == "__main__":
    print("Running MCP server...")
    mcp.run()
