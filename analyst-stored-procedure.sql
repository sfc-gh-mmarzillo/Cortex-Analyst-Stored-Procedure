REATE OR REPLACE PROCEDURE CORTEX_QUERY_EXECUTE(USER_QUERY STRING)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'cortex_query_handler'
AS
$$
import _snowflake
import json
from snowflake.snowpark.context import get_active_session

# Hardcoded context
DATABASE = "SALES_INTELLIGENCE"
SCHEMA = "DATA"
SEMANTIC_MODEL = "SALES_METRICS_MODEL"

def send_message(prompt: str) -> dict:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_view": f"{DATABASE}.{SCHEMA}.{SEMANTIC_MODEL}",
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )

def cortex_query_handler(user_query):
    """
    Handler function that gets SQL from Cortex Analyst and executes it.
    Returns results as a Snowpark DataFrame for table output.
    """
    try:
        session = get_active_session()
        
        # Get the Cortex Analyst response using the working function
        response = send_message(user_query)
        
        # Extract SQL from the response
        sql_statement = None
        for content_item in response["message"]["content"]:
            if content_item.get("type") == "sql":
                sql_statement = content_item.get("statement")
                break
        
        if not sql_statement:
            # Return error as a DataFrame table
            return session.create_dataframe([{"error": "No SQL statement found in Cortex Analyst response"}])
        
        # Execute the SQL and return results as DataFrame
        result_df = session.sql(sql_statement)
        return result_df
        
    except Exception as e:
        # Return error as a DataFrame table
        session = get_active_session()
        return session.create_dataframe([{"error": f"Error: {str(e)}"}])
$$;

-- Usage Examples:

-- Simple usage - returns results just like a SELECT statement
CALL CORTEX_QUERY_EXECUTE('How many deals did each sales rep close?');
