from sqlalchemy import  exc

def schema_id(connection, schema_name=None):
    """    Fetch the id for a given schema.

    This function fetches the id for a specified schema from the database.
    If no schema name is provided, it defaults to the current schema.

    Args:
        connection: Database connection object.
        schema_name (str?): Name of the schema. Defaults to None.

    Returns:
        int: The id of the specified schema.

    Raises:
        exc.InvalidRequestError: If the specified schema does not exist in the database.
    """
    cur=connection.cursor()
    if schema_name is None:
        cur.execute("SELECT current_schema")
        schema_name=cur.fetchall()[0][0]

    query = """
                SELECT id
                FROM sys.schemas
                WHERE name = ?
            """
    
    cur.execute(query, (schema_name, ))

    try:
        schema_id = cur.fetchall()[0][0]
    except:
        raise exc.InvalidRequestError(schema_name)
        
    return schema_id

    
def table_id(connection, table_name, schema_name=None):
    """    Fetch the id for the specified table in the given schema, defaulting to
    the current schema if schema is None.

    Args:
        connection: The database connection object.
        table_name (str): The name of the table for which the id is to be fetched.
        schema_name (str?): The name of the schema. Defaults to None.

    Returns:
        int: The id of the specified table.

    Raises:
        exc.NoSuchTableError: If the specified table does not exist.
    """

    schema_idm = schema_id(connection=connection,schema_name=schema_name)

    q = f"""
        SELECT id
        FROM sys.tables
        WHERE name = ?
        AND schema_id = {schema_idm}
        """
    
    cur = connection.cursor()
    cur.execute(q, (table_name, ))

    try:
        table_id = cur.fetchall()[0][0] 
    except:
        raise exc.NoSuchTableError(table_name)

    

    return table_id
