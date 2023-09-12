import os
from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_create_statement_from_df
from lookups import ErrorHandling, PreHookSteps, SQLTablesToReplicate, InputTypes, SourceName, DirectoryPaths
from logging_handler import show_error_message

def execute_folder(directory_path, input_file_type, db_session, input_schema=None):
    files = [file for file in os.listdir(directory_path.value) if file.endswith(input_file_type.value)] # edited InputTypes in lookups
    sorted_files =  sorted(files)    
    if input_file_type == InputTypes.SQL:
        for sql_file in sorted_files:
            with open(os.path.join(directory_path.value,sql_file), 'r') as sql_file:
                sql_query = sql_file.read()
                return_val = execute_query(db_session= db_session, query= sql_query)
                if not return_val == ErrorHandling.NO_ERROR:
                    raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(sql_file))
    elif input_file_type == InputTypes.CSV:
        for csv_file in sorted_files:
            csv_df = return_data_as_df(file_executor=os.path.join(directory_path.value,csv_file), input_type=InputTypes.CSV)
            old_columns = csv_df.columns
            new_columns = []
            for column in old_columns:
                column = column.replace(' ','_')
                column = column.replace('-','_')
                new_columns.append(column)
            csv_df.columns = new_columns
            table_name = csv_file[:-4] # generating table name by removing '.csv'
            csv_query = return_create_statement_from_df(csv_df,schema_name=input_schema.value,table_name=table_name)
            return_val = execute_query(db_session= db_session, query= csv_query)
            if not return_val == ErrorHandling.NO_ERROR:
                raise Exception(f"{PreHookSteps.EXECUTE_CSV_QUERY.value} = CSV File Error on CSV FILE = {str(csv_file)}" )

# added new schema and tables in lookups, (schema was created manually in pgadmin)
# return_tables_by_schema unchanged.
# handled error in create_sql_staging_tables

def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schema_tables.append(table.split('.')[1])
    return schema_tables

def create_sql_staging_tables(db_session, source_name):
    tables = return_tables_by_schema(source_name)
    for table in tables:
        staging_query = f"""
                SELECT * FROM {source_name}.{table} LIMIT 1
        """
        staging_df = return_data_as_df(db_session= db_session, input_type= InputTypes.SQL, file_executor= staging_query)
        dst_table = f"stg_{source_name}_{table}"
        create_stmt = return_create_statement_from_df(staging_df, 'dw_reporting', dst_table)
        return_val = execute_query(db_session=db_session, query= create_stmt)
        if not return_val == ErrorHandling.NO_ERROR:
            raise Exception(f"{PreHookSteps.CREATE_STG_TABLE.value} = Error creating stg table of : {str(table)}" )

# altered execute_prehook to include both cases, added new enum class in lookups (DirectoyPaths)
def execute_prehook(directory_path,schema):
    try:
        db_session = create_connection()
        if directory_path == DirectoryPaths.SQL_FOLDER:
            # Step 1:
            execute_folder(directory_path, InputTypes.SQL , db_session) 
            # Step 2 getting dvd rental staging:
            create_sql_staging_tables(db_session,SourceName.DVD_RENTAL.value)
        elif directory_path == DirectoryPaths.CSV_FOLDER:
            # Step 1:
            execute_folder(directory_path, InputTypes.CSV , db_session, input_schema=schema) 
            # Step 2 getting powerbi staging tables:
            create_sql_staging_tables(db_session,SourceName.POWERBI.value)
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
