import psycopg2
from lookups import ErrorHandling, InputTypes
from logging_handler import show_error_message
import pandas as pd

config_dict = {
    "database": "dvd_rental",
    "host":"localhost",
    "port":5432,
    "user":"postgres",
    "password": "admin"
}

def create_connection():
    db_session = None
    try:
        db_session = psycopg2.connect(**config_dict)
    except Exception as singer:
        error_string_prefix = ErrorHandling.DB_CONNECT_ERROR.value
        error_string_suffix = str(singer)
        show_error_message(error_string_prefix, error_string_suffix)
    finally:
        return db_session
    
def return_query(db_session,query):
    results = None
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        db_session.commit()
    except Exception as hadi:
        error_string_prefix = ErrorHandling.DB_RETURN_QUERY_ERROR.value
        error_string_suffix = str(hadi)
        show_error_message(error_string_prefix, error_string_suffix)
    finally:
        return results
    
def return_data_as_df(file_executor, input_type, db_session = None):
    return_dataframe = None
    try:
        if input_type == InputTypes.CSV:
            return_dataframe = pd.read_csv(file_executor)
        elif input_type == InputTypes.EXCEL:
            return_dataframe = pd.read_excel(file_executor)
        elif input_type == InputTypes.SQL:
            return_dataframe = pd.read_sql_query(con= db_session, sql= file_executor)
        else:
            raise Exception("The file type does not exist, please check main function")
    except Exception as ola:
        suffix = str(ola)
        if input_type == InputTypes.CSV:
            error_prefix = ErrorHandling.RETURN_DATA_CSV_ERROR.value
        elif input_type == InputTypes.EXCEL:
            error_prefix = ErrorHandling.RETURN_DATA_EXCEL_ERROR.value
        elif input_type == InputTypes.SQL:
            error_prefix = ErrorHandling.RETURN_DATA_SQL_ERROR.value
        else:
            error_prefix = ErrorHandling.RETURN_DATA_UNDEFINED_ERROR.value
        show_error_message(error_prefix, suffix)
    finally:
        return return_dataframe

def execute_query(db_session, query):
    return_val = ErrorHandling.NO_ERROR
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        db_session.commit()
    except Exception as zahraa:
        error_prefix = ErrorHandling.EXECUTE_QUERY_ERROR
        return_val = error_prefix
        suffix = str(zahraa)
        show_error_message(error_prefix.value, suffix)
    finally:
        return return_val
    
# error handling + logging missing
def return_create_statement_from_df(dataframe,schema_name, table_name):
    type_mapping = {
        'int64':'INT',
        'float64':'FLOAT',
        'datetime64[ns]': 'TIMESTAMP',
        'bool':'BOOLEAN',
        'object': 'TEXT'
    }
    fields = []
    for column, dtype in dataframe.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        fields.append(f"{column} {sql_type}")
    
    create_table_statemnt = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"
    create_table_statemnt += ",\n".join(fields)
    create_table_statemnt += "\n);"
    return create_table_statemnt

# error handling + logging missing
def return_insert_into_sql_statement_from_df(dataframe, schema_name, table_name):
    columns = ', '.join(dataframe.columns)
    insert_statement_list = []
    for _ , row in dataframe.iterrows():
        value_strs = []
        for val in row.values:
            if pd.isna(val):
                value_strs.append("NULL")
            elif isinstance(val, (str)):
                value_strs.append(f"'{val}'")
            else:
                value_strs.append(str(val))
        values = ', '.join(value_strs)
        insert_statement = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values});"
        insert_statement_list.append(insert_statement)
    return insert_statement_list

def close_connection(db_session):
    db_session.close()