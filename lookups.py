from enum import Enum


class ErrorHandling(Enum):
    DB_CONNECT_ERROR = "DB Connect Error"
    DB_RETURN_QUERY_ERROR = "DB Return Query Error"
    API_ERROR = "Error calling API"
    RETURN_DATA_CSV_ERROR = "Error returning CSV"
    RETURN_DATA_EXCEL_ERROR = "Error returning Excel"
    RETURN_DATA_SQL_ERROR = "Error returning SQL"
    RETURN_DATA_UNDEFINED_ERROR = "Cannot find File type"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    NO_ERROR = "No Errors"
    PREHOOK_SQL_ERROR = "Prehook: SQL Error"

class InputTypes(Enum):
    SQL = ".sql"
    CSV = ".csv"
    EXCEL = ".xlsx"
    
class PreHookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_folder (sql)"
    EXECUTE_CSV_QUERY = "execute_folder (csv)"
    CREATE_STG_TABLE = "create_sql_staging_tables"

class SourceName(Enum):
    DVD_RENTAL = "dvd_rental"
    COLLEGE = "college"
    POWERBI = "powerbi"

class SQLTablesToReplicate(Enum):
    RENTAL = "dvd_rental.rental"
    FILM = "dvd_rental.film"
    STUDENTS = "college.student"
    DIM_CUSTOMER = "powerbi.Dim_Customer"
    DIM_LOCATION = "powerbi.Dim_Location"
    DIM_PRODUCT = "powerbi.Dim_Product"
    DIM_SALESPERSON = "powerbi.Dim_SalesPerson"
    FCT_PURCHASE = "powerbi.Fct_Purchase"

class DirectoryPaths(Enum):
    SQL_FOLDER = "./SQL_Commands"
    CSV_FOLDER = "./CSV_Files"
