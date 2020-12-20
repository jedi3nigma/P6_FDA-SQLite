import os
import sys
import pandas as pd
import numpy as np
import sqlite3 as s3
import psycopg2 as pg2
from psycopg2 import sql as psql
from psycopg2.extensions import register_adapter, AsIs

pg2.extensions.register_adapter(np.int64, pg2._psycopg.AsIs)

class SQLiteDB():
    
    def __init__(self, dbname, pathname=None):
        self.dbname = str(dbname)
        
        if pathname:
            self.pathname = os.path.abspath(os.path.join(pathname, dbname))
        else:
            self.pathname = os.path.join(os.getcwd(), dbname)
        
    def __repr__(self):
        return 'DBName: {} - found at: {}'.format(self.dbname, self.pathname)

    def DBConnect(self):
        try:
            self.conn = s3.connect(self.pathname, detect_types=s3.PARSE_DECLTYPES | s3.PARSE_COLNAMES)
            self.cursor = self.conn.cursor()
            print('Database is set up for: {}'.format(self.dbname))
#             return conn, cursor
        
        except s3.Error as err:
            print('Cannot connect to database.  Error triggered: {}'.format(err))
        
    def DBClose(self):
        print('Database has been closed.')
        return self.conn.close()
    
    def CreateTable(self, df, tab_name, **kwargs):
        col_data_dict = self.SetSQLTabDataType(df)
        prim_null_attrib, forkey_attrib = self.GetSQLTabColAttrib(df, **kwargs)
        forkey_query_str = ''
        
        query_str = f'CREATE TABLE IF NOT EXISTS {tab_name} (\n'
        for col, dtype in col_data_dict.items():
            tmp_str = f'{col} {dtype} {prim_null_attrib[col]},\n'
            query_str += tmp_str
        
            if col in forkey_attrib.keys():
                forkey_query_str += f'{forkey_attrib[col]},\n'
        
        if forkey_query_str != '':
            query_str += forkey_query_str
                
        query_str = query_str[:-2] + '\n);'
        
#         print(query_str)
        
        self.cursor.execute(query_str)
        self.conn.commit()
        print('Table made for: {}'.format(tab_name))
    
    def InsertData(self, df, tab_name):
        col_names = ', '.join(df.columns.tolist())
        val_input_count = ', '.join(['?'] * len(df.columns.tolist()))
        # print(col_names)
        # print(val_input_count)
        try:
            data = df.itertuples(index=False, name=None)
            self.conn.executemany('INSERT OR IGNORE INTO {} ({}) VALUES ({})'.format(tab_name, col_names, val_input_count), data)
            self.conn.commit()
            print('Done data insert into {}'.format(tab_name))
        except s3.Error as err:
            print(err)
    
    @staticmethod
    def SetSQLTabDataType(df):
        col_type = dict()
        for col in df.dtypes.index.tolist():
            if 'int' in str(df.dtypes[col]):
                sql_type = 'INTEGER'
            elif 'float' in str(df.dtypes[col]):
                sql_type = 'REAL'
            elif 'object' in str(df.dtypes[col]):
                sql_type = 'TEXT'
            elif 'datetime' in str(df.dtypes[col]):
                sql_type = 'INTEGER'
            else:
                sql_type = 'NULL'

            col_type[col] = sql_type
#             print(col, sql_type)
        return col_type
    
    @staticmethod
    def GetSQLTabColAttrib(df, **kwargs):
        '''
        attributes:
        (1) 'prim null': assigns primary key and not null restrictions
        (2) 'foreign key': (optional) assigns foreign key with reference to specific column of another table
        '''
        prim_null_dict, for_key_dict = dict(), dict()
        
        col_list = df.columns.tolist()
        prim_key = kwargs.get('prim_key', '')
        for_key = kwargs.get('for_key', '')

        for k in kwargs.keys():
            if 'prim null' in kwargs[k]:
                tmp_keys, tmp_value = list(), list()
                tmp_df = df.isnull().sum()
                nonull_list = tmp_df[tmp_df == 0].index.tolist()

                for col in col_list:
                    tmp_keys.append(col)
                    if prim_key == col:
                        tmp_value.append('PRIMARY KEY')
                    elif col in nonull_list:
                        tmp_value.append('NOT NULL')
                    else:
                        tmp_value.append('')

                prim_null_dict = dict(zip(tmp_keys, tmp_value))

            elif 'foreign key' in kwargs[k] == 'foreign key' and not for_key == '':
                for key, ref_tab_tup in for_key.items():
                    tab_name, tab_id = ref_tab_tup

                    for_key_dict = {key: f'FOREIGN KEY ({key}) REFERENCES {tab_name} ({tab_id})'}
        
#         print('Done getting attributes')
        return prim_null_dict, for_key_dict

class PostgresDB():

    def __init__(self, **kwargs):
        self.host = kwargs.get('host', '')
        self.database = kwargs.get('database', '')
        self.user = kwargs.get('user', '')
        self.password = kwargs.get('password', '')
        self.port = kwargs.get('port', '')
        
    def __repr__(self):
        return f'''PostgreSQL database credentials:
                    host: {self.host}
                    database: {self.database}
                    user: {self.user}
                    password: ******
                    port: {self.port}
                '''

    # database functions
    def ConnectPGSQL(self):
        self.conn = None
        try:
            print('Connecting to the PostgreSQL database...')
            self.conn = pg2.connect(host=self.host, 
                                    database=self.database,
                                    user=self.user,
                                    password=self.password,
                                    port=self.port
                                    )
            self.cursor = self.conn.cursor()
        except (Exception, pg2.DatabaseError) as error:
            print(error)
            print('Unable to connect to database')
            sys.exit(1) 
        print("Connection successful")
        # return self.conn  

    def ExecuteQuery(self, query):
        ret = True
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except (Exception, pg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            # self.CloseCursor()
            return False

        if 'select' in query.lower():
            ret = self.cursor.fetchall()
            # self.CloseCursor()
        return ret

    def SetSchema(self, schema_name):
        schema_query = f'''
                SELECT schema_name FROM information_schema.schemata;
                ''' 
        result = self.ExecuteQuery(schema_query)
#         print(result)
        if not isinstance(result, bool):
            result = [r[0] for r in result]
            if schema_name.lower() in result:
                self.schema = schema_name
                print(f'{self.schema} is set.')
            else:
                schema_query = f'''
                    CREATE SCHEMA IF NOT EXISTS {schema_name};
                    '''
                result = self.ExecuteQuery(schema_query)
#                 print(result)
                if result:
                    self.schema = schema_name
                    print(f'{self.schema} is created.')
                else:
                    print(f'Schema cannot be created.')

    def BulkInsertRecords(self, df, table_name):
        # Create a list of tuples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL query to execute
        values = [self.cursor.mogrify("({})".format(','.join(['%s']*len(tup))), tup).decode('utf8') for tup in tuples]
        query  = "INSERT INTO {} ({}) VALUES {};".format('.'.join([self.schema, table_name.lower()]), cols, ",".join(values))
        
        try:
            self.cursor.execute(query, tuples)
            self.conn.commit()
            print("execute_mogrify() done")
        except (Exception, pg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            # self.CloseCursor()

        # self.CloseCursor()

    def CloseCursor(self):
        return self.cursor.close()

    def CloseConnect(self):
        return self.conn.close()

    def CreateTable(self, df, tab_name, **kwargs):
        col_data_dict = self.SetSQLTabDataType(df)
        prim_null_attrib, forkey_attrib = self.GetSQLTabColAttrib(df, col_data_dict, **kwargs)
        forkey_query_str = ''

        query_str = f"CREATE TABLE IF NOT EXISTS {'.'.join([self.schema, tab_name])} (\n"
        for col, dtype in col_data_dict.items():
            tmp_str = f'{col} {dtype} {prim_null_attrib[col]},\n'
            query_str += tmp_str
        
            if col in forkey_attrib.keys():
                forkey_query_str += f'{forkey_attrib[col]},\n'
        
        if forkey_query_str != '':
            query_str += forkey_query_str
                
        query_str = query_str[:-2] + '\n);'
        self.cursor.execute(query_str)
        self.conn.commit()
        print('Table made for: {}'.format(tab_name))

    @staticmethod
    def SetSQLTabDataType(df):
        col_type = dict()
        for col in df.dtypes.index.tolist():
            if 'int' in str(df.dtypes[col]):
                sql_type = 'NUMERIC'
            elif 'float' in str(df.dtypes[col]):
                sql_type = 'REAL'
            elif 'object' in str(df.dtypes[col]):
                sql_type = 'TEXT'
            elif 'datetime' in str(df.dtypes[col]):
                sql_type = 'TIMESTAMP'
            else:
                sql_type = 'NULL'

            col_type[col] = sql_type
#             print(col, sql_type)
        return col_type

    @staticmethod
    def GetSQLTabColAttrib(df, col_att_dict, **kwargs):
        '''
        attributes:
        (1) 'prim null': assigns primary key and not null restrictions
        (2) 'foreign key': (optional) assigns foreign key with reference to specific column of another table
        '''
        prim_null_dict, for_key_dict = dict(), dict()
        # col_list = df.columns.tolist()
        prim_key = kwargs.get('prim_key', '')
        for_key = kwargs.get('for_key', '')
        ref_tab_tup = kwargs.get('ref_tab_tup', '')

        for k, v in kwargs.items():
            if v.lower() == 'prim null':
#                 if prim_key == '':
#                     print('No primary key found')
#                     continue
#                 else:
                tmp_keys, tmp_value = list(), list()
                tmp_df = df.isnull().sum()
                nonull_list = tmp_df[tmp_df == 0].index.tolist()

                for col in col_att_dict.keys():
                    tmp_keys.append(col)
                    if prim_key == col:
                        tmp_value.append('PRIMARY KEY')
                    elif col in nonull_list:
                        tmp_value.append('NOT NULL')
                    else:
                        tmp_value.append('')

                prim_null_dict = dict(zip(tmp_keys, tmp_value))

            elif v.lower() == 'foreign key' and not for_key == '':
#                 if for_key == '':
#                     print('No foreign key found')
#                     continue
#                 else:
#                 if for_key in col_att_dict.keys() and not ref_tab_tup == '':
                for key, ref_tab_tup in for_key.items():
                    tab_name, tab_id = ref_tab_tup

                    for_key_dict = {for_key: f'FOREIGN KEY {key} REFERENCES {tab_name} ({tab_id})'}
             
        return prim_null_dict, for_key_dict