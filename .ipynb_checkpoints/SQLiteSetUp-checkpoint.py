import os
import pandas as pd
import sqlite3 as s3

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
            self.conn = s3.connect(self.pathname)
            self.cursor = self.conn.cursor()
            return self.conn, self.cursor
        
        except s3.Error as err:
            print('Cannot connect to database.  Error triggered: {}'.format(err))
            return
        
    def DBClose(self):
        return self.conn.close()
    
    def CreateTable(self, df, tab_name, *attr, prim_key='', for_key='', ref_tab_tup=None):
        col_data_dict = self.SetSQLTabDataType(df)
        prim_null_attrib, forkey_attrib = self.GetSQLTabColAttrib(df, *attr, prim_key, for_key, ref_tab_tup)
        
        query_str = f'CREATE TABLE {tab_name} (\n'
        for col, dtype in col_data_dict.items():
            tmp_str = f'{col} {dtype} {prim_null_attrib[col]},\n'
            query_str += tmp_str
        
            if col in forkey_attrib.keys():
                forkey_query_str = forkey_attrib[col]
        
        if forkey_query_str:
                query_str += forkey_query_str
                
        query_str = query_str[:-2] + '\n);'
        self.cursor.execute(query_str)
        print('Table made for: {}'.format(tab_name))
    
    def InsertData(self, df, tab_name):
        col_names = ', '.join(df.columns.tolist())
        val_input_count = ', '.join(['?'] * len(df.columns.tolist()))
        try:
            data = df.itertuples(index=False, name=None)
            self.conn.executemany('INSERT OR IGNORE INTO {} ({}) VALUES ({})'.format(tab_name, col_names, val_input_count), data)
            self.conn.commit()
            print('Done data insert into {}'.format(tab_name))
        except s3.Error as err:
            print('Cannot insert one or more data rows into database.')
    
    def SetSQLTabDataType(df):
        col_type = dict()
        for col in df.dtypes.index.tolist():
            if 'int' in str(df.dtypes[col]):
                sql_type = 'INTEGER'
            elif 'float' in str(df.dtypes[col]):
                sql_type = 'REAL'
            elif 'object' in str(df.dtypes[col]):
                sql_type = 'TEXT'
            else:
                sql_type = 'NULL'

            col_type[col] = sql_type
        
        return col_type
    
    def GetSQLTabColAttrib(df, *attr, prim_key='', for_key='', ref_tab_tup=None):
        # get col not null
        col_list = df.columns.tolist()
        if attr.lower() == 'prim null' and not prim_key == '':
            # set nulls and primary key
            if prim_key in col_list:
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
                
                return dict(zip(tmp_keys, tmp_value))
            else:
                print('No primary key found')

        elif attr.lower() == 'foreign key' and not for_key == '' and not ref_tab_tup == None:
            # get foreign key
            if for_key in col_list:
                tab_name, tab_id = ref_tab_tup
                
                return {for_key: f'FOREIGN KEY {for_key} REFERENCES {tab_name} ({tab_id})'}
            else:
                print('No foreign key found')
        else:
            print('Invalid attribute request.')

def BasicClean(df):
    tmp_df = df.copy(deep=True)
    tmp_df = tmp_df.fillna('').applymap(lambda x: x.strip() if isinstance(x, str) else x)
    tmp_df = tmp_df.replace('Null', '')
    
    return tmp_df
    
def ConvDateFormat(df, *args):
    tmp_df = df.copy(deep=True)
    
    for col in args:
        tmp_df[col] = tmp_df[col].apply(lambda x: pd.to_datetime(x, format='%Y/%m/%d'))
    
    return tmp_df