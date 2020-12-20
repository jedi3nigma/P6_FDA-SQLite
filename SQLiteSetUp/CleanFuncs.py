import pandas as pd

def BasicClean(df):
    tmp_df = df.copy(deep=True)
    tmp_df = tmp_df.fillna('').applymap(lambda x: x.strip() if isinstance(x, str) else x)
    tmp_df = tmp_df.replace('Null', '')
    
    return tmp_df
    
def ConvDateFormat(df, *cols):
    tmp_df = df.copy(deep=True)
    
    for col in cols:
        tmp_df[col] = tmp_df[col].apply(lambda x: pd.to_datetime(x, format='%Y/%m/%d'))
    
    return tmp_df

def GetSplitFromCol(string, delimiter, get_itm_idx, replacement=''):
#     print(string)
    try:
        result = string.split(delimiter)[get_itm_idx] if len(string.split(delimiter))>1 else replacement
        return result
    except Exception as err:
        return string

def RearrangeCol(df, target_col, adj_col):
    tmp = df.copy(deep=True)
    col_list = tmp.columns.tolist()
    last_item = col_list.pop(col_list.index(target_col))
    col_list.insert(col_list.index(adj_col), last_item)
    return tmp[col_list]