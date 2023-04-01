from utils.exceptions import InvalidFileException
import pandas as pd
from datetime import date

def missingColumns(df:pd.DataFrame):

    cols = {'id', 'description', 'amount', 'date'} 
    if set(df.columns.str.lower()) == cols:
        return {"bool":False}
    else:

        missing_cols = []

        for col in cols:
            if col in df.columns.str.lower().to_list():
                pass 
            else:
                missing_cols.append(col)

        if "description" in missing_cols:
            raise InvalidFileException

        return {"missingCols":missing_cols, "bool":True}

def fillMissingColumns(df):

    nrows = df.shape[0]

    missing_columns = missingColumns(df)["missingCols"]

    if "description" in missing_columns:
        raise InvalidFileException()
    
    for col in missing_columns:

        if col == "date":

            today = str(date.today())

            df[col] = pd.Series([today]*nrows)

        elif col == "id":

            df[col] = pd.Series([str(i) for i in range(nrows)]) 

        elif col == "amount":

            df[col] = pd.Series(['100']*nrows)

    return {"DataFrame":df.astype(str), "bool": True}
