import pandas as pd 
from scripts.api_request import csvToJson, callSync, callAsync 

df = pd.read_csv("./test/test.csv") 

callSync(df, 'test_result')