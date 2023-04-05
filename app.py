import streamlit as st 
import pandas as pd 
from scripts.api_request import createApiKey, callSync, callAsync, csvToJson
from utils.columnTest import fillMissingColumns, missingColumns
from utils.exceptions import InvalidFileException
from utils.envUtils import createEnvFile

st.set_page_config(layout="wide")

def test_columns(df:pd.DataFrame):

    if "description" in df.columns.values:
        return True
    else:
        raise InvalidFileException()
    
def dropDuplicates(df:pd.DataFrame) -> pd.DataFrame:

    df_unique = df.loc[:, ['description']]

    df_unique.drop_duplicates(inplace=True)

    return df_unique 
    
def checkUser(client_id, client_secret, x_api_key:str):
    st.session_state['authenticated'] = True
    with st.sidebar:
        st.success('Token Generated!', icon = "✅")

def checkData(df:pd.DataFrame):

        if test_columns(df):

            st.success("File Uploaded!")

            st.write(df.head()) 

            return True

def processData(df_unique:pd.DataFrame) -> pd.DataFrame:

    if missingColumns(df_unique)["bool"] is True:

        df_filled = fillMissingColumns(df_unique)['DataFrame']

        return df_filled
    
def numberRequests(df:pd.DataFrame) -> int:
    
    df_unique = dropDuplicates(df) 

    if df_unique.shape[0] > 100:

        chunks = df.shape[0] / 100 

        if df.shape[0] % 100 != 0:
            chunks += 1

        return int(chunks)
        
    else:
        return int(1)
    
def createMapDicts(df:pd.DataFrame) -> dict():

    mapCols = ["category", "merchant.name", "merchant.businessName", "merchant.cnpj", "merchant"]

    map_dicts = {} 

    for col in mapCols:
        map_dict = dict(zip(df.description, df[col])) 
        map_dicts[col] = map_dict 
    
    return map_dicts

def mapResultColumns(categorizedDf: pd.DataFrame, mappingDf:pd.DataFrame) -> pd.DataFrame:

    map_dicts = createMapDicts(categorizedDf) 

    keys = map_dicts.keys() 

    for key in keys:
        mappingDf[str(key)] = mappingDf['description'].map(map_dicts[key])

    return mappingDf

if __name__ == "__main__":

    ## SESSION STATE VARIABLE 
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False

    if 'x-api-key' not in st.session_state:
        st.session_state['x-api-key'] = None

    st.markdown("![](https://emoji.slack-edge.com/TP18SFFM5/pluggy/aa88fa984d4d9448.png)\n # Pluggy Categorizer App :tada:")

    ## TEXT SIDEBAR
    st.sidebar.header("Client Information :memo:") 
    client_id = st.sidebar.text_input('**Client ID**') 
    client_secret = st.sidebar.text_input('**Client Secret**', type='password')
    client_user_id = st.sidebar.text_input('Client User ID (optional)', type="default")

    st.markdown(":one: Authenticate yourself with a valid **Client ID** and a **Client Secret**")
    st.markdown(":two: Upload the csv file that you want to categorize using **Pluggy Enrichment API**")
    st.markdown(":three: Download the categorized file! :sunglasses:")
    st.markdown(":exclamation: The file must contain at least a ***description*** column")

    ## AUTH
    if st.sidebar.button("Authenticate"):
        X_API_KEY = createApiKey(client_id, client_secret)
        if X_API_KEY:
            st.session_state['x-api-key'] = X_API_KEY
            checkUser(client_id, client_secret, x_api_key=X_API_KEY)

    if st.session_state['authenticated'] == True:

        uploaded_csv = st.file_uploader(label="Upload the file to be categorized", type=['csv'], help="Files larger than 200Mb are not supported.")

        if uploaded_csv:

            df_raw = pd.read_csv(uploaded_csv)

            if checkData(df_raw):

                df_unique = dropDuplicates(df_raw)

                df_unique_filled = processData(df_unique)

    ## SYNCRONOUS/ASSYNC REQUESTS
                
                st.write(f":rocket: **Number of Requests**: {numberRequests(df_unique)}")

                placeholder = st.empty()

                if placeholder.button("Categorize file"):

                    with st.spinner("Categorizing your file..."):
                        
                        placeholder.empty()

                        array = csvToJson(df_unique_filled) 

                        if numberRequests(df_unique) > 200:
                            callAsync(df_unique_filled, fileFinalName="result", numberofRequests=numberRequests(df_unique), x_api_key=st.session_state['x-api-key'])

                        else:
                            callSync(df_unique_filled, fileFinalName="result", x_api_key=st.session_state['x-api-key'])

                    st.success('File categorized!')

                    df_categorized = pd.read_csv("result.csv")

                    final_df = mapResultColumns(df_categorized, df_raw)

                    st.write(final_df.head())

                    final_csv = df_raw.to_csv().encode('utf-8')

                    st.download_button(
                    label="Download Categorized CSV",
                    data=final_csv,
                    file_name="result.csv",
                    mime='text/csv'
                )