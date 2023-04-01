import os
from api_request import createApiKey
import re 

CLIENT_ID = os.environ.get("CLIENT_ID") 
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

def updateEnvFile(client_id:str, client_secret:str): 

    X_API_KEY = createApiKey(client_id, client_secret)

    file_dict = {}

    with open("../.env", 'r') as f:
        lines = f.readlines() 

        for line in lines:
            key, value = line.rstrip().split("=")
            file_dict[key] = value 

        print(file_dict)

        matched = False

        for line in lines:
            x_api_key_match = re.match("X_API_KEY", line) 
            if x_api_key_match:
                matched = True
                print("Updating X_API_KEY")
                file_dict['X_API_KEY'] = X_API_KEY
                
        if matched is False:
            print("No X_API_KEY, creating a new one")
            file_dict['X_API_KEY'] = X_API_KEY 

    with open("../.env", "w") as f: #opening in write mode, erases it 
        for line in file_dict.items():
            f.write(f"{line[0]}={line[1]}\n")
    
if __name__ == "__main__":
    updateEnvFile(CLIENT_ID, CLIENT_SECRET)