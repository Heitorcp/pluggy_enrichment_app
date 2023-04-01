def deleteEnvFile():

    with open("./.env", "w") as f:
        return 

def createEnvFile(CLIENTID:str, CLIENTSECRET:str, XAPIKEY=None):

    if XAPIKEY is not None:
        file_dict = {"CLIENT_ID":CLIENTID, "CLIENT_SECRET":CLIENTSECRET, "X_API_KEY":XAPIKEY}
    else:
        file_dict = {"CLIENT_ID":CLIENTID, "CLIENT_SECRET":CLIENTSECRET}

    with open("./.env", "w") as f:
        for line in file_dict.items():
            f.write(f"{line[0]}={line[1]}\n")
    
    return 
    