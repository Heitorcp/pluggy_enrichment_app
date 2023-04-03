class InvalidFileException(Exception):
    """
    Raised when the input csv file does not match the Pluggy Enrichment API requirements
    """ 
    
    def __init__(self, message="Invalid CSV File: Your file must contain a column named **description**.") -> None:
        self.message=message
        super().__init__(self.message)

class NotAuthenticatedUser(Exception):
    """
    Raised when a non-authenticated user tries to call the Pluggy Enrichment API
    """

    def __init__(self, message="User not authenticated. Make sure you have provided a valid ClientId and ClientSecret and hit the button Authenticate") -> None:
        self.message=message
        super().__init__(self.message) 

class InvalidFutureResponse(Exception):

    def __init__(self, message="The future type received is not a DataFrame") -> None:
        self.message = message
        super().__init__(self.message)