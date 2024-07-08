class Handler:
    def __init__(self):
        self.dbPathOrUrl = ''
        
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return True

class UploadHandler(Handler):
    def pushDataToDb(self, path: str):
        pass

class QueryHandler(Handler):
    def getById(self, id: str):
        pass