class Handler:
    def __init__(self):
        self.dbPathOrUrl = ''
        
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        if pathOrUrl and isinstance(pathOrUrl, str):
            self.dbPathOrUrl = pathOrUrl
            return True
        else:
            return False

class UploadHandler(Handler):
    def pushDataToDb(self, path: str):
        pass

class QueryHandler(Handler):
    def getDbPathOrUrl(self) -> str:
        if not self.dbPathOrUrl:
            raise Exception('Query path not set.')
        return self.dbPathOrUrl

    def getById(self, id: str):
        pass