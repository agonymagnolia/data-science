import data_model_classes
from data_model_classes import *
from handlers import ProcessDataUploadHandler, MetadataUploadHandler, ProcessDataQueryHandler, MetadataQueryHandler
from pandas import notna


class BasicMashup:
    def __init__(self):
        self.metadataQuery = []
        self.processQuery = []

    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery = []
        return True

    def cleanProcessHandlers(self) -> bool:
        self.processQuery = []
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:
        self.processQuery.append(handler)
        return True

    def getEntityById(self, identifier: str) -> IdentifiableEntity | None:
        if not self.metadataQuery:
            return None

        df = self.metadataQuery[0].getById(identifier)

        if df.empty:
            return None
        elif 'class' in df:
            className = df['class'][0]
            objClass = globals()[className]
            data = df.loc[0, 'identifier':'date'].dropna()
            hasAuthor = []
            if notna(df['hasAuthor'][0]):
                for identifier, name in zip(df['identifier_p'], df['name']):
                    hasAuthor.append(Person(identifier=identifier, name=name))
                    

            return objClass(**data, hasAuthor=hasAuthor)
        else:
            data = df.loc[0, 'identifier':'name'].dropna()
            return Person(**data)

    def getAllPeople(self) -> list[Person]:
        result = list()

        if self.metadataQuery:
            df = self.metadataQuery[0].getAllPeople()
            result = [Person(identifier=identifier, name=name) for identifier, name in zip(df['identifier'], df['name'])]

        return result

    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:
        result = list()
        
        if not self.metadataQuery:
            return result

        df = self.metadataQuery[0].getAllCulturalHeritageObjects()
        objId = ''
        for row in df.to_numpy(dtype=object, na_value=None):
            if objId != row[0]:
                objId = row[0]
                objClass = vars(data_model_classes)[row[1]]
                result.append(objClass(identifier=str(row[2]), title=row[3], owner=row[4], place=row[5], date=row[6]))

            if row[7]:
                person = Person(identifier=row[8], name=row[9])
                result[-1].hasAuthor.append(person)

        return result


class AdvancedMashup(BasicMashup):
    pass