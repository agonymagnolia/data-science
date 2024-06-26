from pandas import notna

from data_model_classes import (
    IdentifiableEntity,
    Person,
    CulturalHeritageObject,
    NauticalChart,
    ManuscriptPlate,
    ManuscriptVolume,
    PrintedVolume,
    PrintedMaterial,
    Herbarium,
    Specimen,
    Painting,
    Model,
    Map,
    Activity,
    Acquisition,
    Processing,
    Modelling,
    Optimising,
    Exporting,
)

from handlers import (
    ProcessDataUploadHandler,
    MetadataUploadHandler,
    ProcessDataQueryHandler,
    MetadataQueryHandler,
)

# -----------------------------------------------------------------------------

class BasicMashup:
    def __init__(self):
        self.metadataQuery = list()
        self.processQuery = list()

    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery = list()
        return True

    def cleanProcessHandlers(self) -> bool:
        self.processQuery = list()
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:
        self.processQuery.append(handler)
        return True

    def getEntityById(self, identifier: str) -> IdentifiableEntity | None: # Francesca
        for metadataQueryHandler in self.metadataQuery:
            df = metadataQueryHandler.getById(identifier)

            if df.empty:
                continue

            elif 'class' in df:
                obj_class = eval(df['class'][0])
                data = df.loc[0, 'identifier':'date'].dropna()
                hasAuthor = []
                if notna(df['hasAuthor'][0]):
                    for identifier, name in zip(df['identifier_p'], df['name']):
                        hasAuthor.append(Person(identifier=identifier, name=name))

                return obj_class(**data, hasAuthor=hasAuthor)

            else:
                data = df.loc[0, 'identifier':'name'].dropna()
                return Person(**data)

        else:
            return None

    def getAllPeople(self) -> list[Person]: # Francesca
        result = list()

        for metadataQueryHandler in self.metadataQuery:
            df = metadataQueryHandler.getAllPeople()
            if df.empty:
                continue
            else:
                result += [Person(identifier=identifier, name=name) for identifier, name in zip(df['identifier'], df['name'])]

        return result

    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]: # Francesca
        result = list()
        
        for metadataQueryHandler in self.metadataQuery:
            df = metadataQueryHandler.getAllCulturalHeritageObjects()
            obj_id = ''
            for row in df.to_numpy(dtype=object, na_value=None):
                if obj_id != row[0]:
                    obj_id = row[0]
                    obj_class = eval(row[1])
                    result.append(obj_class(identifier=row[2], title=row[3], owner=row[4], place=row[5], date=row[6]))

                if row[7]:
                    author = Person(identifier=row[8], name=row[9])
                    result[-1].hasAuthor.append(author)

        return result

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[Person]: # Francesca
        pass

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> list[CulturalHeritageObject]: # Francesca
        pass

    def getAllActivities(self) -> list[Activity]: # Anna
        pass

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]: # Anna
        pass

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]: # Anna
        pass

    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]: # Anna
        pass

    def getActivitiesStartedAfter(self, date: str) -> list[Activity]: # Anna
        pass

    def getActivitiesEndedBefore(self, date:str) -> list[Activity]: # Anna
        pass

    def getAcquisitionsByTechnique(self, partialName: str) -> list[Acquisition]: # Anna
        pass






class AdvancedMashup(BasicMashup): # Anna
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
        pass

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        pass

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
        pass

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:
        pass