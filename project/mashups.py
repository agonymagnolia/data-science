from pandas import DataFrame

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

    def _to_person(self, df: DataFrame) -> list[Person]:
        # Unpack values from the row and feed them to the class cosntructor
        return [Person(*row) for row in df.to_numpy(dtype=object, na_value=None)]

    def _to_cho(self, df: DataFrame) -> list[CulturalHeritageObject]:
        result = list()

        # Variable to track the current object identifier
        obj_id = ''

        # Unpack values from each row and feed them to the class constructor.
        # NaN values are turned to None and handled accordingly by the class
        # __init__. Attribute hasAuthor is aways initialised as an empty list
        # and populated after the object creation.
        for row in df.to_numpy(dtype=object, na_value=None):
            # If the identifier is the same of the previous row, do not
            # recreate the object but only append the author to the hasAuthor
            # list of the last created object
            if obj_id != row[1]:
                obj_id = row[1]

                # Retrieve the constructor of the class from its name
                obj_class = eval(row[0])

                # Create a new object and append it to the result list
                result.append(obj_class(*row[1:6]))
            
            if row[6]:
                result[-1].hasAuthor.append(Person(*row[6:8]))

        return result

    def _to_activity(self, df: DataFrame) -> list[Activity]: # Lin
        pass

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
        # For search queries the iteration on the handler instances stops
        # at the first result found, while in the getAll methods the results
        # are aggregated
        for handler in self.metadataQuery:
            df = handler.getById(identifier)

            if df.empty:
                continue
            elif 'class' in df: # The result is an object
                return self._to_cho(df)[0]
            else:
                return self._to_person(df)[0]

        else:
            return None

    def getAllPeople(self) -> list[Person]: # Francesca
        result = list()
        for handler in self.metadataQuery:
            df = handler.getAllPeople()

            if df.empty:
                continue
            else:
                result += self._to_person(df)

        # Manage duplicate and/or unordered metadata query handlers
        if len(self.metadataQuery) > 1:
            return sorted(list(set(result)))
        else:
            return result

    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]: # Francesca
        result = list()
        for handler in self.metadataQuery:
            df = handler.getAllCulturalHeritageObjects()

            if df.empty:
                continue
            else:
                result += self._to_cho(df)

        # Manage duplicate and/or unordered metadata query handlers
        if len(self.metadataQuery) > 1:
            return sorted(list(set(result)))
        else:
            return result

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[Person]: # Francesca
        for handler in self.metadataQuery:
            df = handler.getAuthorsOfCulturalHeritageObject(objectId)

            if df.empty:
                continue
            else:
                return self._to_person(df)

        else:
            return list()

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> list[CulturalHeritageObject]: # Francesca
        for handler in self.metadataQuery:
            df = handler.getCulturalHeritageObjectsAuthoredBy(personId)

            if df.empty:
                continue
            else:
                return self._to_cho(df)

        else:
            return list()

    def getAllActivities(self) -> list[Activity]: # Lin
        pass

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]: # Lin
        pass

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]: # Lin
        pass

    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]: # Lin
        pass

    def getActivitiesStartedAfter(self, date: str) -> list[Activity]: # Lin
        pass

    def getActivitiesEndedBefore(self, date:str) -> list[Activity]: # Lin
        pass

    def getAcquisitionsByTechnique(self, partialName: str) -> list[Acquisition]: # Lin
        pass






class AdvancedMashup(BasicMashup): # Lin
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
        pass

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        pass

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
        pass

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:
        pass