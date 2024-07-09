from ..handlers import MetadataQueryHandler, ProcessDataQueryHandler, ACTIVITIES
from ..domain import (
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
    Exporting
)
from ..utils import key

from pandas import DataFrame, concat

class BasicMashup:
    def __init__(self):
        self.metadataQuery = list()
        self.processQuery = list()

    def _to_person(self, df: DataFrame) -> list[Person]:
        # Unpack values from the row and feed them to the class constructor
        return [Person(*row) for row in df.to_numpy(dtype=object, na_value=None)]

    def _to_cho(self, df: DataFrame) -> list[CulturalHeritageObject]:
        result = list()

        # Variable to track the current object identifier
        object_id = ''

        # Unpack values from each row and feed them to the class constructor.
        # NaN values are turned to None and handled accordingly by the class
        # __init__. Attribute hasAuthor is aways initialised as an empty list
        # and populated after the object creation.
        for row in df.to_numpy(dtype=object, na_value=None):
            # If the identifier is the same of the previous row, do not
            # recreate the object but only append the author to the hasAuthor
            # list of the last created object
            if object_id != row[1]:
                object_id = row[1]

                # Retrieve the constructor of the class from its name
                object_class = eval(row[0])

                # Create a new object and append it to the result list
                result.append(object_class(*row[1:-2]))
            
            if row[-2]:
                result[-1].hasAuthor.append(Person(*row[-2:]))

        return result

    def _to_activity(self, df: DataFrame) -> list[Activity]:
        if df.empty:
            return list()

        activity_ids = df.index
        object_ids, objects = self.getCulturalHeritageObjectsById(activity_ids)

        activity_map = []
        for activity_name in df.columns.get_level_values(0).unique():
            activity_class = eval(activity_name)
            step = len(ACTIVITIES[activity_name]) - 1
            activity_map.append((activity_class, step))

        result = []
        for row, obj in zip(df[activity_ids.isin(object_ids)].to_numpy(dtype=object, na_value=None), objects):
            index = 0
            for activity_class, step in activity_map:
                data = row[index:index + step]
                if data[0]:
                    result.append(activity_class(obj, *data))
                index += step

        return result

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

    def getCulturalHeritageObjectsById(self, identifiers: list[str]) -> tuple[set[str], list[CulturalHeritageObject]]: # Francesca
        object_ids, result = set(), list()
        for handler in self.metadataQuery:
            df = handler.getById(identifiers)

            if df.empty:
                continue
            else:
                object_ids.update(df.identifier)
                result += self._to_cho(df)

        # Manage duplicate and/or unordered metadata query handlers
        if len(self.metadataQuery) > 1:
            result = sorted(list(set(result)))

        return object_ids, result

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
        try:
            df = concat(handler.getAllActivities() for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)

def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]: # Lin
        try:
            df = concat(handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)
    

    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]: # Lin
        try:
            df = concat(handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)

    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]: # Lin
        try:
            df = concat(handler.getActivitiesUsingTool(partialName) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)

    def getActivitiesStartedAfter(self, date: str) -> list[Activity]: # Lin
        try:
            df = concat(handler.getActivitiesStartedAfter(date) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)

    def getActivitiesEndedBefore(self, date:str) -> list[Activity]: # Lin
        try:
            df = concat(handler.getActivitiesEndedBefore(date) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)

    def getAcquisitionsByTechnique(self, partialName: str) -> list[Acquisition]: # Lin
        try:
            df = concat(handler.getAcquisitionsByTechnique(partialName) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)
