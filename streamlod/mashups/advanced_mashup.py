from .basic_mashup import BasicMashup
from ..handlers import MetadataQueryHandler, ProcessDataQueryHandler
from ..domain import Person, CulturalHeritageObject, Activity
from ..utils import key

from pandas import DataFrame, concat, read_sql_query
from sqlite3 import connect
from sparql_dataframe import get


class AdvancedMashup(BasicMashup): # Lin
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
         # Get objects authored by the person
        objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        # Get activities on these objects
        object_ids = [obj.getId() for obj in objects]
        try:
            df = concat(handler.getById(object_ids) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_activity(df)

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        try:
            df = concat(handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_cho(df)

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
        try:
            df = concat(handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_cho(df)

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:
        try:
            df = concat(handler.getActivitiesStartedAfter(start) & handler.getActivitiesEndedBefore(end) for handler in self.processQuery)
        except ValueError:
            return list()
        df = df[~df.index.duplicated()].sort_index(key=lambda x: x.map(key))
        return self._to_person(df)
