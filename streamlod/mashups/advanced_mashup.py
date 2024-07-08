from .basic_mashup import BasicMashup
from ..handlers import MetadataQueryHandler, ProcessDataQueryHandler
from ..domain import Person, CulturalHeritageObject, Activity
from ..utils import key

from pandas import DataFrame, concat, read_sql_query
from sqlite3 import connect
from sparql_dataframe import get


class AdvancedMashup(BasicMashup): # Lin
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
        pass

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        pass

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
        pass

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:
        pass