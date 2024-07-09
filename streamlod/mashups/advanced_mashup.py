from typing import List
from sparql_dataframe import get
import pandas as pd

from .basic_mashup import BasicMashup
from ..domain import Person, CulturalHeritageObject, Activity

class AdvancedMashup(BasicMashup): # Lin
    """
    The AdvancedMashup class handles two-way filter queries to multiple graph or relational databases
    and integrates the data into unified Python objects.
    """
class AdvancedMashup(BasicMashup): # Lin
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
         # Get objects authored by the person
        objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        # Get activities on these objects
        object_ids = [obj.getId() for obj in objects]
        try:
            df_list = [handler.getById(object_ids) for handler in self.processQuery]
            df = pd.concat(df_list)
            return self.toActivity(df)
        except ValueError:
            return list()

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        try:
            df_list = [handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processQuery]
            df = pd.concat(df_list)
        except ValueError:
            return list()
        
        #df = df.drop_duplicates().sort_index(key=lambda x: x.map(key))
        #return self.toCHO(df)

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
        try:
            df_list = [handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processQuery]
            df = pd.concat(df_list)
        except ValueError:
            return list()
        
        #df = df.drop_duplicates().sort_index(key=lambda x: x.map(key))
        #return self.toCHO(df)

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> list[Person]:
        #try:
        #    df_list = [handler.getActivitiesStartedAfter(start) & handler.getActivitiesEndedBefore(end) for handler in self.processQuery]
        #    df = pd.concat(df_list)
        #except ValueError:
        #    return list()
        
        #df = df.drop_duplicates().sort_index(key=lambda x: x.map(key))
        #return self.toPerson(df)
        pass
