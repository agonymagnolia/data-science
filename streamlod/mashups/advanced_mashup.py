from typing import List
import pandas as pd
from .basic_mashup import BasicMashup
from ..domain import Person, CulturalHeritageObject, Activity

class AdvancedMashup(BasicMashup):
    """
    The AdvancedMashup class handles two-way filter queries to multiple graph or relational databases
    and integrates the data into unified Python objects.
    """
    
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> List[Activity]:
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

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> List[CulturalHeritageObject]:
        try:
            ids_set = set()
            for handler in self.processQuery:
                ids = handler.queryAttribute(attribute='refersTo', filter_condition=f"WHERE person LIKE '%{partialName}%'")
                ids_set.update(ids)
            return self.getCulturalHeritageObjectsByIds(list(ids_set))
        except ValueError:
            return list()

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> List[CulturalHeritageObject]:
        try:
            ids_set = set()
            for handler in self.processQuery:
                ids = handler.queryAttribute(attribute='refersTo', filter_condition=f"WHERE institute LIKE '%{partialName}%'")
                ids_set.update(ids)
            return self.getCulturalHeritageObjectsByIds(list(ids_set))
        except ValueError:
            return list()

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> List[Person]:
        try:
            ids_set = set()
            for handler in self.processQuery:
                start_ids = set(handler.queryAttribute(attribute='refersTo', filter_condition=f"WHERE start >= '{start}'"))
                end_ids = set(handler.queryAttribute(attribute='refersTo', filter_condition=f"WHERE end <= '{end}'"))
                ids_set.update(start_ids.intersection(end_ids))
            return self.getAuthorsOfCulturalHeritageObject(list(ids_set))
        except ValueError:
            return list()
