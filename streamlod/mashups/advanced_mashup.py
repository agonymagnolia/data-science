from typing import List, Set
import pandas as pd
from .basic_mashup import BasicMashup
from ..domain import Person, CulturalHeritageObject, Activity

class AdvancedMashup(BasicMashup):

    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> List[Activity]:
        
        object_ids = set()

        for handler in self.metadataQuery:
            ids = handler.getCulturalHeritageObjectsAuthoredBy(personId)['identifier']
            object_ids.update(ids)

        df_list = [handler.getById(object_ids) for handler in self.processQuery]
        return self.toActivity(df_list)

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> List[CulturalHeritageObject]:
        
        object_ids = set()

        for handler in self.processQuery:
            ids = handler.queryAttribute(filter_condition=f"WHERE person LIKE '%{partialName}%'")
            object_ids.update(ids)
        
        return self.getCulturalHeritageObjectsByIds(object_ids)[0]

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> List[CulturalHeritageObject]:

        object_ids = set()

        for handler in self.processQuery:
            ids = handler.queryAttribute(filter_condition=f"WHERE institute LIKE '%{partialName}%'")
            object_ids.update(ids)

        return self.getCulturalHeritageObjectsByIds(object_ids)[0]
    
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> List[Person]:
        
        object_ids = set()

        for handler in self.processQuery:
            ids = handler.queryAttribute(
                activity_list=['Acquisition'],
                filter_condition=f"WHERE start >= '{start}' AND end <= '{end}'"
            )
            object_ids.update(ids)

        # Using toPerson method to process the list of dataframes
        df_list = [metadata_handler.getAuthorsOfCulturalHeritageObject(list(object_ids)) for metadata_handler in self.metadataQuery]
        return self.toPerson(df_list)
