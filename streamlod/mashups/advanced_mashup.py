from typing import List

from streamlod.mashups.basic_mashup import BasicMashup
from streamlod.entities import Person, CulturalHeritageObject, Activity

class AdvancedMashup(BasicMashup):
    """
    The AdvancedMashup class manages two-way filter queries to multiple graph or relational databases
    and integrates the data into unified Python objects.
    """
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> List[Activity]:
        object_ids = set()

        for handler in self.metadataQuery:
            ids = handler.getEntities(select_only='identifier', by=('hasAuthor', 'identifier'), value=personId)
            object_ids.update(ids)

        dfs = [handler.getById(object_ids) for handler in self.processQuery]

        return self.toActivity(dfs)

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> List[CulturalHeritageObject]:
        object_ids = set()

        for handler in self.processQuery:
            ids = handler.getAttribute(condition=f"WHERE person LIKE '%{partialName}%'")
            object_ids.update(ids)
        
        return self.getCulturalHeritageObjectsByIds(object_ids)

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> List[CulturalHeritageObject]:
        object_ids = set()

        for handler in self.processQuery:
            ids = handler.getAttribute(condition=f"WHERE institute LIKE '%{partialName}%'")
            object_ids.update(ids)

        return self.getCulturalHeritageObjectsByIds(object_ids)
    
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> List[Person]:
        object_ids = set()

        for handler in self.processQuery:
            ids = handler.getAttribute(
                condition=f"WHERE class LIKE 'Acquisition' AND start >= '{start}' AND end <= '{end}'"
            )
            object_ids.update(ids)

        dfs = [handler.getAuthorsOfCulturalHeritageObject(object_ids) for handler in self.metadataQuery]
        return self.toPerson(dfs)
