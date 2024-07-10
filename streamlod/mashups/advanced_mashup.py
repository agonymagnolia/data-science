from typing import List, Set
import pandas as pd
from .basic_mashup import BasicMashup
from ..domain import Person, CulturalHeritageObject, Activity

class AdvancedMashup(BasicMashup):
    """
    The AdvancedMashup class handles two-way filter queries to multiple graph or relational databases
    and integrates the data into unified Python objects.
    """
    
    def __init__(self, metadata_handlers=None, process_handlers=None):
        super().__init__()
        if metadata_handlers is not None:
            for handler in metadata_handlers:
                self.addMetadataHandler(handler)
        if process_handlers is not None:
            for handler in process_handlers:
                self.addProcessHandler(handler)

    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> List[Activity]:
        objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        object_ids = [obj.getId() for obj in objects]
        df_list = [handler.getById(object_ids) for handler in self.processQuery]
        df = pd.concat(df_list)
        return self.toActivity(df)

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> List[CulturalHeritageObject]:
        ids_set = set()
        for handler in self.processQuery:
            ids = handler.queryAttribute(attribute='refersTo', filter_condition=f"WHERE person LIKE '%{partialName}%'")
            ids_set.update(ids)
        return self.getCulturalHeritageObjectsByIds(list(ids_set))[0]

    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> List[CulturalHeritageObject]:
        ids_set = set()
        for handler in self.processQuery:
            ids = handler.queryAttribute(attribute='refersTo', filter_condition=f"WHERE institute LIKE '%{partialName}%'")
            ids_set.update(ids)
        return self.getCulturalHeritageObjectsByIds(list(ids_set))[0]
    
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str) -> List[Person]:
        ids_set = set()
        for handler in self.processQuery:
            ids = handler.queryAttribute(attribute='refersTo', filter_condition=f"WHERE start >= '{start}' AND end <= '{end}'", activity_list=['Acquisition'])
            ids_set.update(ids)

        authors: Set[Person] = set()
        for metadata_handler in self.metadataQuery:
            for obj_id in ids_set:
                author_objs_df = metadata_handler.getAuthorsOfCulturalHeritageObject([obj_id])
                for _, row in author_objs_df.iterrows():
                    person = Person(row['identifier'], row['name'])
                    authors.add(person)

        return list(authors)
