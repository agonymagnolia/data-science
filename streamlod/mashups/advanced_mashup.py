from typing import List
from sparql_dataframe import get
import pandas as pd

from streamlod.mashups.basic_mashup import BasicMashup
from streamlod.domain import Person, CulturalHeritageObject, Activity

class AdvancedMashup(BasicMashup): # Lin
    """
    The AdvancedMashup class handles two-way filter queries to multiple graph or relational databases
    and integrates the data into unified Python objects.
    """
    
    """
    Not needed! When you don't override __init__ everything is inherited, you need to redeclare
    the __init__ method only if you need to add some more attributes, that we don't need to do.
    If you don't redeclare __init__, the AdvancedMashup inherits all the attributes and methods from the parent class BasicMashup.
    This includes also the self.processQuery and self.metadataQuery lists. When an instance of AdvancedMashup
    is created, like mshpp = AdvancedMashup(), mshpp has a mshpp.processQuery list and a mshpp.metadataQuery list,
    both initialized as empty. They are filled with the addHandlers methods. Look at streamlod.py code to see
    what I'm talking about.

    def __init__(self, metadata_handlers=None, process_handlers=None):
        super().__init__()
        if metadata_handlers is not None:
            for handler in metadata_handlers:
                self.addMetadataHandler(handler)
        if process_handlers is not None:
            for handler in process_handlers:
                self.addProcessHandler(handler)
    """
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> List[Activity]:
        """
        Here I would suggest that you use the MetadataQueryHandler method instead of the mashup method.
        What you need is just the identifiers of the objects to finally query for the related activities.

        What happens now with your method is that you MAKE the cultural heritage objects, you extract
        from them one by one their ID, and use the list of IDs to make the activities dataframes,
        which are sent to the toActivity internal method of the mashup. Do you know what happens next?
        The toActivity method makes AGAIN the cultural heritage objects to put them inside the activities.
        So there is a lot of redundancy.

        The best would be to have an equivalent method to the queryAttribute we have for the ProcessQueryHandler,
        querying specifically for the dc:identifiers of the objects with dc:creator x, y or z and outputting the plain list of identifiers.
        In that way we could do:

        object_ids = set()

        for handler in self.metadataQuery:
            ids = handler.queryAttribute(attribute='identifier', filter_condition=f'?internalId dc:creator / dc:identifier "{personId}" .') # '/' is an implicit node, which in our case is the URI of the author
            object_ids.update(ids)
    
        dfs = [handler.getById(object_ids) for handler in self.processQuery]

        return self.toActivity(dfs)

        But we don't have that for now. The next best thing would be making a tailored SPARQL query to do just that,
        but I understand that it might be hard for you. So, the third best thing is to retrieve yes the whole
        DataFrame of the objects authored by the personId, but not use that to first make the objects and then 
        extract the IDs from them, as they will be remade after anyway. You can just get the identifier column
        out of each of them and update your set.
        So you do:

        object_ids = set()

        for handler in self.metadataQuery:
            ids = handler.getCulturalHeritageObjectsAuthoredBy(personId)['identifier']
            object_ids.update(ids)

        """
        objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        object_ids = [obj.getId() for obj in objects]
        df_list = [handler.getById(object_ids) for handler in self.processQuery]
        """
        Concat is not needed, you can feed directly the list of DataFrames to toActivity because they are handled by _normalizeDFs and _ConcatDedupSort.
        If you do the concat here, toActivity does not recognize the fact that it is a concatenated DataFrame and might not be ordered (because we don't
        know in which order the ProcessQueryHandlers will retrieve the activities!), so it won't sort it again and the linking to the CulturalHeritageObjects will be broken.
        """
        # df = pd.concat(df_list)
        return self.toActivity(df_list)

    # Good job! I just normalized the syntax to the terms we have been using all the other methods, but the functions are the same.
    # The last one, though, could be improved.
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
            ids = handler.queryAttribute(['Acquisition'], filter_condition=f"WHERE start AND end BETWEEN '{start}' AND '{end}'")
            object_ids.update(ids)

        # Hint: there is a toPerson method you can use with the list of dataframes retrieved by querying all metadata handlers
        # with handler.getAuthorsOfCulturalHeritageObject(object_ids)
        authors: Set[Person] = set()
        for metadata_handler in self.metadataQuery:
            for obj_id in object_ids:
                author_objs_df = metadata_handler.getAuthorsOfCulturalHeritageObject([obj_id])
                for _, row in author_objs_df.iterrows():
                    person = Person(row['identifier'], row['name'])
                    authors.add(person)

        return list(authors)
