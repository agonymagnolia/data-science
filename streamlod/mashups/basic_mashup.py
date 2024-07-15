from typing import Union, List, Set, Iterable
import pandas as pd

from streamlod.handlers import MetadataQueryHandler, ProcessDataQueryHandler
from streamlod.entities.mappings import ACTIVITIES
from streamlod.entities import (
    IdentifiableEntity,
    Person,
    CulturalHeritageObject,
    Activity,
    Acquisition
)
from streamlod.utils import key, rank
import streamlod.entities as entities

class BasicMashup:
    """
    The BasicMashup class manages one-sided filter queries to multiple graph or relational databases
    and integrates the data into unified Python objects.

    - For single match queries (e.g., getEntityById, getAuthorsOfCulturalHeritageObject), 
      the iteration on the handler instances stops at the first result found.
    - For multiple single matches (e.g., getCulturalHeritageObjectsByIds),
      the search stops once all matches are found or all handlers have been queried.
    - For other queries (e.g., getAllActivities, getCulturalHeritageObjectsAuthoredBy), 
      results from all handlers are aggregated.
    """
    def __init__(self):
        self.metadataQuery = []
        self.processQuery = []

    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery = []
        return True

    def cleanProcessHandlers(self) -> bool:
        self.processQuery = []
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:
        self.processQuery.append(handler)
        return True

    def _normalizeDFs(self, dfs: Union[pd.DataFrame, List[pd.DataFrame]], entity_name: str) -> pd.DataFrame:
        """
        Normalize input DataFrame or list of DataFrames.

        - If all DataFrames in the list are empty, returns an empty DataFrame.
        - If there is a single DataFrame it is returned as is.
        - If more, they are concatenated.
        """
        if isinstance(dfs, list):
            non_empty_dfs = [df for df in dfs if not df.empty]
            if not non_empty_dfs:
                return pd.DataFrame()
            elif len(non_empty_dfs) == 1:
                df = non_empty_dfs[0]
            else:
                df = self._concatDedupSort(non_empty_dfs, entity_name)
        else:
            df = dfs

        if entity_name == 'CHO':
            df = df[~df.duplicated(['identifier', 'p_identifier'])]
        elif entity_name == 'Person':
            df = df[~df.duplicated('identifier')]

        return df



    def _concatDedupSort(self, dfs: List[pd.DataFrame], entity_name: str) -> pd.DataFrame:
        """
        Concatenate DataFrames, remove duplicates, and sort based on the object class.
        """
        df = pd.concat(dfs)

        match entity_name:
            case 'Person':
                df = df.sort_values(by='name', ignore_index=True)
            case 'CHO':
                df = df.sort_values(by=['identifier', 'p_name'], key=lambda x: x.map(key), ignore_index=True)
                df.update(df.groupby('identifier').bfill())
            case 'Activity':
                df = df.sort_index(key=lambda x: x.map(key)).groupby(level=0).bfill()
                df = df[~df.index.duplicated()]
                df = df[sorted(df.columns, key=lambda x: rank[x[0]])]

        return df

    def toPerson(self, dfs: Union[pd.DataFrame, List[pd.DataFrame]]) -> List[Person]:
        """
        Convert DataFrame(s) into a list of Person objects.
        """
        if (df := self._normalizeDFs(dfs, 'Person')).empty:
            return []

        # Create Person objects from DataFrame rows
        return [Person(*row) for row in df.to_numpy(dtype=object, na_value=None)]

    def toCHO(self, dfs: Union[pd.DataFrame, List[pd.DataFrame]]) -> List[CulturalHeritageObject]:
        """
        Convert DataFrame(s) into a list of CulturalHeritageObjects.
        """
        result: List[CulturalHeritageObject] = []
        if (df := self._normalizeDFs(dfs, 'CHO')).empty:
            return []

        object_id = '' # Variable to track the current object identifier
        for row in df.to_numpy(dtype=object, na_value=None):
            # Create a new object if the identifier is different from the previous row
            if object_id != row[1]:
                object_id = row[1]
                object_class = getattr(entities, row[0])
                result.append(object_class(*row[1:-2]))
            # Append author to the hasAuthor list if present
            if row[-2]:
                result[-1].hasAuthor.append(Person(*row[-2:]))

        return result

    def toActivity(self, dfs: Union[pd.DataFrame, List[pd.DataFrame]]) -> List[Activity]:
        """
        Convert DataFrame(s) into a list of Activity objects.
        """
        result: List[Activity] = []
        if (df := self._normalizeDFs(dfs, 'Activity')).empty:
            return result

        objects, missing_ids = self.getCulturalHeritageObjectsByIds(df.index)
        # Map activity names to their corresponding classes and attribute counts
        row_map = [
            (getattr(entities, activity_name), len(ACTIVITIES[activity_name]) - 1) # Do not count 'refersTo'
            for activity_name in df.columns.get_level_values(0).unique()
        ]
        array = df[~df.index.isin(missing_ids)].to_numpy(dtype=object, na_value=None)
        # Iterate through the DataFrame rows, creating Activity instances and linking them with cultural heritage objects
        for obj, row in zip(objects, array):
            index = 0
            for activity_class, step in row_map:
                data = row[index:index + step]
                if data[0]: # Only create an activity if institute is defined
                    result.append(activity_class(obj, *data))
                index += step

        return result

    def getEntityById(self, identifier: str) -> Union[IdentifiableEntity, None]: # Francesca
        dfs = []
        for handler in self.metadataQuery:
            if (df := handler.getById(identifier)).empty:
                continue
            elif 'class' in df: # The result is an object
                dfs.append(df)
            else:
                return self.toPerson(df)[0]
        if dfs:
            return self.toCHO(dfs)[0]
        else:
            return None

    def getCulturalHeritageObjectsByIds(self, identifiers: Iterable[str]) -> tuple[List[CulturalHeritageObject], Set[str]]: # Francesca
        """
        Retrieve cultural heritage objects by their identifiers from multiple metadata handlers.

        In addition to the object list, the set of identifiers that were not found is also returned
        for alignment purposes during the construction of Activity objects.
        """
        dfs = []
        identifiers, missing_ids = set(identifiers), set(identifiers)
        for handler in self.metadataQuery:
            if (df := handler.getEntities(by='identifier', value=identifiers)).empty:
                continue
            missing_ids -= set(df['identifier'])
            dfs.append(df)

        return self.toCHO(dfs), missing_ids

    def getAllPeople(self) -> List[Person]: # Francesca
        dfs = [handler.getAllPeople() for handler in self.metadataQuery]
        return self.toPerson(dfs)

    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]: # Francesca
        dfs = [handler.getAllCulturalHeritageObjects() for handler in self.metadataQuery]
        return self.toCHO(dfs)

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> List[Person]: # Francesca
        dfs = [handler.getAuthorsOfCulturalHeritageObject(objectId) for handler in self.metadataQuery]
        return self.toPerson(dfs)

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> List[CulturalHeritageObject]: # Francesca
        dfs = [handler.getCulturalHeritageObjectsAuthoredBy(personId) for handler in self.metadataQuery]
        return self.toCHO(dfs)

    def getAllActivities(self) -> List[Activity]: # Lin
        dfs = [handler.getAllActivities() for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> List[Activity]: # Lin
        dfs = [handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesByResponsiblePerson(self, partialName: str) -> List[Activity]: # Lin
        dfs = [handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesUsingTool(self, partialName: str) -> List[Activity]: # Lin
        dfs = [handler.getActivitiesUsingTool(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesStartedAfter(self, date: str) -> List[Activity]: # Lin
        dfs = [handler.getActivitiesStartedAfter(date) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesEndedBefore(self, date:str) -> List[Activity]: # Lin
        dfs = [handler.getActivitiesEndedBefore(date) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getAcquisitionsByTechnique(self, partialName: str) -> List[Activity]: # Lin
        dfs = [handler.getAcquisitionsByTechnique(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)