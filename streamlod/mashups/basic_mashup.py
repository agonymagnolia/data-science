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

    def _normalize(self, dfs: Union[pd.DataFrame, List[pd.DataFrame]], entity_name: str) -> pd.DataFrame:
        """
        Normalizes input DataFrame or list of DataFrames.

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
                df = self._integrate(non_empty_dfs, entity_name)
        else:
            df = dfs

        return self._validate(df, entity_name)

    def _selective_bfill(self, group: pd.DataFrame, activity_names: List[str]) -> pd.DataFrame:
        """
        Performs backward filling on a column subset within each group of rows if all values in the first row are NaN.
        This is done to fill in missing values for the same activity type on the same cultural heritage object across different databases.
        """
        is_undefined = group.iloc[0].isna() # Boolean mask on the first row of the same-id group
        for name in activity_names:
            if is_undefined[name].all(): # If all values of the activity are NaNs
                # Perform backward filling on those columns
                group[name] = group[name].infer_objects(copy=False).bfill(axis=0) # Inferring data types is required to ensure that downcasting after bfill is handled correctly

        return group.iloc[0] # Only the first row of the group is returned

    def _integrate(self, dfs: List[pd.DataFrame], entity_name: str) -> pd.DataFrame:
        """
        Concatenates DataFrames and sorts them again according to a class-dependent parameter.

        Combines data from different databases: links activities on the same object and
        adds missing data for authors or cultural objects.
        Removing duplicate identifiers from the dataframe without first performing this integration
        would result in the loss of this potentially valuable data.
        """
        df = pd.concat(dfs)

        match entity_name:
            case 'Person':
                df = df.sort_values(by='name', ignore_index=True)
                df.update(df.groupby('identifier').bfill())
            case 'CHO':
                df = df.sort_values(by=['identifier', 'p_name'], key=lambda x: x.map(key), ignore_index=True)
                df.update(df.groupby('identifier').bfill())
            case 'Activity':
                activity_names = df.columns.get_level_values(0).unique()
                df = df.sort_index(key=lambda x: x.map(key)) \
                       .groupby(level=0).apply(lambda group: self._selective_bfill(group, activity_names))
                df = df[sorted(df.columns, key=lambda x: rank[x[0]])]

        return df

    def _validate(self, df: pd.DataFrame, entity_name: str) -> pd.DataFrame:
        """
        Removes duplicated entries and verifies all required attributes are defined for graph database data.
        """
        match entity_name:
            case 'Person':
                df = df[~df.duplicated('identifier') & df['name'].notna()] # Identifier was already required by the query
            case 'CHO':
                df = df[
                    ~df.duplicated(['identifier', 'p_identifier']) & # Duplicates are evaluated on every subject-multivalue pair
                    df[['title', 'owner', 'place']].notna().all(axis=1) & # Identifier and class were already required by the query
                    ~(df['p_identifier'].notna() ^ df['p_name'].notna()) # Reversed XOR operator: either the author is fully defined or they are not
                ]
            case 'Activity':
                df = df[~df.index.duplicated()]

        return df
       
    def toPerson(self, dfs: Union[pd.DataFrame, List[pd.DataFrame]]) -> List[Person]:
        """
        Converts DataFrame(s) into a list of Person objects.
        """
        df = self._normalize(dfs, 'Person')
        if df.empty:
            return []

        # Creates Person objects from DataFrame rows
        return [Person(*row) for row in df.to_numpy(dtype=object, na_value=None)]

    def toCHO(self, dfs: Union[pd.DataFrame, List[pd.DataFrame]]) -> List[CulturalHeritageObject]:
        """
        Converts DataFrame(s) into a list of CulturalHeritageObjects.
        """
        df = self._normalize(dfs, 'CHO')
        if df.empty:
            return []

        result: List[CulturalHeritageObject] = []
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
        Converts DataFrame(s) into a list of Activity objects.
        """
        df = self._normalize(dfs, 'Activity')
        if df.empty:
            return []

        result: List[Activity] = []
        objects, missing_ids = self.getCulturalHeritageObjectsByIds(df.index)

        # Map activity names to their corresponding classes and attribute counts
        row_map = [
            (getattr(entities, activity_name), len(ACTIVITIES[activity_name]) - 1) # Do not count 'refersTo'
            for activity_name in df.columns.get_level_values(0).unique()
        ]

        # Iterate through the DataFrame rows, creating Activity instances and linking them with cultural heritage objects
        array = df[~df.index.isin(missing_ids)].to_numpy(dtype=object, na_value=None)
        for obj, row in zip(objects, array):
            index = 0
            for activity_class, step in row_map:
                data = row[index:index + step]
                if data[0]: # Only create an activity if institute is defined
                    result.append(activity_class(obj, *data))
                index += step

        return result

    def getEntityById(self, identifier: str) -> Union[IdentifiableEntity, None]:
        obj_dfs, people_dfs = [], []
        for handler in self.metadataQuery:
            df = handler.getById(identifier)
            if df.empty:
                continue

            if 'class' in df.columns: # The result is an object
                obj_dfs.append(df)
            else:
                people_dfs.append(df)

        if obj_dfs:
            result = self.toCHO(obj_dfs)
        elif people_dfs:
            result = self.toPerson(people_dfs)
        else:
            return None

        return result[0] if result else None # Check result again as constructors might return an empty list for invalid data

    def getCulturalHeritageObjectsByIds(self, identifiers: Iterable[str]) -> tuple[List[CulturalHeritageObject], Set[str]]:
        """
        Retrieves cultural heritage objects by their identifiers from multiple metadata handlers.

        In addition to the object list, the set of identifiers that were not found is also returned
        for alignment purposes during the construction of Activity objects.
        """
        dfs = []
        identifiers, missing_ids = set(identifiers), set(identifiers)
        for handler in self.metadataQuery:
            df = handler.getEntities(by='identifier', value=identifiers)
            if df.empty:
                continue
            missing_ids -= set(df['identifier'])
            dfs.append(df)

        return self.toCHO(dfs), missing_ids

    def getAllPeople(self) -> List[Person]:
        dfs = [handler.getAllPeople() for handler in self.metadataQuery]
        return self.toPerson(dfs)

    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]:
        dfs = [handler.getAllCulturalHeritageObjects() for handler in self.metadataQuery]
        return self.toCHO(dfs)

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> List[Person]:
        dfs = [handler.getAuthorsOfCulturalHeritageObject(objectId) for handler in self.metadataQuery]
        return self.toPerson(dfs)

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> List[CulturalHeritageObject]:
        dfs = [handler.getCulturalHeritageObjectsAuthoredBy(personId) for handler in self.metadataQuery]
        return self.toCHO(dfs)

    def getAllActivities(self) -> List[Activity]:
        dfs = [handler.getAllActivities() for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> List[Activity]:
        dfs = [handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesByResponsiblePerson(self, partialName: str) -> List[Activity]:
        dfs = [handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesUsingTool(self, partialName: str) -> List[Activity]:
        dfs = [handler.getActivitiesUsingTool(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesStartedAfter(self, date: str) -> List[Activity]:
        dfs = [handler.getActivitiesStartedAfter(date) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getActivitiesEndedBefore(self, date:str) -> List[Activity]:
        dfs = [handler.getActivitiesEndedBefore(date) for handler in self.processQuery]
        return self.toActivity(dfs)

    def getAcquisitionsByTechnique(self, partialName: str) -> List[Activity]:
        dfs = [handler.getAcquisitionsByTechnique(partialName) for handler in self.processQuery]
        return self.toActivity(dfs)