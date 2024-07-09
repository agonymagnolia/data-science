from typing import Union, Any, List, Dict
import pandas as pd
import json
import sqlite3

from .base import UploadHandler, QueryHandler
from ..utils import key, id_join

ACTIVITY_ATTRIBUTES = { # Attribute : required
    'refersTo': True,
    'institute': True,
    'person': False,
    'start': False,
    'end': False,
    'tool': False
}

ACQUISITION_ATTRIBUTES = {
    'refersTo': True,
    'institute': True,
    'technique': True,
    'person': False,
    'start': False,
    'end': False,
    'tool': False
}

ACTIVITIES = {
    'Acquisition': ACQUISITION_ATTRIBUTES,
    'Processing': ACTIVITY_ATTRIBUTES,
    'Modelling': ACTIVITY_ATTRIBUTES,
    'Optimising': ACTIVITY_ATTRIBUTES,
    'Exporting': ACTIVITY_ATTRIBUTES
}


class ProcessDataUploadHandler(UploadHandler): # Alberto
    def __init__(self):
        super().__init__()
        # Initialize an empty set to track the activities present in the database
        self.identifiers: Set[str] = set()

    def _json_map(self, activity: str) -> Dict[str, str]:
        return {
            'object id': 'refersTo',
            f'{activity}.responsible institute': 'institute',
            f'{activity}.responsible person': 'person',
            f'{activity}.technique': 'technique',
            f'{activity}.tool': 'tool',
            f'{activity}.start date': 'start',
            f'{activity}.end date': 'end'
        }

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        # Set the new database path
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False
        try:
            # Connect to the database
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                query = '\nUNION\n'.join(f"SELECT internalId FROM {activity}" for activity in ACTIVITIES) + ';'

                try:
                    # Execute the combined query and update identifiers set
                    self.identifiers.update(row[0] for row in con.execute(query).fetchall())
                    
                except sqlite3.Error: # Tables do not exist
                    pass

        except sqlite3.Error as e:
            print(e)
            return False
        
        return True

    def pushDataToDb(self, path: str) -> bool:
        # Load the JSON file
        try:
            with open(path, 'r', encoding='utf-8') as file:
                json_doc = json.load(file)
        # Error raised if the data being deserialized is not a valid JSON document.
        # Repeated entries are accepted, and only the value of the last name-value pair is used
        except json.JSONDecodeError:
            return False

        # Flatten the JSON document into a DataFrame
        df = pd.json_normalize(json_doc)
        df['internalId'] = df['object id']  # Copy object id to internalId for activity identification

        activities, tools = dict(), pd.DataFrame()  # Initialize storage for activities and tools

        # Process each activity
        for name, attrs in ACTIVITIES.items():
            cols = ['internalId'] + list(attrs)
            
            # Rename and select relevant columns
            try:
                activity = df.rename(columns=self._json_map(name.lower()))[cols]
            except KeyError as e:
                print(e)
                return False

            # Separate tool column and trim spaces in each activity column
            tool = activity.pop('tool')
            for col in activity:
                activity[col] = activity[col].str.strip()
            activity['internalId'] = name + '-' + activity['internalId']  # Prefix internalId with activity name

            # Filter out activity instances already in the database
            activity = activity[~activity['internalId'].isin(self.identifiers)]
            if activity.empty:
                continue

            self.identifiers.update(activity['internalId'])  # Update existing activities set

            # Drop rows not compliant with the data model
            validate = [attr for attr, required in attrs.items() if required]
            activity.replace(r'', pd.NA, inplace=True)
            activity = activity[activity[validate].notna().all(axis=1)]

            activities[name] = activity  # Store valid activities DataFrame linked to activity name

            # Process tools: split lists in tool column into separate rows with the same internalId
            split = pd.concat([activity['internalId'], tool], axis=1)
            split = split[split['internalId'].notna()].explode('tool')
            split.tool = split.tool.str.strip()

            tools = pd.concat([tools, split], ignore_index=True)  # Accumulate tools

        if not activities:
            return True

        # Add tables to the database
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            for name, activity in activities.items():
                activity.to_sql(name, con, if_exists='append', index=False, dtype='TEXT')
            tools.to_sql('Tool', con, if_exists='append', index=False, dtype='TEXT')

        return True


class ProcessDataQueryHandler(QueryHandler): # Anna

    def queryAttribute(
        self,
        activity_list: Union[List[str], Dict[str, Any]] = ACTIVITIES,
        attribute: str = 'refersTo',
        filter_condition: str = ''
    ) -> List[Any]:
        subqueries = []

        # Build subqueries for each activity and combine them with UNION
        for activity in activity_list:
            sql= f"""
                SELECT {attribute}
                FROM {activity}
                {filter_condition}"""
            subqueries.append(sql)
        query = '\nUNION\n'.join(subqueries) + ';'

        # Execute the combined query and fetch the results
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
                result = con.execute(query).fetchall()

        # Return a list of the attribute values
        return [row[0] for row in result]

    def queryAttributes(
        self,
        activity_dict: Dict[str, Union[List[str], Dict[str, Any]]] = ACTIVITIES,
        filter_condition: str = ''
    ) -> pd.DataFrame:
        activities = {}

        # Build and execute the query for each activity
        for name, attrs in activity_dict.items():
            cols = ", ".join(list(attrs)[:-1]) # Exclude the 'tool' column
            query = f"""
                SELECT {cols}, GROUP_CONCAT(T.tool) AS tool
                FROM {name} AS A
                JOIN Tool AS T
                ON A.internalId = T.internalId
                {filter_condition}
                GROUP BY {cols};
                """
            with sqlite3.connect(self.getDbPathOrUrl()) as con:
                activity = pd.read_sql_query(query, con)

            # Skip activity if the DataFrame is all NaNs (no instance fulfills the condition)
            if activity.isnull().values.all():
                continue

            # Split tool combined string in a set and set 'refersTo' as index
            activity.tool = activity.tool.apply(lambda x: set(x.split(',')) if x else set())
            activities[name] = activity.set_index('refersTo')

        # Return an empty DataFrame if no valid activities are found
        if not activities:
            return pd.DataFrame()

        # Concatenate activity DataFrames sorting alphanumerically the index
        return pd.concat(activities.values(), axis=1, join='outer', keys=activities.keys()) \
              .sort_index(key=lambda x: x.map(key))

    def getById(self, identifiers: Union[str, List[str]]) -> pd.DataFrame:
        # Normalize identifiers to a string
        identifiers = id_join(identifiers, ', ')
        return self.queryAttributes(filter_condition=f'WHERE A.refersTo IN ({identifiers})')

    def getAllActivities(self) -> pd.DataFrame:
        return self.queryAttributes()

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> pd.DataFrame:
        return self.queryAttributes(filter_condition=f"WHERE A.institute LIKE '%{partialName}%'")

    def getActivitiesByResponsiblePerson(self, partialName: str) -> pd.DataFrame:
        return self.queryAttributes(filter_condition=f"WHERE A.person LIKE '%{partialName}%'")

    def getActivitiesUsingTool(self, partialName: str) -> pd.DataFrame:
        return self.queryAttributes(filter_condition=f"WHERE T.tool LIKE '%{partialName}%'")

    def getActivitiesStartedAfter(self, date: str) -> pd.DataFrame:
        return self.queryAttributes(filter_condition=f"WHERE A.start >= '{date}'")

    def getActivitiesEndedBefore(self, date: str) -> pd.DataFrame:
        return self.queryAttributes(filter_condition=f"WHERE A.end <= '{date}'")

    def getAcquisitionsByTechnique(self, partialName: str) -> pd.DataFrame:
        return self.queryAttributes({'Acquisition': ACQUISITION_ATTRIBUTES}, filter_condition=f"WHERE A.technique LIKE '%{partialName}%'")