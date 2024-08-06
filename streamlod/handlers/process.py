from typing import Union, Any, List, Dict, Set, Mapping, Iterable
import pandas as pd
import json
import sqlite3

from streamlod.handlers.base import UploadHandler, QueryHandler
from streamlod.entities.mappings import ACTIVITIES
from streamlod.utils import id_join, sorter


class ProcessDataUploadHandler(UploadHandler):
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

    def setDbPathOrUrl(self, newDbPathOrUrl: str, *, reset: bool = False) -> bool:
        # Set the new database path
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False
        elif reset and not self.clearDb():
            return False

        db = self.getDbPathOrUrl()
        try:
            with sqlite3.connect(db) as con:
                cursor = con.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Activity (
                        internalId INTEGER PRIMARY KEY
                    );
                ''')
                for activity, attrs in ACTIVITIES.items():
                    cols = []
                    for attr, (required, multivalued) in attrs.items():
                        if multivalued:
                            cursor.execute(f'''
                                CREATE TABLE IF NOT EXISTS {attr.capitalize()} (
                                    activityId INTEGER,
                                    {attr} TEXT,
                                    FOREIGN KEY(activityId) REFERENCES Activity(internalId) ON DELETE CASCADE
                                );
                            ''')
                        elif required:
                            cols.append(f'{attr} TEXT NOT NULL')
                        else:
                            cols.append(f'{attr} TEXT')
                    cols_str = ',\n                        '.join(cols)
                    cursor.execute(f'''
                        CREATE TABLE IF NOT EXISTS {activity} (
                            internalId INTEGER PRIMARY KEY,
                            {cols_str},
                            FOREIGN KEY(internalId) REFERENCES Activity(internalId) ON DELETE CASCADE
                        );
                    ''')
            return True
        except sqlite3.OperationalError as e:
            print(e)
            return False

    def pushDataToDb(self, path: str) -> bool:
        if not (db := self.getDbPathOrUrl()):
            print('Exception: Database path not set.')
            return False

        # Load the JSON file
        try:
            with open(path, 'r', encoding='utf-8') as file:
                json_doc = json.load(file)
        except IOError as e:
            print(e)
            return False
        except json.JSONDecodeError as e: # Not a valid JSON document
            print(e)
            return False

        # Flatten the JSON document into a DataFrame
        df = pd.json_normalize(json_doc)
        
        activities, tools = {}, pd.DataFrame()  # Initialize storage for activities and tools

        # Process each activity
        for name, attrs in ACTIVITIES.items():
            cols = list(attrs)
            
            # Rename and select relevant columns
            try:
                activity = df.rename(columns=self._json_map(name.lower()))[cols]
            except KeyError as e:
                continue

            # Separate tool column and trim spaces in each activity column
            tool = activity.pop('tool')
            for col in activity:
                activity[col] = activity[col].str.strip()

            # Drop rows not compliant to the data model
            validate = [attr for attr, (required, _) in attrs.items() if required]
            activity.replace(r'', pd.NA, inplace=True)
            activity = activity[(activity[validate].notna() & activity.map(lambda x: isinstance(x, str))).all(axis=1)]

            if activity.empty:
                continue

            with sqlite3.connect(db) as con:
                cursor = con.cursor()
                cursor.executemany('INSERT INTO Activity DEFAULT VALUES', [()] * len(activity)) # List of tuples
                cursor.execute('SELECT * FROM (SELECT internalId FROM Activity ORDER BY internalId DESC LIMIT ?) ORDER BY internalId', (len(activity),))
                activity['internalId'] = [row[0] for row in cursor.fetchall()]

            activities[name] = activity  # Store valid activities DataFrame linked to activity name

            # Split lists in tool column into separate rows with the same internalId
            split = pd.concat([activity.internalId, tool], axis=1)
            split.rename(columns={'internalId':'activityId'}, inplace=True)
            split = split[split.activityId.notna()].explode('tool')
            split.tool = split.tool.str.strip()

            tools = pd.concat([tools, split], ignore_index=True)  # Accumulate tools

        if not activities:
            return True

        # Add tables to the database
        try:
            with sqlite3.connect(db) as con:
                for name, activity in activities.items():
                    activity.to_sql(name, con, if_exists='append', index=False, dtype='TEXT')
                tools.to_sql('Tool', con, if_exists='append', index=False, dtype='TEXT')
            return True
        except sqlite3.OperationalError as e:
            print(e)
            return False

    def clearDb(self) -> bool:
        db = self.getDbPathOrUrl()
        try:
            with sqlite3.connect(db) as con:
                con.execute(f"DROP TABLE IF EXISTS Activity;")
                for activity in ACTIVITIES:
                    con.execute(f"DROP TABLE IF EXISTS {activity};")
                con.execute(f"DROP TABLE IF EXISTS Tool;")
            return True
        except sqlite3.OperationalError as e:
            print(e)
            return False

class ProcessDataQueryHandler(QueryHandler):

    def getAttribute(
        self,
        activity_list: Iterable[str] = ACTIVITIES.keys(),
        attribute: str = 'refersTo',
        condition: str = ''
    ) -> List[Any]:
        """
        Performs a unified query to retrieve the values of a column in all activity tables for the rows that match the condition.
        """
        # Build a query for each activity table present in the db
        subqueries = []
        db = self.getDbPathOrUrl()

        with sqlite3.connect(db) as con:
            cursor = con.cursor()

            for name in activity_list:
                sql = f"""
                    SELECT {attribute}
                    FROM {name}
                    {condition}"""
                subqueries.append(sql)

            # Combine subqueries using UNION
            query = '\nUNION\n'.join(subqueries) + ';'

            # Execute the combined query and fetch the results
            try:
                result = cursor.execute(query).fetchall()
            except sqlite3.OperationalError: # Attribute column does not exist
                return []

        # Return a list of the attribute values
        return [row[0] for row in result]

    def getActivities(
        self,
        activity_list: Iterable[str] = ACTIVITIES.keys(),
        condition: str = ''
    ) -> pd.DataFrame:
        """
        Retrieves data from multiple activity tables, linking each activity to its associated tools,
        applies an optional filter condition, and returns it as a multi-index DataFrame with object IDs as the primary index,
        attributes as lower-level columns and activities as the higher-level column index.
        If no valid activities are found, an empty DataFrame is returned.
        """
        activities = {}
        db = self.getDbPathOrUrl()

        # Build and execute the query for each activity
        for name in activity_list:
            attrs = list(ACTIVITIES[name])[1:]
            attrs.remove('tool') # Exclude the tool column
            cols = ", ".join(attrs)
            query = f"""
                SELECT refersTo, {cols}, GROUP_CONCAT(T.tool) AS tool
                FROM {name} AS A
                JOIN Tool AS T
                ON A.internalId = T.activityId
                {condition}
                GROUP BY A.internalId, {cols};
                """
            with sqlite3.connect(db) as con:
                activity = pd.read_sql_query(query, con, index_col='refersTo')

            # Skip activity if no instance fulfills the condition
            if activity.empty:
                continue

            # Split tool combined string in a set and set 'refersTo' as index
            activity.tool = activity.tool.apply(lambda x: set(x.split(',')) if x else None)
            activities[name] = activity

        # Return an empty DataFrame if no valid activities are found
        if not activities:
            return pd.DataFrame()

        # Concatenate activity DataFrames sorting alphanumerically the index
        return pd.concat(activities.values(), axis=0, join='outer', keys=activities.keys(), names=['activity']) \
                 .swaplevel().sort_index(key=sorter)

    def getById(self, identifier: Union[str, List[str]]) -> pd.DataFrame:
        # Normalize identifiers to a string
        identifier = id_join(identifier, ', ')
        return self.getActivities(condition=f'WHERE A.refersTo IN ({identifier})')

    def getAllActivities(self) -> pd.DataFrame:
        return self.getActivities()

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> pd.DataFrame:
        return self.getActivities(condition=f"WHERE A.institute LIKE '%{partialName}%'")

    def getActivitiesByResponsiblePerson(self, partialName: str) -> pd.DataFrame:
        return self.getActivities(condition=f"WHERE A.person LIKE '%{partialName}%'")

    def getActivitiesUsingTool(self, partialName: str) -> pd.DataFrame:
        return self.getActivities(condition=f"WHERE T.tool LIKE '%{partialName}%'")

    def getActivitiesStartedAfter(self, date: str) -> pd.DataFrame:
        return self.getActivities(condition=f"WHERE A.start >= '{date}'")

    def getActivitiesEndedBefore(self, date: str) -> pd.DataFrame:
        return self.getActivities(condition=f"WHERE A.end <= '{date}'")

    def getAcquisitionsByTechnique(self, partialName: str) -> pd.DataFrame:
        return self.getActivities(['Acquisition'], condition=f"WHERE A.technique LIKE '%{partialName}%'")