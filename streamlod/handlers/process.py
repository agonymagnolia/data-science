from typing import Union, Any, List, Dict, Set, Mapping, Iterable
import pandas as pd
import json
import sqlite3

from streamlod.handlers.base import UploadHandler, QueryHandler
from streamlod.utils import id_join, sorter

class ProcessDataUploadHandler(UploadHandler):
    _json_map = {
            'responsible institute': 'institute',
            'responsible person': 'person',
            'technique': 'technique',
            'tool': 'tool',
            'start date': 'start',
            'end date': 'end'
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
                        internalId INTEGER PRIMARY KEY,
                        class TEXT NOT NULL,
                        refersTo TEXT NOT NULL,
                        institute TEXT NOT NULL,
                        person TEXT,
                        technique TEXT,
                        start TEXT,
                        end TEXT
                    );
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Tool (
                        activityId INTEGER,
                        tool TEXT,
                        FOREIGN KEY(activityId) REFERENCES Activity(internalId) ON DELETE CASCADE
                    );
                ''')
            return True
        except sqlite3.OperationalError as e:
            print(e)
            return False

    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Reshape DataFrame with attributes as columns
        df.columns = df.columns.str.capitalize().str.split('.', expand=True)
        df = df.set_index(df.columns[0]) \
               .stack(level=0, future_stack=True) \
               .rename(columns=self._json_map) \
               .reset_index(names=['refersTo', 'class']) \
               .reindex(['class', 'refersTo', 'technique', 'institute', 'person', 'start', 'end', 'tool'], axis=1) # Allow for missing columns like technique, filled with NaNs

        # Strip whitespaces and handle missing values
        attributes = ['class', 'refersTo', 'technique', 'institute', 'person', 'start', 'end']
        df[attributes] = df[attributes].astype('string').apply(lambda x: x.str.strip(), axis=1).replace(r'', pd.NA)

        # Perform the same on list values of the tool column
        df['tool'] = df['tool'].explode().astype('string').str.strip() \
                               .groupby(level=0).agg(lambda x: x.tolist() if x.notna().all() else pd.NA)

        # Use boolean mask to exclude invalid rows
        activities = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
        mask = (
            (df['class'].isin(activities)) &      # 'class' must be one of the allowed values
            df['refersTo'].notna() &              # 'refersTo' must be defined
            df['institute'].notna() &             # 'institute' must be defined
            (
                (df['class'] == 'Acquisition') & df['technique'].notna() |  # Acquisition with technique defined
                (df['class'] != 'Acquisition') & df['technique'].isna()     # Non-Acquisition with technique undefined
            )
        )

        return df[mask]

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
        array = self._validate(df).to_numpy(dtype=object, na_value=None)

        activity_query = f"INSERT INTO Activity (class, refersTo, technique, institute, person, start, end) VALUES (?, ?, ?, ?, ?, ?, ?)"
        tool_query = "INSERT INTO Tool (activityId, tool) VALUES (?, ?)"

        try:
            with sqlite3.connect(db) as con:
                cursor = con.cursor()
                for row in array:
                    cursor.execute(activity_query,(row[:-1]))
                    internalId = cursor.lastrowid
                    if (tools := row[-1]):
                        cursor.executemany(tool_query, [(internalId, tool) for tool in tools])
                    else:
                        cursor.execute(tool_query, (internalId, None))
            return True

        except sqlite3.OperationalError as e:
            print(e)
            return False

    def clearDb(self) -> bool:
        db = self.getDbPathOrUrl()
        try:
            with sqlite3.connect(db) as con:
                con.execute(f"DROP TABLE IF EXISTS Activity;")
                con.execute(f"DROP TABLE IF EXISTS Tool;")
            return True
        except sqlite3.OperationalError as e:
            print(e)
            return False

class ProcessDataQueryHandler(QueryHandler):

    def getAttribute(
        self,
        attribute: str = 'refersTo',
        condition: str = ''
    ) -> List[Any]:
        """
        Performs a query to retrieve the values of a column of the main activity table for the rows that match the condition.
        """
        db = self.getDbPathOrUrl()

        with sqlite3.connect(db) as con:
            cursor = con.cursor()
            query = f"""
                SELECT {attribute}
                FROM Activity
                {condition};"""
            # Execute the combined query and fetch the results
            try:
                result = cursor.execute(query).fetchall()
            except sqlite3.OperationalError: # Attribute column does not exist
                return []

        # Return a list of the attribute values
        return [row[0] for row in result]

    def getActivities(
        self,
        condition: str = ''
    ) -> pd.DataFrame:
        """
        Retrieves data from the main activity table, linking each activity to its associated tools and applying an optional filter condition.
        If no valid activities are found, an empty DataFrame is returned.
        """
        activities = {}
        db = self.getDbPathOrUrl()

        query = f"""
            SELECT class, refersTo, technique, institute, person, start, end, GROUP_CONCAT(T.tool) AS tool
            FROM Activity AS A
            JOIN Tool AS T
            ON A.internalId = T.activityId
            {condition}
            GROUP BY A.internalId;
            """
        with sqlite3.connect(db) as con:
            df = pd.read_sql_query(query, con, dtype='object')

        # Split tool combined string in a set
        df.tool = df.tool.apply(lambda x: set(x.split(',')) if x else None)

        # Sort alphanumerically the index
        return df.sort_values(by=['refersTo', 'class'], key=sorter)

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
        return self.getActivities(condition=f"WHERE A.class LIKE 'Acquisition' AND A.technique LIKE '%{partialName}%'")