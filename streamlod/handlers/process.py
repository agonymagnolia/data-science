from typing import Union, Any, List, Dict, Set, Mapping, Iterable
import pandas as pd
import json
import sqlite3

from streamlod.handlers.base import UploadHandler, QueryHandler
from streamlod.entities.mappings import ACTIVITIES, ACTIVITY_ATTRIBUTES, ACQUISITION_ATTRIBUTES
from streamlod.utils import id_join, sorter


class ProcessDataUploadHandler(UploadHandler):
    _json_map = {
            'object id': 'refersTo',
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

    def validate(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame | pd.Series]:
        result = {}
        df.columns = pd.MultiIndex.from_tuples(((e[0].capitalize(), self._json_map[e[1]]) if len(e) > 1 else ('', self._json_map[e[0]]) for e in df.columns.str.split('.')))
        df.set_index(df.columns[0], inplace=True)
        df = df.stack(level=0, future_stack=True)
        df.reset_index(level=0, names=['refersTo'], inplace=True)

        mask = pd.DataFrame(ACTIVITIES).T
        df = df.reindex(mask.columns, axis=1) # Allows for missing columns like technique, filled with NaNs

        multivalued = [attr for attr, (_, multivalued) in ACTIVITY_ATTRIBUTES.items() if multivalued]
        main_cols = [col for col in df.columns if col not in multivalued]

        df[main_cols] = df[main_cols].astype('string').apply(lambda x: x.str.strip(), axis=1)
        df = df.map(lambda x: pd.NA if len(x)==0 else x, na_action='ignore')

        isna = df.isna()
        required = mask.map(lambda x: x[0], na_action='ignore').reindex(df.index).astype('boolean')
        permitted = required.notna()
        validated = (isna & ~required) | (~isna & permitted)
        df = df[validated.all(axis=1)].reset_index(names=['class'])
        main_cols.insert(0, 'class')

        if not df.empty:
            result['activity'] = df[main_cols].to_numpy(dtype=object, na_value=None)
        for col in multivalued:
            result[col] = df[col].explode().astype('string').str.strip().groupby(level=0).agg(lambda x: x.tolist() if x.notna().all() else []).to_numpy(dtype=object, na_value=None)

        return result, main_cols




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
        df_dict, main_cols = self.validate(df)

        queries = []
        for pos, name in enumerate(df_dict.keys()):
            if pos == 0:
                queries.append(f"INSERT INTO Activity ({', '.join(list(main_cols))}) VALUES ({', '.join(['?'] * len(main_cols))})")
            else:
                queries.append(f"INSERT INTO {name.capitalize()} (activityId, {name}) VALUES (?, ?)")

        try:
            with sqlite3.connect(db) as con:
                cursor = con.cursor()
                internalId = 0
                for row in zip(*df_dict.values()):
                    for pos, query in enumerate(queries):
                        if pos == 0:
                            cursor.execute(query,(row[pos]))
                            internalId = cursor.lastrowid
                        elif (values := row[pos]):
                            cursor.executemany(query, [(internalId, val) for val in values])
                        else:
                            cursor.execute(query, (internalId, None))
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
        Performs a unified query to retrieve the values of a column in all activity tables for the rows that match the condition.
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

        attrs = list(ACQUISITION_ATTRIBUTES.keys())
        attrs.remove('tool') # Exclude the tool column
        cols = ", ".join(attrs)
        query = f"""
            SELECT class, {cols}, GROUP_CONCAT(T.tool) AS tool
            FROM Activity AS A
            JOIN Tool AS T
            ON A.internalId = T.activityId
            {condition}
            GROUP BY A.internalId;
            """
        with sqlite3.connect(db) as con:
            df = pd.read_sql_query(query, con)

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