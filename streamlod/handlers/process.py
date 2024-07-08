from .base import UploadHandler, QueryHandler
from ..utils import key, id_join

from json import load, JSONDecodeError

from pandas import (
    DataFrame,
    json_normalize,
    concat,
    NA,
    read_sql_query
)

from sqlite3 import connect, Error


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
        self.activities: set[str] = set()

    def _json_map(self, activity: str) -> dict[str, str]:
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
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False
            
        try:
            con = connect(self.getDbPathOrUrl())
        except Error as e:
            print(e)
            return False

        # Retrieve existing IDs from the database and store them in the set
        query = '\nUNION\n'.join([f"SELECT internalId FROM {activity}" for activity in ACTIVITIES]) + ';'

        try:
            self.activities.update(row[0] for row in con.execute(query).fetchall()) 
        except Error: # Tables do not exist
            pass

        con.close()
        return True


    def pushDataToDb(self, path: str) -> bool:
        with open(path, 'r', encoding='utf-8') as file:

            try:
                json_doc = load(file)
            # Error raised if the data being deserialized is not a valid
            # JSON document. Repeated names within an object are accepted,
            # and only the value of the last name-value pair is used
            except JSONDecodeError:
                return False

        # Flatten the json file in one DataFrame
        df = json_normalize(json_doc)

        # Copy the object id column to identify the activity
        df['internalId'] = df['object id']

        # Initialize a dictionary to store activities DataFrames and an empty
        # DataFrame to store tool information
        activities, tools = dict(), DataFrame()

        # Rename columns for each activity, select the relevant columns and
        # finally store each DataFrame in the dictionary
        for name, attrs in ACTIVITIES.items():
            cols = ['internalId'] + list(attrs)

            try:
                activity = df.rename(columns=self._json_map(name.lower()))[cols]
            except KeyError as e:
                print(e)
                return False

            # String operations can only be performed without the tool
            # column as it contains lists
            tool = activity.pop('tool')

            # Trim spaces in every column
            for col in activity:
                activity[col] = activity[col].str.strip()

            # Add activity prefix to the internalId
            activity.internalId = name + '-' + activity.internalId

            # Check for existing IDs and skip if the activity instance is
            # already present in the database. This check takes place at
            # the level of the unique activity id and not of the object id
            # because distinct activities could be performed on the same
            # object on various stages and added at different times
            activity = activity[~activity['internalId'].isin(self.activities)]

            if activity.empty:
                continue

            self.activities.update(activity['internalId'])

            # Drop every row non compliant to the data model
            validate = [attr for attr, required in attrs.items() if required]
            activity.replace(r'', NA, inplace=True)
            activity = activity[activity[validate].notna().all(axis=1)]

            activities[name] = activity

            # Create a tool DataFrame combining internalId and tool columns,
            # drop rows where internalId is not defined and split each list
            # in the tool column into a separate row, while duplicating the
            # internalId values for each expandend row (explode method)
            activity_tool = concat([activity.internalId, tool], axis=1)
            activity_tool = activity_tool[activity_tool['internalId'].notna()] \
                           .explode('tool')
            activity_tool.tool = activity_tool.tool.str.strip()

            # Concatenate tool_df with tools DataFrame to accumulate results
            tools = concat([tools, activity_tool], axis=0, ignore_index=True)

        if not activities:
            return True

        # Add tables to the database
        db = self.getDbPathOrUrl()
        with connect(db) as con:
            for name, activity in activities.items():
                activity.to_sql(name, con, if_exists='append', index=False, dtype='TEXT')
            tools.to_sql('Tool', con, if_exists='append', index=False, dtype='TEXT')

        return True


class ProcessDataQueryHandler(QueryHandler): # Anna

    def sql_list(
        self,
        activity_list: list[str] | dict[str, 'Any'] = ACTIVITIES,
        attribute: str = 'refersTo',
        filter_condition: str = ''
    ) -> list[str]:
        db = self.getDbPathOrUrl()
        subqueries = []

        for activity in activity_list:
            sql= f"""
                SELECT {attribute}
                FROM {activity}
                {filter_condition}"""
                
            subqueries.append(sql)
            
        query = '\nUNION\n'.join(subqueries) + ';'

        with connect(db) as con:
                result = con.execute(query).fetchall()

        return [row[0] for row in result]

    def sql_df(
        self,
        activity_dict: dict[str, list[str] | dict[str, 'Any']] = ACTIVITIES,
        filter_condition: str = ''
    ) -> DataFrame:
        db = self.getDbPathOrUrl()
        activities = dict()

        for name, attrs in activity_dict.items():
            cols = ", ".join(list(attrs)[:-1])
            query = f"""
                SELECT {cols}, GROUP_CONCAT(T.tool) AS tool
                FROM {name} AS A
                JOIN Tool AS T
                ON A.internalId = T.internalId
                {filter_condition}
                GROUP BY {cols};
                """
            # Execute the query and store the resulting DataFrame in the dictionary
            with connect(db) as con:
                activity = read_sql_query(query, con)

            # If none of the instances of that activity fulfil the condition, do not
            # add it to the result dataframe
            if activity.isnull().values.all():
                continue

            activity.tool = activity.tool.apply(lambda x: set(x.split(',')) if x else set())
            activities[name] = activity.set_index('refersTo')

        if not activities:
            return DataFrame()

        return concat(activities.values(), axis=1, join='outer', keys=activities.keys()) \
              .sort_index(key=lambda x: x.map(key))

    def getById(self, identifiers: str | list[str]) -> DataFrame:
        # Normalize identifiers to a string
        identifiers = id_join(identifiers, ', ')
        return self.sql_df(filter_condition=f'WHERE A.refersTo IN ({identifiers})')

    def getAllActivities(self) -> DataFrame:
        return self.sql_df()

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> DataFrame:
        return self.sql_df(filter_condition=f"WHERE A.institute LIKE '%{partialName}%'")

    def getActivitiesByResponsiblePerson(self, partialName: str) -> DataFrame:
        return self.sql_df(filter_condition=f"WHERE A.person LIKE '%{partialName}%'")

    def getActivitiesUsingTool(self, partialName: str) -> DataFrame:
        return self.sql_df(filter_condition=f"WHERE T.tool LIKE '%{partialName}%'")

    def getActivitiesStartedAfter(self, date: str) -> DataFrame:
        return self.sql_df(filter_condition=f"WHERE A.start >= '{date}'")

    def getActivitiesEndedBefore(self, date: str) -> DataFrame:
        return self.sql_df(filter_condition=f"WHERE A.end <= '{date}'")

    def getAcquisitionsByTechnique(self, partialName: str) -> DataFrame:
        return self.sql_df({'Acquisition': ACQUISITION_ATTRIBUTES}, filter_condition=f"WHERE A.technique LIKE '%{partialName}%'")