from rdflib.graph import Graph
from rdflib.term import URIRef, Literal
from rdflib.namespace import Namespace, DC, FOAF, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from urllib.error import URLError
from pandas import (
    DataFrame,
    Series,
    read_csv,
    concat,
    json_normalize,
    read_sql_query,
    NA,
    notna,
    set_option
)
from sparql_dataframe import get
from json import load
from sqlite3 import connect, Error

set_option('display.max_rows', 500)
set_option('display.max_colwidth', 33)

EDM = Namespace('http://www.europeana.eu/schemas/edm/')
MAG = Namespace('https://agonymagnolia.github.io/data-science#')

OBJECT_QUERY = """
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX edm: <http://www.europeana.eu/schemas/edm/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT DISTINCT ?class ?identifier ?title ?owner ?place ?date ?hasAuthor_identifier ?hasAuthor_name
    WHERE {
        ?class rdfs:subClassOf edm:PhysicalThing .
        ?internalId a ?class ;
                    dc:identifier ?identifier ;
                    dc:title ?title ;
                    edm:currentLocation ?owner ;
                    dc:coverage ?place .

        OPTIONAL { ?internalId dc:date ?date . }
        OPTIONAL {
            ?hasAuthor ^dc:creator ?internalId ;
                        dc:identifier ?hasAuthor_identifier ;
                        foaf:name ?hasAuthor_name .
        }
    """
PERSON_QUERY = """
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX edm: <http://www.europeana.eu/schemas/edm/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT DISTINCT ?identifier ?name
    WHERE {
        ?internalId a edm:Agent ;
                    dc:identifier ?identifier ;
                    foaf:name ?name ;
    """
ACTIVITIES = ['acquisition', 'processing', 'modelling', 'optimising', 'exporting']
ACTIVITY_COLUMNS = ['internalId', 'refersTo', 'institute', 'person', 'start', 'end', 'tool']
ACQUISITION_COLUMNS = ['internalId', 'refersTo', 'technique', 'institute', 'person', 'start', 'end', 'tool']

def alphanumeric_sort(val):
    """
    Provides a custom sorting key for alphanumeric string identifiers.
    For numeric values returns a tuple (0, int(val)).
    For non-numeric values returns a tuple (1, val).

    In this way, integers always precede strings, avoiding direct comparison.
    Integers and strings are then compared separately within each group
    (numbers with numbers and letters with letters).
    """
    if val.isdigit():
        return (0, int(val))
    else:
        return (1, val)

# -----------------------------------------------------------------------------

class Handler(object):
    def __init__(self) -> None:
        self.dbPathOrUrl = ''
        
    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if newDbPathOrUrl and isinstance(newDbPathOrUrl, str):
            self.dbPathOrUrl = newDbPathOrUrl
            return True
        else:
            return False

# -----------------------------------------------------------------------------
# -- Upload Handlers

class UploadHandler(Handler):
    def pushDataToDb(self, path: str) -> bool:
        return self.getDbPathOrUrl() and path and isinstance (path, str)


class MetadataUploadHandler(UploadHandler): # Francesca
    # Dictionary to map csv values to data model classes is set as a class
    # variable (shared among all the instances of MetadataUploadHandler)
    _csv_map = {
            'Nautical chart': 'NauticalChart',
            'Manuscript plate': 'ManuscriptPlate',
            'Manuscript volume': 'ManuscriptVolume',
            'Printed volume': 'PrintedVolume',
            'Printed material': 'PrintedMaterial',
            'Herbarium': 'Herbarium',
            'Specimen': 'Specimen',
            'Painting': 'Painting',
            'Model': 'Model',
            'Map': 'Map'
        }

    def __init__(self) -> None:
        # The store configuration is initialised as an instance variable,
        # each MetadataUploadHandler instance has its own store
        super().__init__()

        # ! Database connection only on commit !
        self.store = SPARQLUpdateStore(autocommit=False, context_aware=False)
        self.store.setTimeout(60)
        self.store.method = 'POST'
        self.entities: set[URIRef] = set() # Set of entities inside the database

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False

        endpoint = self.getDbPathOrUrl()
        store = self.store

        try:
            store.open((endpoint, endpoint))
            self.entities.update(store.subjects(predicate=RDF.type))
            store.close()
            return True

        except URLError as e:
            print(e)
            return False

    def _chunker(self, array: 'numpy.ndarray', size: int) -> 'Generator':
        # Size is the step of the range
        return (array[pos:pos + size] for pos in range(0, len(array), size))

    def _to_rdf(self, array: 'numpy.ndarray', graph: Graph) -> None:
        for row in array:
            subject = MAG['cho-' + row[0]]

            if subject in self.entities:
                continue

            self.entities.add(subject)
            graph.add((subject, DC.identifier, Literal(row[0])))
            graph.add((subject, RDF.type, MAG[row[1]]))
            graph.add((subject, DC.title, Literal(row[2])))
            graph.add((subject, EDM.currentLocation, Literal(row[5])))
            graph.add((subject, DC.coverage, Literal(row[6])))

            if row[3]:
                graph.add((subject, DC.date, Literal(row[3])))

            if row[4]:
                for author_str in row[4].split('; '):
                    try:
                        i = author_str.index('(')
                    except ValueError:
                        continue

                    name, identifier = author_str[:i-1], author_str[i+1:-1]

                    if not name or not identifier:
                        continue

                    author = MAG['person-' + identifier]

                    if author not in self.entities:
                        self.entities.add(author)
                        graph.add((author, RDF.type, EDM.Agent))
                        graph.add((author, DC.identifier, Literal(identifier)))
                        graph.add((author, FOAF.name, Literal(name)))
                    
                    graph.add((subject, DC.creator, author))


    def pushDataToDb(self, path: str) -> bool:
        if not super().pushDataToDb(path):
            return False

        endpoint = self.getDbPathOrUrl()
        store = self.store
        # Link graph to store to commit directly the triples to the database
        graph = Graph(store)

        df = read_csv(
            path,
            header=0,
            names=[
                'identifier',
                'className',
                'title',
                'date',
                'hasAuthor',
                'owner',
                'place'
            ],
            dtype='string',
            on_bad_lines='skip',
            engine='c',
            memory_map=True,
        )

        for col in df: 
            df[col] = df[col].str.strip() # trim spaces in every column

        # Change corresponding entries based on dictionary, others are
        # turned to NaN
        df.className = df.className.map(self._csv_map)

        # Drop every entity non compliant to the data model
        df.dropna(subset=[
            'identifier',
            'className',
            'title',
            'owner',
            'place'
        ], inplace=True)

        # Drop potential duplicated entities
        df.drop_duplicates(subset='identifier', inplace=True)

        for class_name in df.className.unique():
            graph.add((MAG[class_name], RDFS.subClassOf, EDM.PhysicalThing))

        # Convert DataFrame to numpy ndarray for better performance
        array = df.to_numpy(dtype=str, na_value='')

        # Commit array data in chunks to prevent reaching HTTP request size limit
        commits = list()
        for chunk in self._chunker(array, 300):
            self._to_rdf(chunk, graph)

            try:
                store.open((endpoint, endpoint))
                store.commit()
                store.close()
                commits.append(True)
            except URLError as e:
                print(e)
                store.rollback()
                commits.append(False)

        return all(commits)


class ProcessDataUploadHandler(UploadHandler): # Alberto
    def _json_map(self, activity: str) -> dict[str, str]:
        return {
            'object id': 'internalId',
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
            con.close()
            return True
        except Error as e:
            print(e)
            return False

    def pushDataToDb(self, path: str) -> bool:
        if not super().pushDataToDb(path):
            return False

        with open(path, 'r', encoding='utf-8') as f:
            try:
                json_doc = load(f)
            except ValueError:
                return False

        # Flatten the json file in one DataFrame
        df = json_normalize(json_doc)

        # Drop potential duplicated entities
        df.drop_duplicates(subset='object id', inplace=True)

        # Duplicate the object id column to represent the refersTo relation
        df['refersTo'] = df['object id']

        # Rename columns for each activity, select the relevant columns and
        # store each DataFrame in a dictionary to access easily their names
        # while iterating over them
        activities = dict.fromkeys(ACTIVITIES)
        try:
            for name in activities.keys():
                if name == 'acquisition':
                    activities[name] = df.rename(columns=self._json_map(name))[ACQUISITION_COLUMNS]
                else:
                    activities[name] = df.rename(columns=self._json_map(name))[ACTIVITY_COLUMNS]
        except KeyError as e:
            print(e)
            return False

        # Initialize an empty DataFrame to store tool information
        tools = DataFrame()

        for name, activity in activities.items():
            # Extract tool column from each activity DataFrame
            tool = activity.pop('tool')

            # Trim spaces in every column
            for col in activity:
                activity[col] = activity[col].str.strip()

            # Drop every entity non compliant to the data model
            activity.replace(r'', NA, inplace=True)
            activity.dropna(subset=['institute', 'internalId'], inplace=True)

            # Add activity prefix to the internalId
            activity.internalId = name + '-' + activity.internalId

            # Create a tool_df combining internalId and tool columns,
            # drop rows where internalId is not defined and split each list
            # in the tool column into a separate row, while duplicating the
            # internalId values for each expandend row (explode method)
            activity_tools = concat([activity.internalId, tool], axis=1) \
                  .dropna(subset=['internalId']) \
                  .explode('tool')
            activity_tools.tool = activity_tools.tool.str.strip()

            # Concatenate tool_df with tools DataFrame to accumulate results
            tools = concat([tools, activity_tools], axis=0, ignore_index=True)

        # Add tables to the database
        database = self.getDbPathOrUrl()
        with connect(database) as con:
            for name, activity in activities.items():
                activity.to_sql(name.capitalize(), con, if_exists='replace', index=False, dtype='TEXT')
            tools.to_sql('Tool', con, if_exists='replace', index=False, dtype='TEXT')

        return True
        

# -----------------------------------------------------------------------------
# -- Query Handlers

class QueryHandler(Handler):
    def getById(self, identifiers: str | list[str]):
        # Convert identifiers to a joined string if it is a list
        if isinstance(identifiers, str):
            return f'"{identifiers}"'
        else:
            return ', '.join(f'"{identifier}"' for identifier in identifiers)


class MetadataQueryHandler(QueryHandler): # Francesca
    def getById(self, identifiers: str | list[str]) -> DataFrame:
        # Normalize identifiers to a string
        identifiers = super().getById(identifiers)

        # Construct VALUES clause for SPARQL query, specifying identifiers
        # as parameters
        value_clause = f"""
        VALUES ?identifier {{ {identifiers} }}
    }}
        """
        endpoint = self.getDbPathOrUrl()

        # First try to look for cultural heritage objects
        query = OBJECT_QUERY + value_clause
        df = get(endpoint, query, True).astype('string')

        # If the object query found nothing, retry looking for people with
        # the same value clause
        if df.empty:
            query = PERSON_QUERY + value_clause
            df = get(endpoint, query, True).astype('string')

        # If the object query was successful, remove personal namespace URL
        # from class entities
        else:
            df['class'] = df['class'].str.replace(str(MAG), '')

        return df

    def getAllPeople(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = PERSON_QUERY + '} ORDER BY ?name'
        df = get(endpoint, query, True).astype('string')

        return df

    def getAllCulturalHeritageObjects(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = OBJECT_QUERY + '}'

        # Sort the DataFrame by identifier using an alphanumeric sort key
        df = get(endpoint, query, True) \
            .astype('string') \
            .sort_values(by='identifier', key=lambda x: x.map(alphanumeric_sort), ignore_index=True)
        df['class'] = df['class'].str.replace(str(MAG), '')

        return df

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        # The ^ symbol in SPARQL reverses the subject and object of the triple
        # pattern, concisely allowing the subject of the previous triples to
        # be used as the object of the current triple. In this case, this
        # chained triple pattern means that the person identified by
        # ?internalId is the creator (object of dc:creator predicate) of an
        # implicit cultural heritage object with identifier 'objectId'.
        query = PERSON_QUERY + f'               ^dc:creator / dc:identifier "{objectId}" .\n    }} ORDER BY ?name'
        df = get(endpoint, query, True).astype('string')

        return df

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        # The / symbol in SPARQL allows for an implicit unnamed node that is
        # the object of the first triple pattern and the subject of the
        # second, which in this case is an author with a certain 'personId'.
        # Because 'personId' was not linked to ?hasAuthor, the query retrieves
        # all the authors associated with the cultural heritage object,
        # regardless of which specific author was used to select that object.
        query = OBJECT_QUERY + f'\n        ?internalId dc:creator / dc:identifier "{personId}" .\n    }}'
        df = get(endpoint, query, True) \
            .astype('string') \
            .sort_values(by='identifier', key=lambda x: x.map(alphanumeric_sort), ignore_index=True)
        df['class'] = df['class'].str.replace(str(MAG), '')

        return df


class ProcessDataQueryHandler(QueryHandler): # Anna
    def _sql_query(self, activity_list: list[str], filter_condition: str = '') -> DataFrame:
        db = self.getDbPathOrUrl()
        activities = dict.fromkeys(activity_list)

        with connect(db) as con:
            for name in activities.keys():
                if name == 'acquisition':
                    columns = ACQUISITION_COLUMNS[1:-1]
                else:
                    columns = ACTIVITY_COLUMNS[1:-1]

                columns_str = ", ".join(columns)
                sql = f"""
                    SELECT {columns_str}, GROUP_CONCAT(t2.tool) AS tool
                    FROM {name.capitalize()} AS t1
                    JOIN Tool AS t2
                    ON t1.internalId = t2.internalId
                    {filter_condition}
                    GROUP BY {columns_str};
                    """
                # Execute the query and store the resulting DataFrame in the dictionary
                activities[name] = read_sql_query(sql, con)

        for name, activity in activities.items():
            activity.tool = activity.tool.apply(lambda x: set(x.split(',')) if x else set())
            activities[name] = activity.set_index('refersTo')
        
        df = concat(activities.values(), axis=1, join='outer', keys=activities.keys()) \
            .reset_index(col_level=1) \
            .sort_values(by=('', 'refersTo'), key=lambda x: x.map(alphanumeric_sort), ignore_index=True)

        return df

    def getById(self, identifiers: str | list[str]) -> DataFrame:
        # Normalize identifiers to a string
        identifiers = super().getById(identifiers)
        return self._sql_query(ACTIVITIES, f'WHERE t1.refersTo IN ({identifiers})')

    def getAllActivities(self) -> DataFrame:
        return self._sql_query(ACTIVITIES)

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> DataFrame:
        return self._sql_query(ACTIVITIES, f"WHERE t1.institute LIKE '%{partialName}%'")

    def getActivitiesByResponsiblePerson(self, partialName: str) -> DataFrame:
        return self._sql_query(ACTIVITIES, f"WHERE t1.person LIKE '%{partialName}%'")

    def getActivitiesUsingTool(self, partialName: str) -> DataFrame:
        return self._sql_query(ACTIVITIES, f"WHERE t2.tool LIKE '%{partialName}%'")

    def getActivitiesStartedAfter(self, date: str) -> DataFrame:        
        return self._sql_query(ACTIVITIES, f" WHERE  date(t1.start) >= '{date}'") #ordinati secondo refersTo
    
    def getActivitiesEndedBefore(self, date: str) -> DataFrame:
        return self._sql_query(ACTIVITIES, f" WHERE  date(t1.start) <= '{date}'")

    def getAcquisitionsByTechnique(self, partialName: str) -> DataFrame:
        #return self._sql_query(ACQUISITION_COLUMNS, f"WHERE t1.technique LIKE '%{partialName}%'")
        pass
        

