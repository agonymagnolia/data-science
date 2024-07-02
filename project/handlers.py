from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import DC, FOAF, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import DataFrame, Series, read_csv, concat, json_normalize, read_sql_query, NA, set_option
from sparql_dataframe import get
from json import load
from sqlite3 import connect

set_option('display.max_rows', 500)

EDM = Namespace('http://www.europeana.eu/schemas/edm/')
MAG = Namespace('https://agonymagnolia.github.io/data-science#')

OBJECT_QUERY = """
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX edm: <http://www.europeana.eu/schemas/edm/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT DISTINCT ?internalId ?class ?identifier ?title ?owner ?place ?date ?hasAuthor ?author_identifier ?author_name
    WHERE {
            ?class rdfs:subClassOf edm:PhysicalThing .
            ?internalId a ?class ;
                        dc:identifier ?identifier ;
                        dc:title ?title ;
                        edm:currentLocation ?owner ;
                        dc:coverage ?place .

            OPTIONAL { ?internalId dc:date ?date . }
            OPTIONAL {
                       ?internalId dc:creator ?hasAuthor . 
                       ?hasAuthor a edm:Agent;
                                  dc:identifier ?author_identifier;
                                  foaf:name ?author_name .
                     }
    """
PERSON_QUERY = """
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX edm: <http://www.europeana.eu/schemas/edm/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT DISTINCT ?internalId ?identifier ?name
    WHERE {
            ?internalId a edm:Agent ;
                        dc:identifier ?identifier ;
                        foaf:name ?name .
    """

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
    def pushDataToDb(self, path: str):
        pass


class MetadataUploadHandler(UploadHandler): # Francesca
    def __init__(self) -> None:
        super().__init__()
        self.store = SPARQLUpdateStore(autocommit=False, context_aware=False, dirty_reads=True) # ! database connection only on commit
        self.store.setTimeout(60)
        self.store.method = 'POST'
        self.classDict = {
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
        self.entities: set[URIRef] = set() # set of entities

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False

        store = self.store
        endpoint = self.getDbPathOrUrl()

        try:
            store.open((endpoint, endpoint))
            self.entities.update(store.subjects(predicate=RDF.type))
            store.close()
            return True
        except Exception:
            return False

    def _chunker(self, array: 'numpy.ndarray', size: int) -> 'Generator':
        return (array[pos:pos + size] for pos in range(0, len(array), size)) # size is the step of the range

    def _mapper(self, array: 'numpy.ndarray', graph: Graph) -> None:
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
        endpoint = self.getDbPathOrUrl()
        if not endpoint:
            return False
        store = self.store
        graph = Graph(store) # link graph to store to commit directly the graph triples to the database
        df = read_csv(
            path,
            header=0,
            names=['identifier', 'className', 'title', 'date', 'hasAuthor', 'owner', 'place'],
            dtype='string',
            on_bad_lines='skip',
            engine='c',
            memory_map=True,
            )

        for c in df: 
            df[c] = df[c].str.strip() # trim spaces in every column

        df.className = df.className.map(self.classDict) # change corresponding entries based on dictionary, others are turned to NaN
        df.dropna(subset=['identifier', 'className', 'title', 'owner', 'place'], inplace=True) # drop every entity non compliant to the data model
        df.drop_duplicates(subset='identifier', inplace=True) # drop potential duplicated entities

        for class_name in df.className.unique():
            graph.add((MAG[class_name], RDFS.subClassOf, EDM.PhysicalThing))

        array = df.to_numpy(dtype=str, na_value='') # convert DataFrame to numpy ndarray for better performance
        commits = list()

        for chunk in self._chunker(array, 300): # commit array data in chunks to prevent reaching HTTP request size limit
            self._mapper(chunk, graph)

            try:
                store.open((endpoint, endpoint))
                store.commit()
                store.close()
                commits.append(True)
            except Exception as e:
                print(e)
                store.rollback()
                commits.append(False)

        return all(commits)


class ProcessDataUploadHandler(UploadHandler): # Alberto
    def _mapper(self, label: str) -> dict[str, str]:
        return {
            'object id': 'internalId',
            f'{label}.responsible institute': 'institute',
            f'{label}.responsible person': 'person',
            f'{label}.technique': 'technique',
            f'{label}.tool': 'tool',
            f'{label}.start date': 'start',
            f'{label}.end date': 'end'
        }

    def pushDataToDb(self, path: str) -> bool:
        with open(path, 'r', encoding='utf-8') as f:
            try:
                json_doc = load(f)
            except ValueError:
                return False

        df = json_normalize(json_doc)
        df.drop_duplicates(subset='object id', inplace=True) # drop potential duplicated entities
        df['refersTo'] = 'cho-' + df['object id']
        columns = ['internalId', 'institute', 'person', 'tool', 'start', 'end', 'refersTo']

        try:
            activities = {
                'Acquisition': df.rename(columns=self._mapper('acquisition'))[columns + ['technique']],
                'Processing': df.rename(columns=self._mapper('processing'))[columns],
                'Modelling': df.rename(columns=self._mapper('modelling'))[columns],
                'Optimising': df.rename(columns=self._mapper('optimising'))[columns],
                'Exporting': df.rename(columns=self._mapper('exporting'))[columns],
            }
        except KeyError:
            return False

        tools = DataFrame()
        for name, activity in activities.items():
            tool = activity.pop('tool')
            for c in activity: 
                activity[c] = activity[c].str.strip() # trim spaces in every column

            activity.replace(r'', NA, inplace=True)
            activity.dropna(subset=['institute', 'internalId'], inplace=True) # drop every entity non compliant to the data model
            activity.internalId = name.lower() + '-' + activity.internalId

            tool = concat([activity.internalId, tool], axis=1).dropna(subset=['internalId']).explode('tool')
            tools = concat([tools, tool], axis=0, ignore_index=True)

        # Add tables to the database
        database = self.getDbPathOrUrl()
        with connect(database) as con:
            for name, activity in activities.items():
                activity.to_sql(name, con, if_exists='replace', index=False, dtype='TEXT')
            tools.to_sql('Tool', con, if_exists='replace', index=False, dtype='TEXT')

            # Print for check
            for name in activities.keys():
                print(name)
                print(read_sql_query(f"SELECT * FROM {name}", con, dtype='string'))
            print('Tool')
            print(read_sql_query(f"SELECT * FROM Tool", con, dtype='string'))

        return True
        

# -----------------------------------------------------------------------------
# -- Query Handlers

class QueryHandler(Handler):
    def getById(self, identifier: str):
        pass


class MetadataQueryHandler(QueryHandler): # Francesca
    def _alphanumeric_sort(self, val):
        return (0, int(val)) if val.isdigit() else (1, val)

    def getByInternalIds(self, identifiers: Series) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        newline = '\n                                '
        query = OBJECT_QUERY + f"""
            VALUES ?internalId {{
                                {newline.join(MAG[identifier].n3() for identifier in identifiers.dropna().unique())}
                               }}
          }}
            """
        df = get(endpoint, query, True).astype('string')
        df['internalId'] = df['internalId'].str.replace(str(MAG), '')
        df['class'] = df['class'].str.replace(str(MAG), '')
        df['hasAuthor'] = df['hasAuthor'].str.replace(str(MAG), '')

        return df

    def getById(self, identifier: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        values = f"""
            VALUES ?identifier {{ "{identifier}" }}
          }}
            """
        query = OBJECT_QUERY + values
        df = get(endpoint, query, True).astype('string')

        if df.empty:
            query = PERSON_QUERY + values
            df = get(endpoint, query, True).astype('string')

        else:
            df['class'] = df['class'].str.replace(str(MAG), '')
            df['hasAuthor'] = df['hasAuthor'].str.replace(str(MAG), '')

        df['internalId'] = df['internalId'].str.replace(str(MAG), '')

        return df

    def getAllPeople(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = PERSON_QUERY + '      } ORDER BY ?name'
        df = get(endpoint, query, True).astype('string')
        df['internalId'] = df['internalId'].str.replace(str(MAG), '')

        return df

    def getAllCulturalHeritageObjects(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = OBJECT_QUERY + '      }'
        df = get(endpoint, query, True).astype('string').sort_values(by='identifier', key=lambda x: x.map(self._alphanumeric_sort), ignore_index=True)
        df['internalId'] = df['internalId'].str.replace(str(MAG), '')
        df['class'] = df['class'].str.replace(str(MAG), '')
        df['hasAuthor'] = df['hasAuthor'].str.replace(str(MAG), '')

        return df

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = PERSON_QUERY + f'        ?internalId ^dc:creator / dc:identifier "{objectId}" .\n          }} ORDER BY ?name'
        df = get(endpoint, query, True).astype('string')
        df['internalId'] = df['internalId'].str.replace(str(MAG), '')

        return df

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = OBJECT_QUERY + f'\n            ?internalId dc:creator / dc:identifier "{personId}" .\n          }}'
        df = get(endpoint, query, True).astype('string').sort_values(by='identifier', key=lambda x: x.map(self._alphanumeric_sort), ignore_index=True)
        df['internalId'] = df['internalId'].str.replace(str(MAG), '')
        df['class'] = df['class'].str.replace(str(MAG), '')
        df['hasAuthor'] = df['hasAuthor'].str.replace(str(MAG), '')

        return df


class ProcessDataQueryHandler(QueryHandler): # Lin
    def getAllActivities(self) -> DataFrame:
        pass

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> DataFrame:
        pass

    def getActivitiesByResponsiblePerson(self, partialName: str) -> DataFrame:
        pass

    def getActivitiesUsingTool(self, partialName: str) -> DataFrame:
        pass

    def getActivitiesStartedAfter(self, date: str) -> DataFrame:
        pass

    def getActivitiesEndedBefore(self, date: str) -> DataFrame:
        pass

    def getAcquisitionsByTechnique(self, partialName: str) -> DataFrame:
        pass