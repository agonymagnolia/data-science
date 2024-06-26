from rdflib import URIRef, Literal, Namespace
from rdflib.namespace import DC, FOAF, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import DataFrame, read_csv, unique, merge, concat, notna, json_normalize, read_sql_query
from json import load
from sqlite3 import connect
from SPARQLWrapper import SPARQLWrapper, JSON, SELECT, POST, POSTDIRECTLY
import pprint

EDM = Namespace('http://www.europeana.eu/schemas/edm/')
MAG = Namespace('https://agonymagnolia.github.io/data-science/')

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
        self.classes: set[URIRef] = set() # set of data model classes

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False

        store = self.store
        endpoint = self.getDbPathOrUrl()

        try:
            store.open((endpoint, endpoint))

            for subject in store.subjects(predicate=RDF.type):
                self.entities.add(subject)

            for subject in store.subjects(predicate=RDFS.subClassOf):
                self.classes.add(subject)

            store.close()
            return True

        except Exception:
            return False

    def _chunker(self, array: 'numpy.ndarray', size: int) -> 'Generator':
        return (array[pos:pos + size] for pos in range(0, len(array), size)) # size is the step of the range

    def _mapper(self, array: 'numpy.ndarray', store: SPARQLUpdateStore) -> None:
        for row in array:
            subject = MAG['CulturalHeritageObject-' + row[0]]

            if subject in self.entities:
                continue

            self.entities.add(subject)
            store.add((subject, DC.identifier, Literal(row[0])))
            store.add((subject, RDF.type, MAG[row[1]]))
            store.add((subject, DC.title, Literal(row[2])))
            store.add((subject, EDM.currentLocation, Literal(row[5])))
            store.add((subject, DC.coverage, Literal(row[6])))

            if row[3]:
                store.add((subject, DC.date, Literal(row[3])))

            if row[4]:
                for author_str in row[4].split('; '):

                    try:
                        i = author_str.index('(')
                    except ValueError:
                        continue

                    name, identifier = author_str[:i-1], author_str[i+1:-1]

                    if not name or not identifier:
                        continue

                    author = MAG['Person-' + identifier]

                    if author not in self.entities:
                        self.entities.add(author)
                        store.add((author, RDF.type, EDM.Agent))
                        store.add((author, DC.identifier, Literal(identifier)))
                        store.add((author, FOAF.name, Literal(name)))
                    
                    store.add((subject, DC.creator, author))


    def pushDataToDb(self, path: str) -> bool:

        store = self.store
        endpoint = self.getDbPathOrUrl()

        df = read_csv(
            path,
            header=0,
            names=['identifier', 'className', 'title', 'date', 'hasAuthor', 'owner', 'place'],
            dtype='string',
            on_bad_lines='skip',
            engine='c',
            memory_map=True
            )

        for c in df: 
            df[c] = df[c].str.strip() # trim spaces in every column

        df.className = df.className.map(self.classDict) # change corresponding entries based on dictionary, others are turned to NaN

        df.dropna(subset=['identifier', 'className', 'title', 'owner', 'place'], inplace=True) # drop every entity non compliant to the data model

        df.drop_duplicates(subset='identifier', inplace=True) # drop potential duplicated entities

        for class_name in df.className.unique():
            if class_name not in self.classes:
                self.classes.add(class_name)
                store.add((MAG[class_name], RDFS.subClassOf, EDM.PhysicalThing))

        array = df.to_numpy(dtype=str, na_value='') # convert DataFrame to numpy ndarray for better performance

        commits = list()

        for chunk in self._chunker(array, 1000): # commit array data in chunks to prevent reaching HTTP request size limit
            self._mapper(chunk, store)

            try:
                store.open((endpoint, endpoint))
                store.commit()
                store.close()
                commits.append(True)

            except Exception:
                store.rollback()
                commits.append(False)

        return all(commits)


class ProcessDataUploadHandler(UploadHandler): # Alberto
    def _mapper(self, label: str) -> dict[str, str]:
        base_dict = {
            f'{label}.responsible institute': 'institute',
            f'{label}.responsible person': 'person',
            f'{label}.technique': 'technique',
            f'{label}.tool': 'tool',
            f'{label}.start date': 'start',
            f'{label}.end date': 'end'
        }

        return base_dict

    def pushDataToDb(self, path: str) -> bool:
        with open(path, 'r', encoding='utf-8') as f:
            json_doc = load(f)

        df = json_normalize(json_doc).rename(columns={'object id': 'refersTo'})

        acquisition = df.iloc[:, 0:7].rename(columns=self._mapper('acquisition'))
        processing = df.iloc[:, [0, 7, 8, 9, 10, 11]].rename(columns=self._mapper('processing'))
        modelling = df.iloc[:, [0, 12, 13, 14, 15, 16]].rename(columns=self._mapper('modelling'))
        optimising = df.iloc[:, [0, 17, 18, 19, 20, 21]].rename(columns=self._mapper('optimising'))
        exporting = df.iloc[:, [0, 22, 23, 24, 25, 26]].rename(columns=self._mapper('exporting'))

        activities = {
            'Acquisition': acquisition,
            'Processing': processing,
            'Modelling': modelling,
            'Optimising': optimising,
            'Exporting': exporting
        }

        # Convert tool list to string
        for name, activity in activities.items():
            activity['tool'] = activity['tool'].str.join(',')

        # adding tables to the database
        database = self.getDbPathOrUrl()
        with connect(database) as con:
            for name, activity in activities.items():
                activity.to_sql(name, con, if_exists='replace', index=False, dtype='TEXT')

            # print for check
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                print(table_name)
                table_df = read_sql_query(f"SELECT * FROM {table_name}", con, dtype='string')
                print(table_df)

        return True
        

# -----------------------------------------------------------------------------
# -- Query Handlers

class QueryHandler(Handler):
    def getById(self, identifier: str):
        pass


class MetadataQueryHandler(QueryHandler): # Francesca
    def __init__(self):
        self.objectQuery = """
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX edm: <http://www.europeana.eu/schemas/edm/>

            SELECT DISTINCT ?internalId ?class ?identifier ?title ?owner ?place ?date ?hasAuthor
            WHERE {
                    ?class rdfs:subClassOf edm:PhysicalThing .
                    ?internalId a ?class ;
                                dc:identifier ?identifier ;
                                dc:title ?title ;
                                edm:currentLocation ?owner ;
                                dc:coverage ?place .

                    OPTIONAL {?internalId dc:date ?date . }
                    OPTIONAL {?internalId dc:creator ?hasAuthor . }
            """
        self.personQuery = """
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX edm: <http://www.europeana.eu/schemas/edm/>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>

            SELECT DISTINCT ?internalId ?identifier ?name
            WHERE {
                    ?internalId a edm:Agent ;
                                dc:identifier ?identifier ;
                                foaf:name ?name .
            """

    def to_df(self, endpoint: str, query: str) -> DataFrame:
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)

        sparql.setOnlyConneg(True)
        sparql.setMethod(POST)
        sparql.setRequestMethod(POSTDIRECTLY)
        sparql.setReturnFormat(JSON)

        result = sparql.query().convert()
        meta, record = result['head']['vars'], result['results']['bindings']

        return DataFrame([{key: value['value'] for key, value in entry.items()} for entry in record], columns=meta, dtype='string')

    def mergeAuthors(self, endpoint: str, df: DataFrame) -> DataFrame:
        values = ''
        for author in df['hasAuthor'].dropna().unique():
            values += f"""
                    <{author}>"""
        query = self.personQuery+f"""
                    VALUES ?internalId {{{values}
                    }}
            }}
            """

        df = df.merge(self.to_df(endpoint, query), how='left', left_on='hasAuthor', right_on=f'internalId', suffixes=('', '_p'))
        df.drop(columns=['internalId_p'], inplace=True)

        return df


    def getById(self, identifier: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        values = f"""
                    VALUES ?identifier {{ "{identifier}" }}
            }}
            """
        query = self.objectQuery + values
        df = self.to_df(endpoint, query)

        if df.empty:
            query = self.personQuery + values
            df = self.to_df(endpoint, query)

        elif df['hasAuthor'].notna().all():
            df = self.mergeAuthors(endpoint, df)

        df.replace({str(MAG): ''}, regex=True, inplace=True)

        return df

    def getAllPeople(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.personQuery+'\n            } ORDER BY ?name'

        df = self.to_df(endpoint, query)
        df['internalId'].replace({str(MAG): ''}, regex=True, inplace=True)

        return df

    def getAllCulturalHeritageObjects(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.objectQuery+'\n            } ORDER BY xsd:integer(?identifier)'

        df = self.to_df(endpoint, query)
        df = self.mergeAuthors(endpoint, df)
        print(str(MAG))

        df.replace({str(MAG): ''}, regex=True, inplace=True)

        return df


    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.personQuery+f'        ?internalId ^dc:creator / dc:identifier "{objectId}" .\n            }}'

        df = self.to_df(endpoint, query)
        df['internalId'].replace({str(MAG): ''}, regex=True, inplace=True)

        return df

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.objectQuery+f'        ?internalId dc:creator / dc:identifier "{personId}" .\n            }}'

        df = self.to_df(endpoint, query)
        df = self.mergeAuthors(endpoint, df)

        df.sort_values('identifier', inplace=True, ignore_index=True)
        df.replace({str(MAG): ''}, regex=True, inplace=True)

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