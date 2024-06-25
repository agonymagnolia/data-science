from rdflib import URIRef, Literal, Namespace
from rdflib.namespace import DC, FOAF, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pandas import DataFrame, read_csv, unique, merge, concat, notna
from sparql_dataframe import get

EDM = Namespace('http://www.europeana.eu/schemas/edm/')
MAG = Namespace('https://agonymagnolia.github.io/data-science/')

###################################################################################################################################

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

###################################################################################################################################

# Upload Handlers

class UploadHandler(Handler):
    def pushDataToDb(self, path: str):
        pass


class MetadataUploadHandler(UploadHandler): # Francesca
    def __init__(self) -> None:
        super().__init__()
        self.store = SPARQLUpdateStore(autocommit=False, context_aware=False, dirty_reads=True) # ! database connection only on commit
        self.store.setTimeout(60)
        self.store.method = 'POST'
        self.entities: set[URIRef] = set() # set of entities
        self.classes: set[URIRef] = set() # set of data model classes

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False

        store = self.store
        endpoint = self.dbPathOrUrl

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
        endpoint = self.dbPathOrUrl

        class_dict = {
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

        df.className = df.className.map(class_dict) # change corresponding entries based on dictionary, others are turned to NaN

        df.dropna(subset=['identifier', 'className', 'title', 'owner', 'place'], inplace=True) # drop every entity non compliant to the data model

        df.drop_duplicates(subset='identifier', inplace=True) # drop potential duplicated entities

        for class_name in df.className.unique():
            if class_name not in self.classes:
                self.classes.add(class_name)
                store.add((MAG[class_name], RDFS.subClassOf, EDM.PhysicalThing))

        array = df.to_numpy(na_value='', dtype=str) # convert DataFrame to numpy ndarray for better performance

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


class ProcessDataUploadHandler(UploadHandler):
    pass

###################################################################################################################################

# Query Handlers

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

        df = df.merge(get(endpoint, query, True), how='left', left_on='hasAuthor', right_on=f'internalId', suffixes=('', '_p'))
        df.drop(columns=['internalId_p'], inplace=True)

        return df

    def getById(self, identifier: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        values = f"""
                    VALUES ?identifier {{ "{identifier}" }}
            }}
            """
        query = self.objectQuery + values
        df = get(endpoint, query, True)

        if df.empty:
            query = self.personQuery + values
            df = get(endpoint, query, True)

        elif df['hasAuthor'].notna().all():
            df = self.mergeAuthors(endpoint, df)

        df.replace({str(MAG): ''}, regex=True, inplace=True)

        return df

    def getAllPeople(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.personQuery+'\n            }'

        df = get(endpoint, query, True)
        df['internalId'].replace({str(MAG): ''}, regex=True, inplace=True)

        return df

    def getAllCulturalHeritageObjects(self) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.objectQuery+'\n            }'

        df = get(endpoint, query, True)
        df = self.mergeAuthors(endpoint, df)

        df.sort_values('identifier', inplace=True, ignore_index=True)
        df.replace({str(MAG): ''}, regex=True, inplace=True)

        return df


    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.personQuery+f'        ?internalId ^dc:creator / dc:identifier "{objectId}" .\n            }}'

        df = get(endpoint, query, True)
        df['internalId'].replace({str(MAG): ''}, regex=True, inplace=True)

        return df

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        query = self.objectQuery+f'        ?internalId dc:creator / dc:identifier "{personId}" .\n            }}'

        df = get(endpoint, query, True)
        df = self.mergeAuthors(endpoint, df)

        df.sort_values('identifier', inplace=True, ignore_index=True)
        df.replace({str(MAG): ''}, regex=True, inplace=True)

        return df


class ProcessDataQueryHandler(QueryHandler):
    pass