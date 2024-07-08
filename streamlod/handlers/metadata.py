from .base import UploadHandler, QueryHandler
from ..utils import chunker, id_join, key

from rdflib.graph import Graph
from rdflib.term import URIRef, Literal
from rdflib.namespace import Namespace, DC, FOAF, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from urllib.error import URLError
from pandas import DataFrame, read_csv
from sparql_dataframe import get

CHO_ATTRIBUTES = { # Attribute : required
    'identifier': True,
    'className': True,
    'title': True,
    'date': False,
    'hasAuthor': False,
    'owner': True,
    'place': True,
}

EDM = Namespace('http://www.europeana.eu/schemas/edm/')
MAG = Namespace('https://agonymagnolia.github.io/data-science#')

OBJECT_QUERY = """
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX edm: <http://www.europeana.eu/schemas/edm/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT DISTINCT ?class ?identifier ?title ?owner ?place ?date ?author_id ?author_name
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
                        dc:identifier ?author_id ;
                        foaf:name ?author_name .
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
        self.entities: set[str] = set() # Set of entity IDs inside the database

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False

        endpoint = self.getDbPathOrUrl()
        store = self.store

        try:
            store.open((endpoint, endpoint))
            self.entities.update(identifier.value for identifier in store.objects(predicate=DC.identifier))
            store.close()
            return True

        except URLError as e:
            print(e)
            return False

    def _to_rdf(self, array: 'numpy.ndarray', graph: Graph) -> None:
        for row in array:
            subject = MAG['CHO-' + row[0]]

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

                    author = MAG['Person-' + identifier]

                    if identifier not in self.entities:
                        self.entities.add(identifier)
                        graph.add((author, RDF.type, EDM.Agent))
                        graph.add((author, DC.identifier, Literal(identifier)))
                        graph.add((author, FOAF.name, Literal(name)))
                    
                    graph.add((subject, DC.creator, author))


    def pushDataToDb(self, path: str) -> bool:
        endpoint = self.getDbPathOrUrl()
        store = self.store
        # Link graph to store to commit directly the triples to the database
        graph = Graph(store)

        df = read_csv(
            path,
            header=0,
            names=CHO_ATTRIBUTES,
            dtype='string',
            on_bad_lines='skip',
            engine='c',
            memory_map=True,
        )

        for col in df: 
            df[col] = df[col].str.strip() # trim spaces in every column

        # Drop entities already in database or duplicated
        df = df[~df['identifier'].isin(self.entities) & ~df.duplicated('identifier')]

        if df.empty:
            return True

        self.entities.update(df['identifier'])

        # Change corresponding entries based on dictionary, others are
        # turned to NaN
        df.className = df.className.map(self._csv_map)

        # Drop every entity non compliant to the data model
        validate = [attr for attr, required in CHO_ATTRIBUTES.items() if required]
        df = df[df[validate].notna().all(axis=1)]

        for class_name in df.className.unique():
            graph.add((MAG[class_name], RDFS.subClassOf, EDM.PhysicalThing))

        # Convert DataFrame to numpy ndarray for better performance
        array = df.to_numpy(dtype=str, na_value='')

        # Commit array data in chunks to prevent reaching HTTP request size limit
        commits = list()
        for chunk in chunker(array, 300):
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


class MetadataQueryHandler(QueryHandler):
    def getById(self, identifiers: str | list[str]) -> DataFrame:
        # Normalize identifiers to a string
        identifiers = id_join(identifiers)
        # Construct values clause for SPARQL query, specifying identifiers
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
            .sort_values(by='identifier', key=lambda x: x.map(key), ignore_index=True)
        df['class'] = df['class'].str.replace(str(MAG), '')

        return df

    def getAuthorsOfCulturalHeritageObject(self, objectId: str | list[str]) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        objectId = id_join(objectId)
        # The ^ symbol in SPARQL reverses the subject and object of the triple
        # pattern, concisely allowing the subject of the previous triples to
        # be used as the object of the current triple. In this case, this
        # chained triple pattern means that the person identified by
        # ?internalId is the creator (object of dc:creator predicate) of an
        # implicit cultural heritage object with identifier 'objectId'.
        query = PERSON_QUERY + \
        f"""               ^dc:creator / dc:identifier ?objectId .

        VALUES ?objectId {{ {objectId} }}

    }} ORDER BY ?name
        """
        df = get(endpoint, query, True).astype('string')

        return df

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> DataFrame:
        endpoint = self.getDbPathOrUrl()
        personId = id_join(personId)
        # The / symbol in SPARQL allows for an implicit unnamed node that is
        # the object of the first triple pattern and the subject of the
        # second, which in this case is an author with a certain 'personId'.
        # Because 'personId' was not linked to ?hasAuthor, the query retrieves
        # all the authors associated with the cultural heritage object,
        # regardless of which specific author was used to select that object.
        query = OBJECT_QUERY + \
        f"""
                ?internalId dc:creator / dc:identifier ?personId .
        VALUES ?personId {{ {personId} }}
    }}
        """
        df = get(endpoint, query, True) \
            .astype('string') \
            .sort_values(by='identifier', key=lambda x: x.map(key), ignore_index=True)
        df['class'] = df['class'].str.replace(str(MAG), '')

        return df