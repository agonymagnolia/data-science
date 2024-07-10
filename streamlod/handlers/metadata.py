from typing import Union, List, Set, Generator, Mapping
import pandas as pd
import numpy as np
from rdflib.graph import Graph
from rdflib.term import URIRef, Literal
from rdflib.namespace import Namespace, DC, FOAF, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from urllib.error import URLError
from sparql_dataframe import get
import builtins
import rdflib.namespace

from streamlod.handlers.base import UploadHandler, QueryHandler
from streamlod.utils import chunker, id_join, key

EDM = Namespace('http://www.europeana.eu/schemas/edm/')
LOC = Namespace('https://agonymagnolia.github.io/data-science#') # This is the local namespace

"""
Attribute mapping
 Keys: Attribute names in CSV order
 Values: 
  0. Attribute order for output DataFrame and object init
  1. Required
  2. Separator if multiple else ""
  3. RDF Predicate
  4. Value type (if relation, 0. Subentity, 1. Regex pattern)
"""

CHO_ATTRIBUTES = {
    'identifier': (1, True, '', DC.identifier, ''),
    'class': (0, True, '', RDF.type, LOC),
    'title': (2, True, '', DC.title, ''),
    'date': (5, False, '', DC.date, ''),
    'hasAuthor': (6, False, '; ', DC.creator, ('Person', r"^(?P<name>.+?)\s*?\((?P<identifier>\w+\:\w+)\)$")),
    'owner': (3, True, '', EDM.currentLocation, ''),
    'place': (4, True, '', DC.coverage, '')
}

PERSON_ATTRIBUTES = {
    'identifier': (7, True, '', DC.identifier, ''),
    'name': (8, True, '', FOAF.name, '')
}

"""
Entity mapping
 Keys: internalId prefix
 Values:
  0. Entity class or superclass
  1. Attribute mapping
"""

IDENTIFIABLE_ENTITIES = {
    'CHO': (EDM.PhysicalThing, CHO_ATTRIBUTES),
    'Person': (EDM.Agent, PERSON_ATTRIBUTES), 
}

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

    def __init__(self):
        # The store configuration is initialised as an instance variable,
        # each MetadataUploadHandler instance has its own store
        super().__init__()

        # ! Database connection only on commit !
        self.store = SPARQLUpdateStore(autocommit=False, context_aware=False)
        self.store.setTimeout(60)
        self.store.method = 'POST'
        self.identifiers: Set[str] = set() # Set of entity IDs inside the database

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False

        endpoint = self.getDbPathOrUrl()
        store = self.store

        try:
            store.open((endpoint, endpoint))
            self.identifiers.update(identifier.value for identifier in store.objects(predicate=DC.identifier))
            store.close()
            return True

        except URLError as e:
            print(e)
            return False

    def _to_rdf(self, array: np.ndarray, graph: Graph) -> None:

        for row in array:
            subject = LOC['CHO-' + row[0]]

            graph.add((subject, DC.identifier, Literal(row[0])))
            graph.add((subject, RDF.type, LOC[row[1]]))
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

                    author = LOC['Person-' + identifier]

                    if identifier not in self.identifiers:
                        self.identifiers.add(identifier)
                        graph.add((author, RDF.type, EDM.Agent))
                        graph.add((author, DC.identifier, Literal(identifier)))
                        graph.add((author, FOAF.name, Literal(name)))
                    
                    graph.add((subject, DC.creator, author))


    def pushDataToDb(self, path: str) -> bool:
        endpoint = self.getDbPathOrUrl()
        # Link graph to store to commit directly the triples to the database
        store = self.store
        graph = Graph(store)

        df = pd.read_csv(
            path,
            header=0,
            names=list(CHO_ATTRIBUTES),
            dtype='string',
            on_bad_lines='skip',
            engine='c',
            memory_map=True,
        )

        def clean(df: pd.DataFrame, entity: str = 'CHO') -> pd.DataFrame:
            mapping = IDENTIFIABLE_ENTITIES[entity][1]
            for col in df: 
                df[col] = df[col].str.strip() # trim spaces in every column

            # Drop entities already in database or duplicated
            df = df[~df['identifier'].isin(self.identifiers) & ~df.duplicated('identifier')]

            if df.empty:
                return df

            self.identifiers.update(df['identifier'])

            if 'class' in df:
                # Change corresponding entries in 'class' based on dictionary, others are turned to NaN
                df.loc[:, 'class'] = df['class'].map(self._csv_map)

            # Drop every entity non compliant to the data model
            validate = [attr for attr, required in mapping.items() if required[1]]
            df = df[df[validate].notna().all(axis=1)]

            df.index = f'{entity}-' + df['identifier']
            return df

        def mapper(df: pd.DataFrame, entity: str = 'CHO') -> Generator:
            df = clean(df, entity)
            entity_type, mapping = IDENTIFIABLE_ENTITIES[entity]
            if 'class' in mapping:
                for class_name in df['class'].unique():
                    yield f'<{LOC[class_name]}> <{RDFS.subClassOf}> <{entity_type}> .'
            else:
                for subject in df.index:
                    yield f'<{LOC[subject]}> <{RDF.type}> <{entity_type}> .'

            for attr, (_, required, sep, predicate, value_type) in mapping.items():
                col = df[attr]
                match type(value_type):
                    case rdflib.namespace.Namespace:
                        if sep:
                            col = col.str.split(sep).explode()
                        if required:
                            for subject, obj in zip(df.index, col):
                                yield f'<{LOC[subject]}> <{predicate}> <{value_type[obj]}> .'
                        else:
                            for subject, obj in zip(df.index, col):
                                if pd.notna(obj):
                                    yield f'<{LOC[subject]}> <{predicate}> <{value_type[obj]}> .'
                    case builtins.tuple:
                        if sep:
                            col = col.str.split(sep).explode()
                        subentity, regex = value_type
                        subdf = col.str.extract(regex)
                        yield from mapper(subdf, subentity)
                        for subject, subid in zip(subdf.index, subdf['identifier']):
                            if pd.notna(subid):
                                yield f'<{LOC[subject]}> <{predicate}> <{LOC[f"{subentity}-" + subid]}> .'
                    case _:
                        if sep:
                            col = col.str.split(sep).explode()
                        if required:
                            for subject, obj in zip(df.index, col):
                                yield f'<{LOC[subject]}> <{predicate}> "{obj}" .'
                        else:
                            for subject, obj in zip(df.index, col):
                                if pd.notna(obj):
                                    yield f'<{LOC[subject]}> <{predicate}> "{obj}" .'


        store._transaction().append(f'INSERT DATA {{ {" ".join(mapper(df))} }}')
        try:
            store.open((endpoint, endpoint))
            store.commit()
            store.close()
            return True
        except URLError as e:
            print(e)
            store.rollback()
            return False


class MetadataQueryHandler(QueryHandler):
    def getById(self, identifiers: Union[str, List[str]]) -> pd.DataFrame:
        # Normalize identifiers to a string
        identifiers = id_join(identifiers)
        # Construct values clause for SPARQL query, specifying identifiers as parameters
        value_clause = f"""
        VALUES ?identifier {{ {identifiers} }}
    }}
        """
        endpoint = self.getDbPathOrUrl()

        # First try to look for cultural heritage objects
        query = OBJECT_QUERY + value_clause
        df = get(endpoint, query, True).astype('string').sort_values(by='identifier', key=lambda x: x.map(key), ignore_index=True)

        # If the object query found nothing, retry looking for people with the same value clause
        if df.empty:
            query = PERSON_QUERY + value_clause
            df = get(endpoint, query, True).astype('string').sort_values(by='name', ignore_index=True)

        # If the object query was successful, remove personal namespace URL from class entities
        else:
            df['class'] = df['class'].str.replace(str(LOC), '')

        return df

    def getAllPeople(self) -> pd.DataFrame:
        query = PERSON_QUERY + '} ORDER BY ?name'
        df = get(self.getDbPathOrUrl(), query, True).astype('string')

        return df

    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:
        query = OBJECT_QUERY + '}'

        # Sort the DataFrame by identifier using an alphanumeric sort key
        df = get(self.getDbPathOrUrl(), query, True) \
            .astype('string') \
            .sort_values(by='identifier', key=lambda x: x.map(key), ignore_index=True)
        df['class'] = df['class'].str.replace(str(LOC), '')

        return df

    def getAuthorsOfCulturalHeritageObject(self, objectId: str | list[str]) -> pd.DataFrame:
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
        df = get(self.getDbPathOrUrl(), query, True).astype('string')

        return df

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> pd.DataFrame:
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
        df = get(self.getDbPathOrUrl(), query, True) \
            .astype('string') \
            .sort_values(by='identifier', key=lambda x: x.map(key), ignore_index=True)
        df['class'] = df['class'].str.replace(str(LOC), '')

        return df