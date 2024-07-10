from typing import Union, List, Set, Generator
import pandas as pd
from rdflib.namespace import Namespace, DC, FOAF, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from urllib.parse import quote_plus
from urllib.error import URLError
from sparql_dataframe import get

from streamlod.handlers.base import UploadHandler, QueryHandler
from streamlod.utils import id_join, key

# Namespaces
EDM = Namespace('http://www.europeana.eu/schemas/edm/')
LOC = Namespace('https://agonymagnolia.github.io/data-science#') # This is the local namespace

"""
Attribute mappings
 Keys: Attribute names in CSV order
 Values: 
  0. Attribute order for output DataFrame and object initialization
  1. Required
  2. Separator if multiple
  3. RDF Predicate
  4. Value type (if relation, 0. Subentity, 1. Regex pattern)
"""

CHO_ATTRIBUTES = {
    'identifier': (1, True, None, DC.identifier, ''),
    'class': (0, True, None, RDF.type, LOC),
    'title': (2, True, None, DC.title, ''),
    'date': (5, False, None, DC.date, ''),
    'hasAuthor': (6, False, '; ', DC.creator, ('Person', r"^(?P<name>.+?)\s*?\(\s*(?P<identifier>.+?)\s*\)\s*$")),
    'owner': (3, True, None, EDM.currentLocation, ''),
    'place': (4, True, None, DC.coverage, '')
}

PERSON_ATTRIBUTES = {
    'identifier': (7, True, None, DC.identifier, ''),
    'name': (8, True, None, FOAF.name, '')
}

"""
Entity mappings
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
    # Mapping of csv values to data model classes
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
        super().__init__()
        self.store = SPARQLUpdateStore(autocommit=False, context_aware=False) # Database connection only on commit
        self.store.method = 'POST'
        self.identifiers: Set[str] = set()

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

    def _validateDF(self, df: pd.DataFrame, entity: str, to_validate: List[str]) -> pd.DataFrame:
        """
        Preprocesses the input DataFrame:
        1. Strips leading and trailing whitespace from all columns.
        2. Filters out rows with identifiers that already exist in the database or are duplicated within the DataFrame.
        3. Conforms the 'class' column to a controlled vocabulary.
        4. Drops rows that do not comply with the required attributes defined in the entity's attribute mapping.
        5. Sets the DataFrame index to the URI (entity type + identifier).
        """
        for col in df: 
            df[col] = df[col].str.strip() # Trim spaces in every column

        # Drop entities already in database or duplicated
        df = df[~df.identifier.isin(self.identifiers) & ~df.duplicated('identifier')]

        if df.empty:
            return df

        self.identifiers.update(df.identifier)

        if 'class' in df:
            # Change values based on the corresponding entries of a dictionary, others are turned to NaN
            df.loc[:, 'class'] = df['class'].map(self._csv_map)

        # Drop entities non compliant to the data model
        df = df[df[to_validate].notna().all(axis=1)]

        df.index = (f'{entity}-' + df.identifier).map(lambda x: LOC[quote_plus(x)].n3())

        return df

    def toRDF(self, df: pd.DataFrame, entity: str = 'CHO') -> Generator[str, None, None]:
        """
        Maps the validated DataFrame to RDF triples via an IdentifiableEntity mapping dictionary:
        1. Generates RDF type triples for the class or its subclasses.
        2. Maps every attribute of the class to the corresponding DataFrame column, RDF predicate and value type (Literal, external URI or internal relation).
        3. Traverse vertically each column, linking indexed subjects to single or multiple values via the related predicate.
        3. Recursively handles nested relation entities.

        Returns a generator that yields RDF triples as strings.
        """
        entity_type, mapping = IDENTIFIABLE_ENTITIES[entity]
        to_validate = [attr for attr, specs in mapping.items() if specs[1]]
        df = self._validateDF(df, entity, to_validate)

        if 'class' in mapping:
            for class_name in df['class'].unique():
                yield f'<{LOC[class_name]}> <{RDFS.subClassOf}> <{entity_type}> .'
        else:
            for subject in df.index:
                yield f'{subject} <{RDF.type}> <{entity_type}> .'

        for attr, (_, _, sep, predicate, value_type) in mapping.items():
            # Dropna can be safely performed because required attribute columns were already validated.
            # Alignment is guaranteed by the Series index (the URI).
            if sep:
                col = df[attr].str.split(sep).explode().dropna()
            else:
                col = df[attr].dropna()

            if isinstance(value_type, Namespace):
                for subject, obj in zip(col.index, col.to_list()):
                    yield f'{subject} <{predicate}> <{value_type[obj]}> .'

            elif isinstance(value_type, tuple):
                rel_entity, regex = value_type
                rel_df = col.str.extract(regex).dropna(subset='identifier')
                yield from self.toRDF(rel_df, rel_entity)

                for subject, rel_id in zip(rel_df.index, rel_df.identifier.to_list()):
                    yield f'{subject} <{predicate}> <{LOC[f"{rel_entity}-" + quote_plus(rel_id)]}> .'

            else:
                for subject, obj in zip(col.index, col.to_list()):
                    yield f'{subject} <{predicate}> "{obj}" .'

    def pushDataToDb(self, path: str) -> bool:
        endpoint = self.getDbPathOrUrl()
        store = self.store

        df = pd.read_csv(
            path,
            header=0,
            names=list(CHO_ATTRIBUTES),
            dtype='string',
            on_bad_lines='skip',
            engine='c',
            memory_map=True,
        )
        # It's not meant to be done in this way, but it's beautiful
        store._transaction().append(f'INSERT DATA {{ {" ".join(self.toRDF(df))} }}')

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