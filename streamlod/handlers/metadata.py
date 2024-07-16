from typing import Union, List, Set, Generator, Optional, Any, Iterable
import pandas as pd
import numpy as np
from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from urllib.parse import quote_plus
from urllib.error import URLError
from SPARQLWrapper import SPARQLWrapper
from io import StringIO

from streamlod.handlers.base import UploadHandler, QueryHandler
import streamlod.entities as entities
from streamlod.entities.mappings import IDE, BASE, NS, Relation, MapMeta
from streamlod.utils import id_join, key


class MetadataUploadHandler(UploadHandler): # Francesca
    def __init__(self):
        super().__init__()
        self.store = SPARQLUpdateStore(autocommit=False, context_aware=False) # Database connection only on commit
        self.store.method = 'POST'

    def setDbPathOrUrl(self, newDbPathOrUrl: str, *, reset: bool = False) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl):
            return False
        elif reset and not self.clearDb():
            return False

        endpoint = self.getDbPathOrUrl()
        store = self.store

        try:
            store.open((endpoint, endpoint))
            store.close()
            return True
        except Exception as e:
            print(e)
            return False

    def _check_class(self, string: str) -> str:
        string = ''.join(word.capitalize() for word in string.split())
        if hasattr(entities, string):
            return string
        else:
            return pd.NA

    def _validateIDE(self, df: pd.DataFrame, entity_name: str) -> pd.DataFrame:
        for col in df: 
            df[col] = df[col].str.strip()

        if 'class' in df:
            df['class'] = df['class'].map(self._check_class, na_action='ignore')
            df.dropna(subset=['identifier', 'class'], inplace=True)
        else:
            df.dropna(subset='identifier', inplace=True)

        df.index = df.identifier.map(lambda identifier: f'loc:{entity_name}-{quote_plus(identifier)}')

        return df

    def toRDF(self, df: pd.DataFrame, entity_name: str = BASE) -> Generator[str, None, None]:
        try:
            entity_map = IDE[entity_name]
            entity, attrs = entity_map['entity'], entity_map['attributes']
        except KeyError as e:
            raise ValueError(f"Entity '{entity_name}' is not defined in the identifiable entities mapping.") from e

        df = self._validateIDE(df, entity_name)

        if 'class' in attrs:
            class_ns = attrs['class'].vtype # Class namespace
            for c in df['class'].unique():
                yield f'{class_ns}:{c} rdfs:subClassOf {entity} .'
        else:
            for s in df.index:
                yield f'{s} rdf:type {entity} .'

        for name, attr in attrs.items():
            p = attr.predicate

            if attr.sep:
                col = df[name].str.split(attr.sep).explode().dropna()
            else:
                col = df[name].dropna()

            if isinstance((ns := attr.vtype), str): # Namespace
                for s, o in zip(col.index, col.to_list()):
                    yield f'{s} {p} {ns}:{o} .'

            elif isinstance((rel := attr.vtype), Relation): # Related entity
                df2 = col.str.extract(rel.pattern).dropna(subset=['identifier'])
                entity_name2 = rel.name

                for s, id2 in zip(df2.index, df2.identifier.to_list()):
                    yield f'{s} {p} loc:{entity_name2}-{quote_plus(id2)} .'

                yield from self.toRDF(df2, entity_name2)

            else:
                for s, o in zip(col.index, col.to_list()):
                    yield f'{s} {p} "{o}" .'

    def pushDataToDb(self, path: str) -> bool:
        if not (endpoint := self.getDbPathOrUrl()):
            print('Exception: Database path not set.')
            return False
        store = self.store
        graph = Graph(store, bind_namespaces='core')
        for prefix, ns in NS.items():
            graph.bind(prefix, ns, override=False, replace=False)

        try:
            df = pd.read_csv(
                path,
                header=0,
                names=IDE[BASE]['attributes'],
                dtype='string',
                on_bad_lines='skip',
                engine='c',
                memory_map=True,
            )
        except FileNotFoundError as e:
            print(e)
            return False
        except ValueError as e:
            print(e)
            return False

        graph.update(f'INSERT DATA {{ {" ".join(self.toRDF(df))} }}')

        try:
            store.open((endpoint, endpoint))
            store.commit()
        except URLError as e:
            print(e)
            store.rollback()
            return False
        except Exception as e:
            print('Metadata push to database failed: Update endpoint is not set.')
            store.rollback()
            return False
        else:
            return True
        finally:
            store.close()

    def clearDb(self) -> bool:
        endpoint = self.getDbPathOrUrl()
        store = self.store
        try:
            store.open((endpoint, endpoint))
            store.update('DELETE { ?s ?p ?o } WHERE { ?s ?p ?o . }')
            store.commit()
        except URLError as e:
            print(e)
            store.rollback()
            return False
        except Exception:
            print('Database clearing failed: Update endpoint is not set.')
            store.rollback()
            return False
        else:
            return True
        finally:
            store.close()

class MetadataQueryHandler(QueryHandler, metaclass=MapMeta):
    def __init__(self):
        super().__init__()
        self.sparql: Optional[SPARQLWrapper] = None

    def setDbPathOrUrl(self, newDbPathOrUrl: str) -> bool:
        if not super().setDbPathOrUrl(newDbPathOrUrl): # Set new endpoint
            return False

        # Initialize sparql wrapper around endpoint
        endpoint = self.getDbPathOrUrl()
        self.sparql = SPARQLWrapper(endpoint, returnFormat='csv')
        self.sparql.setOnlyConneg(True)
        self.sparql.addCustomHttpHeader('Content-type', 'application/sparql-query')
        self.sparql.addCustomHttpHeader('Accept', 'text/csv')
        self.sparql.setMethod('POST')
        self.sparql.setRequestMethod('postdirectly')

        return True

    def _filter_map(self, entity_name: str, by: Union[str, tuple[str, ...]]) -> str:
        attrs = IDE[entity_name]['attributes']

        if isinstance(by, tuple): # Relation
            if len(by) == 3: # Inverse relation
                entity_name2, name2, name = by
                attrs2 = IDE[entity_name2]['attributes']
                predicate2 = '^' + attrs2[name2].predicate
                predicate = attrs2[name].predicate
            else:
                name2, name = by
                attr2 = attrs[name2]
                predicate2, entity_name2 = attr2.predicate, attr2.vtype.name
                predicate = IDE[entity_name2]['attributes'][name].predicate

            return f'?s {predicate2} / {predicate} ?x .'
        else:
            predicate = attrs[by].predicate
            return f'?s {predicate} ?x .'

    def _query(self, query: str) -> pd.DataFrame:
        wrapper = self.sparql
        wrapper.setQuery(query)
        result = wrapper.queryAndConvert()
        _csv = StringIO(result.decode('utf-8'))
        return pd.read_csv(_csv, sep=",", dtype='string')

    def getEntities(
        self,
        entity_name: str = BASE,
        select_only: Optional[str] = None,
        by: Optional[Union[str, tuple[str, ...]]] = None,
        value: Any = None
    ) -> Union[pd.DataFrame, np.ndarray[Any]]:
        select_clause = "SELECT {}"
        where_clause = """
WHERE {{
        {}
}} """
        query_map = self.query_dict[entity_name]
        select, where = list(query_map[0]), list(query_map[1])

        if select_only:
            select = ['?' + select_only]
            where = where[:1] + [triple for triple in where[1:] if select_only in triple or 'class' in triple]

        if by and value:
            value_clause = '\n        VALUES ?x {{ {} }}'
            filter_condition = self._filter_map(entity_name, by)
            where.append(filter_condition)
            where.append(value_clause.format(id_join(value)))

        query = self.prefixes + select_clause.format(' '.join(select)) + where_clause.format('\n        '.join(where))
        df = self._query(query)

        if select_only:
            return df.iloc[:, 0].to_numpy()

        for col, uri in self.uri_strip[entity_name]:
            df[col] = df[col].str.replace(uri, '')

        cols_to_sort, sort_key = self.sort_by[entity_name]
        return df.sort_values(by=cols_to_sort, key=sort_key, ignore_index=True)

    def getById(self, identifier: str) -> pd.DataFrame:
        df = self.getEntities(by='identifier', value=identifier)
        if df.empty:
            df = self.getEntities('Person', by='identifier', value=identifier)
        return df

    def getAllPeople(self) -> pd.DataFrame:
        return self.getEntities('Person')

    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:
        return self.getEntities()

    def getAuthorsOfCulturalHeritageObject(self, objectId: Union[str, List[str]]) -> pd.DataFrame:
        return self.getEntities('Person', by=('CHO', 'hasAuthor', 'identifier'), value=objectId)

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> pd.DataFrame:
        return self.getEntities(by=('hasAuthor', 'identifier'), value=personId)