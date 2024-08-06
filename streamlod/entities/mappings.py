from typing import NamedTuple, Union, Dict, List, TypeVar, TypeAlias, Callable, TypedDict, Optional, Iterable
from rdflib.namespace import Namespace, DC, FOAF, RDF, RDFS
import pandas as pd
from streamlod.utils import key

T = TypeVar('T')
Some: TypeAlias = T | Iterable[T]

# Namedtuples
class Relation(NamedTuple):
    name: str # Related entity name
    pattern: str # Regex pattern

class Attribute(NamedTuple):
    order: int # Attribute order for output DataFrame and object initialization
    required: bool
    sep: Optional[str] # Separator if multiple values accepted
    predicate: str # RDF predicate
    vtype: type | str | Relation # Literal, external URI or internal relation

class EntityMap(TypedDict):
    entity: str
    attributes: Dict[str, Attribute]
    sort_by: str
    key: Optional[Callable[[pd.Series], pd.Series]]

AttributeMap = Dict[str, bool]

# Custom namespaces
EDM = Namespace('http://www.europeana.eu/schemas/edm/')
LOC = Namespace('https://agonymagnolia.github.io/data-science/') # This is the local namespace

# Custom namespace prefixes
NS = {'edm': EDM, 'loc': LOC}

# Entity name on which to map the CSV
BASE: str = 'CHO'

# Identifiable Entities mapping
IDE: Dict[str, EntityMap] = {
    'CHO': {
        'entity': 'edm:PhysicalThing',
        'attributes': {
            'identifier': Attribute(1, True, None, 'dc:identifier', str),
            'class': Attribute(0, True, None, 'rdf:type', 'loc'),
            'title': Attribute(2, True, None, 'dc:title', str),
            'date': Attribute(5, False, None, 'dc:date', str),
            'hasAuthor': Attribute(6, False, '; ', 'dc:creator', Relation('Person', r"^(?P<name>.*?)\s*?\(\s*(?P<identifier>.+?)\s*\)\s*$")),
            'owner': Attribute(3, True, None, 'edm:currentLocation', str),
            'place': Attribute(4, True, None, 'dc:coverage', str)
        },
        'sort_by': 'identifier',
        'key': (lambda x: x.map(key)),
    },
    'Person': {
        'entity': 'edm:Agent',
        'attributes': {
            'identifier': Attribute(0, True, None, 'dc:identifier', str),
            'name': Attribute(1, True, None, 'foaf:name', str)
        },
        'sort_by': 'name',
        'key': None,
    }
}

# Activity mapping
ACTIVITY_ATTRIBUTES: AttributeMap = {
    'refersTo': (True, False),
    'institute': (True, False),
    'person': (False, False),
    'start': (False, False),
    'end': (False, False),
    'tool': (False, True)
}

ACQUISITION_ATTRIBUTES: AttributeMap = {
    'refersTo': (True, False),
    'technique': (True, False),
    'institute': (True, False),
    'person': (False, False),
    'start': (False, False),
    'end': (False, False),
    'tool': (False, True)
}

ACTIVITIES: Dict[str, AttributeMap] = {
    'Acquisition': ACQUISITION_ATTRIBUTES,
    'Processing': ACTIVITY_ATTRIBUTES,
    'Modelling': ACTIVITY_ATTRIBUTES,
    'Optimising': ACTIVITY_ATTRIBUTES,
    'Exporting': ACTIVITY_ATTRIBUTES
}

class MapMeta(type):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        # Prefab queries select and where clause per entity
        cls.query_dict = {entity_name: cls._query_map(entity_name) for entity_name in IDE}

        # Custom prefixes clause
        cls.prefixes = '\n'.join(f'PREFIX {prefix}: <{ns}>'for prefix, ns in NS.items()) + '\n\n'

        # Columns to be sorted and sorting key per entity DataFrame
        cls.sort_by = {entity_name: (cls._sort_map(entity_name), mapping['key']) for entity_name, mapping in IDE.items()}

        # Columns to be stipped of the uri per entity DataFrame
        cls.uri_strip = {entity_name: cls._uri_map(entity_name) for entity_name in IDE}

    def _query_map(cls, entity_name: str) -> tuple[List[str], List[str]]:
        select, where = [], []
        optional_clause = "OPTIONAL {{ {} }}"
        entity_map = IDE[entity_name]
        entity, attrs = entity_map['entity'], entity_map['attributes']

        if 'class' in attrs: # Only the superclass is known
            where.append(f'?class rdfs:subClassOf {entity} .')
        else:
            where.append(f'?s rdf:type {entity} .')

        sorted_attrs = sorted(attrs.keys(), key=lambda k: attrs[k][0])
        for name in sorted_attrs:
            attr = attrs[name]

            select_attr = '?' + name
            triple = f'?s {attr.predicate} {select_attr} .'

            # If relation, select only the related entity attributes, prefixed
            if isinstance((rel := attr.vtype), tuple):
                select2, where2 = cls._query_map(rel.name) # Recursive step
                prefix = rel.name[:1].lower()

                for name2, triple2 in zip(select2, where2[1:]): # Exclude first rdf:type triple
                    select_attr2 = f'?{prefix}_{name2[1:]}' # Remove ? from related attribute name
                    select.append(select_attr2)
                    triple += ('\n        ' + triple2.replace('?s', select_attr).replace(name2, select_attr2))
            else:
                select.append(select_attr)

            if name == 'class' or name == 'identifier':
                where.append(triple)
            else:
                where.append(optional_clause.format(triple))

        return select, where

    def _sort_map(cls, entity_name: str):
        entity_map = IDE[entity_name]
        sort_by, attrs = entity_map['sort_by'], entity_map['attributes']
        result = [sort_by]
        for name, attr in attrs.items():
            if isinstance((rel := attr.vtype), Relation):
                entity_name2 = rel.name
                result += [f'{entity_name2.lower()[:1]}_{name}' for name in cls._sort_map(entity_name2)]
        return result

    def _uri_map(cls, entity_name: str) -> List[tuple[str, str]]:
        result = []
        attrs = IDE[entity_name]['attributes']
        for name, attr in attrs.items():
            if isinstance(attr.vtype, str):
                uri = NS[attr.vtype]
                result.append((name, str(uri)))
            elif isinstance((rel := attr.vtype), Relation):
                entity_name2 = rel.name
                result += [(f'{entity_name2.lower()[:1]}_{name}', uri) for name, uri in cls._uri_map(rel.name)]
        return result