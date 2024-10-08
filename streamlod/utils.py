from typing import Union, List
import pandas as pd

def id_join(identifiers: Union[str, int, List[str]], join_char: str = ' ') -> str:
    """
    Normalize input identifiers (strings or lists) into a joined string for queries.
    """
    if isinstance(identifiers, (str, int)):
        return f'"{identifiers}"'
    else:
        return join_char.join(f'"{identifier}"' for identifier in identifiers)

def key(val: str) -> tuple[int, Union[int, str]]:
    """
    Provides a custom sorting key for alphanumeric string identifiers.
    For numeric values returns a tuple (0, int(val)).
    For non-numeric values returns a tuple (1, val).

    In this way, integers always precede strings, avoiding direct comparison.
    Integers and strings are then compared separately within each group
    (numbers with numbers and letters with letters).
    """
    if isinstance(val, str):
        if val.isdigit():
            return (0, int(val))
        else:
            return (1, val)
    else:
        return val

rank = {
    'Acquisition': 1,
    'Processing': 2,
    'Modelling': 3,
    'Optimising': 4,
    'Exporting': 5
}

def sorter(s: pd.Index | pd.Series) -> pd.Index | pd.Series:
    if s.name == 'refersTo' or s.name == 'identifier':
        return s.map(key)
    elif s.name == 'class':
        return s.map(rank)
    else:
        return s
