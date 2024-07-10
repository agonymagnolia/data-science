from typing import Union, List, Generator
import numpy as np

def chunker(array: np.ndarray, size: int) -> Generator[np.ndarray, None, None]:
    """
    Split an array into chunks of a specified size (the step of the range).
    """
    return (array[pos:pos + size] for pos in range(0, len(array), size))

def id_join(identifiers: Union[str, int, List[str]], join_char: str = ' '):
    """
    Normalize input identifiers (strings or lists) into a joined string for queries.
    """
    if isinstance(identifiers, (str, int)):
        return f'"{identifiers}"'
    else:
        return join_char.join(f'"{identifier}"' for identifier in identifiers)

def key(val):
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