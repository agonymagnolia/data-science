def chunker(array: 'numpy.ndarray', size: int) -> 'Generator':
    # Size is the step of the range
    return (array[pos:pos + size] for pos in range(0, len(array), size))

def id_join(identifiers: str | list[str], join_char: str = ' '):
    # Convert identifiers to a joined string if it is a list
    if isinstance(identifiers, str) or isinstance(identifiers, int):
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