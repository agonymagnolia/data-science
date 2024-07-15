from typing import List, Iterable
import os
import pandas as pd
try:
    from rich.pretty import pprint as rich_print
except:
    from pprint import pprint as rich_print


WIDTH = os.get_terminal_size().columns
pd.set_option('display.width', WIDTH)
pd.set_option('display.max_rows', 999)

def max_length(series: Iterable) -> int:
    max_l = 0
    for item in series:
        if isinstance(item, (set, list)):
            item_l = sum(len(str(e)) + 1 for e in item) + 1
        else:
            item_l = len(str(item))
        if item_l > max_l:
            max_l = item_l
    return max_l

def max_threshold(L: List[int], width: int) -> int:
    low, high = 0, max(L)
    best_T = low

    while low <= high:
        T = (low + high) // 2
        if sum(min(x, T) for x in L) + 2 <= width:
            best_T = T
            low = T + 1
        else:
            high = T - 1

    return best_T

def unique_rows(df):
    unique_rows = set()

    for i in range(len(df)):
        row = tuple(df.iloc[i, :-2])

        if row in unique_rows:
            df.iloc[i, :-2] = '='
        else:
            unique_rows.add(row)

    return df

def single_print(df: pd.DataFrame) -> None:
    if df.empty:
        print(df)
        print('')
        return None

    df = unique_rows(df)

    indexw = max_length(df.index)
    colsw = [indexw] + [max(len(col), max_length(df[col])) + 2 for col in df.columns]

    if sum(colsw) + 2 <= WIDTH:
        print(df)
        print('')
        return None

    max_colwidth = max_threshold(colsw, WIDTH)
    if max_colwidth >= 10:
        with pd.option_context("display.max_colwidth", max_colwidth):
            print(df)
        print('')
        return None

    with pd.option_context("display.max_colwidth", 10):
        print(df)
    print('')
    return None


def stack_print(df: pd.DataFrame) -> None:
    if df.empty:
        print(df)
        print('')
        return None

    df = df.stack(level=0, sort=False)

    indexw = max_length(df.index)
    colsw = [indexw] + [max(len(col), max_length(df[col])) + 2 for col in df.columns]

    if sum(colsw) + 2 <= WIDTH:
        print(df)
        print('')
        return None

    max_colwidth = max_threshold(colsw, WIDTH)
    if max_colwidth >= 10:
        with pd.option_context("display.max_colwidth", max_colwidth):
            print(df)
        print('')
        return None

    with pd.option_context("display.max_colwidth", 10):
        print(df)
    print('')
    return None

def subtitle_print(title: str) -> None:
    print(f"\n{'—' * WIDTH}\n\n * {title}\n")

def title_print(title: str) -> None:
    fill = (WIDTH - (len(title) + 2))
    l_delim = fill // 2
    r_delim = l_delim if fill % 2 == 0 else l_delim + 1
    print(f"\n{'—' * WIDTH}\n\n{' ' * l_delim} {title} {' ' * r_delim}")