from os import get_terminal_size
from pandas import set_option, option_context, IndexSlice
try:
    from rich.pretty import pprint as rich_print
except:
    from pprint import pprint as rich_print

WIDTH = get_terminal_size().columns
set_option('display.width', WIDTH)

def max_length(series):
    max_l = 0
    for item in series:
        if isinstance(item, (set, list)):
            item_l = sum(len(str(e)) + 1 for e in item) + 1
        else:
            item_l = len(str(item))
        if item_l > max_l:
            max_l = item_l
    return max_l

def max_threshold(L, width):
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

def single_print(df):
    if df.empty:
        print(df)
        print('')
        return None

    indexw = max_length(df.index)
    colsw = [indexw] + [max(len(col), max_length(df[col])) + 2 for col in df.columns]

    if sum(colsw) + 2 <= WIDTH:
        print(df)
        print('')
        return None

    max_colwidth = max_threshold(colsw, WIDTH)
    if max_colwidth >= 10:
        with option_context("display.max_colwidth", max_colwidth):
            print(df)
        print('')
        return None

    with option_context("display.max_colwidth", 10):
        print(df)
    print('')
    return None


def multi_print(df):
    if df.empty:
        print(df)
        print('')
        return None

    indexw = max(len(df.index.name), max_length(df.index)) + 2
    activities = df.columns.get_level_values(0).unique()
    colsw_dict = dict()
    colsw = [indexw]

    for activity in activities:
        activity_colsw = [max(len(col[1]), max_length(df[col])) + 2 for col in df.loc[:, IndexSlice[activity, :]].columns]
        colsw_dict[activity] = activity_colsw
        colsw += activity_colsw

    if sum(colsw) + 2 <= WIDTH:
        print(df)
        print('')
        return None

    max_colwidth = max_threshold(colsw, WIDTH)
    if max_colwidth >= 15:
        with option_context("display.max_colwidth", max_colwidth):
            print(df)
        print('')
        return None

    for idx, activity in enumerate(activities):
        activity_df = df.loc[:, IndexSlice[activity, :]].copy()
        if len(activities) > 1 and idx < (len(activities) - 1):
            activity_df[('\\', ' ')] = ''
        activity_colsw = [indexw] + colsw_dict[activity] + [3]
        if sum(activity_colsw) + 2 <= WIDTH:
            print(activity_df)
            print('')
            continue
        else:
            max_colwidth = max_threshold(activity_colsw, WIDTH)
            with option_context("display.max_colwidth", max_colwidth):
                print(activity_df)
            print('')
            continue

    return None

def subtitle_print(title):
    print(f"\n{'—' * WIDTH}\n\n * {title}\n")

def title_print(title):
    fill = (WIDTH - (len(title) + 2))
    l_delim = fill // 2
    r_delim = l_delim if fill % 2 == 0 else l_delim + 1
    print(f"\n{'—' * WIDTH}\n\n{' ' * l_delim} {title} {' ' * r_delim}")