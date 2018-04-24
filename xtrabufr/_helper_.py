"""
xtrabufr._helper_
~~~~~~~~~~~~~~~~~~
Helper functions
"""
from __future__ import print_function
import re as _re
from pprint import pformat as _pformat
from numpy import array as _arr


def print_list(x, key=''):
    if isinstance(x, list):
        y = _pformat(_arr(x))
        y = y.replace(', dtype=object)', '')
        # y = y.replace('dtype=object,', '')
        y = y.replace('array(', '')
        y = y.replace(')', '')
        if '\n' in y:
            y = y.replace('[', '[\n       ', 1)
        else:
            y = _re.sub(' +', ' ', y)
        y = y.replace('[', '{')
        y = y.replace(']', '}')
        print('  {}[{}] = '.format(key, len(x)), end='')
        print(y)


def print_var(key_name, x, tab=0, ignore_missing=False):
    """Print a variable read from BUFR file

    Here x may be a string, integer, float etc... or a list of values.
    Functions chooses appropriate format according to type of x.

    :param key_name: KName of key to print
    :param x: Value to be printed
    :param tab: Number of leading spaces
    :param ignore_missing: If True, missing key/values are not printed.
    :returns: None
    """
    tab = ' ' * tab
    if not isinstance(x, list):
        if ignore_missing:
            if x is None:
                return(None)
        print('{}{} = {}'.format(tab, key_name, x))
    else:
        if len(x) == 0:
            return(None)
        if ignore_missing:
            if all([i is None for i in x]):
                return(None)
        print_list(x, key_name)


def print_msg(msg, filename='', ignore_missing=False):
    """Print message

    :param msg: Message to be printed. Must be a proper dict object
    :param filename: Name of the file to be printed
    :param ignore_missing: If True, missing values are not printed
    :returns: None
    """
    for i, m in msg.items():
        if m is None:
            continue
        h = m['header']
        subset = m['subset']
        nos = h['numberOfSubsets']
        if len(subset) == 0:
            continue
        print('MSG #{} ({} Subsets)'.format(i, nos))
        for k in h.keys():
            print_var(k, h[k], 2, ignore_missing)
        print('')
        for i, s in subset.items():
            print('  Subset #{}'.format(i))
            for k in s.keys():
                print_var(k, s[k], 4, ignore_missing)
