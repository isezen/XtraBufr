#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" blist_keys.py : List keys in messages in a BUFR file

       USAGE: ./blist_keys.py --help
      AUTHOR: Ismail SEZEN (isezen)
       EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
ORGANIZATION: Turkish State Meteorological Service
     CREATED: 03/27/2018 06:00:00 AM

Copyright (C) 2018  Ismail SEZEN

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>."""

# =============================================================================
#         FILE: blist_keys.py
#        USAGE: ./blist_keys.py --help
#  DESCRIPTION: List content of a bufr file
#       AUTHOR: Ismail SEZEN (isezen)
#        EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
# ORGANIZATION: Turkish State Meteorological Service
#      CREATED: 03/29/2018 10:00:00 PM
# =============================================================================

from __future__ import print_function
import os as _os
from sys import stderr as _stderr
from sys import exit as _exit
import time as _time
import re as _re
import argparse as _argparse
from argparse import RawTextHelpFormatter
from numpy import array as _arr
from numpy import ndarray as _nd
from pprint import pformat
import eccodes as _ec

__author__ = 'ismail sezen'
__contact__ = 'sezenismail@gmail.com'
__copyright__ = "Copyright (C) 2018, Ismail SEZEN"
__credits__ = []
__license__ = "AGPL 3.0"
__version__ = "0.0.1"
__status__ = "Production"


def eprint(*args, **kwargs):
    print(*args, file=_stderr, **kwargs)


def get_keys(bufr):
    """Get keys from a BUFR handle

    :param bufr: Handle to BUFR file
    :returns: List of keys
    """
    keys = []
    iterid = _ec.codes_bufr_keys_iterator_new(bufr)
    while _ec.codes_bufr_keys_iterator_next(iterid):
        keyname = _ec.codes_bufr_keys_iterator_get_name(iterid)
        keys.append(keyname)
    _ec.codes_bufr_keys_iterator_delete(iterid)
    return(keys)


def extract_subset(bufr, subset_number):
    """Extract a subset from a BUFR handle

    :param bufr: Handle to BUFR file
    :param subset_number: Number og subset
    :returns: Handle to BUFR message contains subset
    """
    _ec.codes_set(bufr, 'extractSubset', subset_number)
    _ec.codes_set(bufr, 'doExtractSubsets', 1)
    bufr2 = _ec.codes_clone(bufr)
    _ec.codes_set(bufr2, 'unpack', 1)
    return(bufr2)


def blist_keys(file_name):
    """ List keys in a bufr file

    :param file_name: BUFR file name
    :param args: Arguments to filter results
    :return: List of print results
    """
    ret = {}
    cnt = 0
    with open(file_name, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)
            if bufr is None:
                break
            nos = _ec.codes_get(bufr, 'numberOfSubsets')
            try:
                _ec.codes_set(bufr, 'unpack', 1)
            except _ec.DecodingError as e:
                eprint('ERROR: MSG #{} {}'.format(cnt, e.msg))
                continue
            ret[cnt] = {}
            for i in range(1, nos + 1):
                try:
                    bufr2 = extract_subset(bufr, i)
                except _ec.CodesInternalError as e:
                    msg = 'ERROR: MSG #{} - Subset #{} "{}"'
                    eprint(msg.format(cnt, i, e.msg))
                    del ret[cnt]
                    break
                keys = get_keys(bufr2)
                keys = [_re.sub('#.*?#', '', k) for k in keys]
                ret[cnt][i] = keys
                _ec.codes_release(bufr2)
            _ec.codes_release(bufr)
    #
    return(ret)


def print_keys(x, var_name):
    if len(x) == 0:
        return(None)
    print(var_name + '[{}]={{'.format(len(x)))
    s = []
    for i, j in enumerate(sorted(list(x))):
        s.append(j)
        if len(', '.join(s)) > 60:
            print('    ' + ', '.join(s), end=',\n')
            s = []
        elif len(x) == i + 1:
            print('    ' + ', '.join(s))
            s = []
    print('}')


def print_results(filename, ret):
    if len(ret) == 0:
        return(None)
    cks = [set(keys) for msg_id, v in ret.items() for _, keys in v.items()]
    u = set.intersection(*cks)
    others = set(k for i in cks for k in i - u)

    print('File : {}'.format(filename))
    print_keys(u, 'common_keys')
    print_keys(others, 'uncommon_keys')


def print_keys_distinct(filename, x):
    if len(x) == 0:
        return(None)
    uniq = {}
    for id, msg in x.items():
        for k, v in msg.items():
            h = hash(str(set(v)))
            if h not in uniq.keys():
                uniq[h] = set(v)
    uniq = [v for k, v in uniq.items()]
    print('# Number of Unique Subsets = {}'.format(len(uniq)))
    isect = set.intersection(*uniq)
    print('File : {}'.format(filename))
    print_keys(isect, 'common_keys')
    for i, v in enumerate(uniq):
        print_keys(v - isect, 'uniq_subset{}'.format(i + 1))


def get_val(bufr, key):
    """Read value of a key in a BUFR message

    :param bufr: Handle to BUFR file
    :param key: Key value to read
    :returns: Read value (if value is missing returns None)
    """
    size = _ec.codes_get_size(bufr, key)
    v = None
    if size == 1:
        v = _ec.codes_get(bufr, key)
        if isinstance(v, float):
            if v == _ec.CODES_MISSING_DOUBLE:
                v = None
        if isinstance(v, int):
            if v == _ec.CODES_MISSING_LONG:
                v = None
    else:
        v = _ec.codes_get_array(bufr, key)
        if isinstance(v, _nd):
            v = v.tolist()
            if isinstance(v[0], int):
                for i, j in enumerate(v):
                    if j == _ec.CODES_MISSING_LONG:
                        v[i] = None
            if isinstance(v[0], float):
                for i, j in enumerate(v):
                    if j == _ec.CODES_MISSING_DOUBLE:
                        v[i] = None
        return(v)
    return(v)


def blist_key_vals(file_name):
    """ List headaer key vlaues in a bufr file

    :param file_name: BUFR file name
    :param args: Arguments to filter results
    :return: List of print results
    """
    ret = {}
    cnt = 0
    with open(file_name, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)
            if bufr is None:
                break
            keys = get_keys(bufr)
            for k in keys:
                if k not in ret.keys():
                    ret[k] = list()
                v = get_val(bufr, k)
                if v not in ret[k]:
                    ret[k].append(v)
            _ec.codes_release(bufr)
    #
    return(ret)


def print_list(x, key=''):
    if isinstance(x, list):
        y = pformat(_arr([_arr(i) for i in x], dtype=object))
        y = y.replace('\n      dtype=object)', '')
        y = y.replace(', dtype=object)', '')
        y = y.replace('array(', '')
        y = y.replace(')', '')
        if '\n' in y:
            y = y.replace('[', '[\n       ', 1)
        else:
            y = _re.sub(' +', ' ', y)
        y = y.replace('[', '{')
        y = y.replace(']', '}')
        y = y.replace('},', '}')
        print('{}[{}] = '.format(key, len(x)), end='')
        print(y)


def print_key_vals(x):
    """Print message

    :param msg: Message to be printed. Must be a proper dict object
    :param filename: Name of the file to be printed
    :param ignore_missing: If True, missing values are not printed
    :returns: None
    """
    keys = sorted(x.keys())
    for k in keys:
        v = x[k]
        if isinstance(v, list) and len(v) == 1:
            v = v[0]
        if isinstance(v, list):
            print_list(v, k)
        else:
            print(k, '=', v)


def main():
    file_py = _os.path.basename(__file__)
    description = 'List keys for each message in a BUFR file.\n'
    epilog = 'Example of use:\n' + \
             ' {0} -d input.bufr\n' + \
             ' {0} input1.bufr input2.bufr input3.bufr\n' + \
             ' {0} -v input*.bufr\n'
    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=RawTextHelpFormatter)
    p.add_argument('-d', '--distinct', help="distinct print",
                   action="store_true")
    p.add_argument('-v', '--values', help="print unique values of keys",
                   action="store_true")
    p.add_argument('bufr_files', type=str, nargs='+',
                   help='BUFR files to process\n' +
                        '(at least a single file required)')

    args = p.parse_args()
    args = {a: v for a, v in sorted(vars(args).items())
            if v is not None}

    try:
        for fn in args['bufr_files']:
            t = _time.clock()
            ret = blist_key_vals(fn) if args['values'] else blist_keys(fn)
            elapsed_time = _time.clock() - t
            if args['values']:
                print_key_vals(ret)
            else:
                if args['distinct']:
                    print_keys_distinct(fn, ret)
                else:
                    print_results(fn, ret)
            print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except _ec.CodesInternalError as err:
        eprint(err.msg)

    return(1)


if __name__ == "__main__":
    _exit(main())
