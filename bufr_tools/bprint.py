#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" bprint.py : Print content of a BUFR file

       USAGE: ./bprint.py --help
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
#         FILE: bprint.py
#        USAGE: ./bprint.py --help
#  DESCRIPTION: List content of a bufr file
#       AUTHOR: Ismail SEZEN (isezen)
#        EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
# ORGANIZATION: Turkish State Meteorological Service
#      CREATED: 04/04/2018 09:38:00 AM
# =============================================================================

from __future__ import print_function
import os as _os
from sys import stderr as _stderr
from sys import exit as _exit
import time as _time
import argparse as _argparse
from argparse import RawTextHelpFormatter
from collections import OrderedDict as _od
from numpy import ndarray as _nd
from numpy import array as _arr
from pprint import pformat
import re as _re
import eccodes as _ec

__author__ = 'ismail sezen'
__contact__ = 'sezenismail@gmail.com'
__copyright__ = "Copyright (C) 2018, Ismail SEZEN"
__credits__ = []
__license__ = "AGPL 3.0"
__version__ = "0.0.1"
__status__ = "Production"


def eprint(*args, **kwargs):
    print('ERROR: ', *args, file=_stderr, **kwargs)


def get_keys(bufr):
    """Get keys from a BUFR handle

    :param bufr: Handle to BUFR file
    :returns: List of keys
    """
    keys = []
    iterid = _ec.codes_bufr_keys_iterator_new(bufr)
    while _ec.codes_bufr_keys_iterator_next(iterid):
        keys.append(_ec.codes_bufr_keys_iterator_get_name(iterid))
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


def msg_count(bufr_file):
    """Return number of messages in a BUFR file

    :param bufr_file: Name of BUFR file
    :returns: Number of messages in a BUFR file
    """
    ret = None
    with open(bufr_file, 'rb') as f:
        ret = _ec.codes_count_in_file(f)
    return(ret)


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


def read_header_and_unpack(bufr, msg_id=None):
    """Read header from BUFR message and unpack

    :param bufr: Handle to BUFR file
    :param msg_id: Id if message (Required only for Error handling)
    :returns: (OrderedDict) Header key and values
    """
    header_keys = get_keys(bufr)
    h = _od([(k, get_val(bufr, k)) for k in header_keys])

    try:
        _ec.codes_set(bufr, 'unpack', 1)
    except _ec.DecodingError as e:
        eprint('MSG #{} {}'.format(msg_id, e.msg))
        return(None)

    return(h)


def read_compressed_msg(bufr, msg_id=None, subset_id=None):
    """Read compressed message from BUFR file

    :param bufr: Handle to BUFR file
    :param msg_id: Id of message to read
    :param subset_id: Id of subset in message to read
                      If None, all subsets are read
    :returns: (dict) Read message
    """
    h = read_header_and_unpack(bufr, msg_id)
    if h is None:
        return(None)

    keys = [k for k in get_keys(bufr) if k not in h.keys()]
    s = _od([(k, get_val(bufr, k)) for k in keys])

    if subset_id is not None:
        if isinstance(subset_id, list):
            for k, v in s.items():
                if isinstance(v, list):
                    s[k] = [s[k][i - 1] for i in subset_id]

    return({'header': h, 'subset': {'all': s}})


def read_uncompressed_msg(bufr, msg_id=None, subset_id=None):
    """Read uncompressed message from BUFR file

    :param bufr: Handle to BUFR file
    :param msg_id: Id of message to read
    :param subset_id: Id of subset in message to read
                      If None, all subsets are read
    :returns: (dict) Read message
    """
    h = read_header_and_unpack(bufr, msg_id)

    if subset_id is None:
        nos = get_val(bufr, 'numberOfSubsets')
        subset_id = list(range(1, nos + 1))

    subset = {}
    for i in subset_id:

        try:
            bufr2 = extract_subset(bufr, i)
        except _ec.CodesInternalError as e:
            eprint('MSG #{} - Subset #{} "{}"'.format(msg_id, i, e.msg))
            continue

        keys = [k for k in get_keys(bufr2) if k not in h.keys()]
        subset[i] = _od([(k, get_val(bufr2, k)) for k in keys])
        _ec.codes_release(bufr2)
    return({'header': h, 'subset': subset})


def read_msg(file_name, msg_id=None, subset_id=None):
    """ Read a message from BUFR file

    :param file_name: BUFR file name
    :param msg_id: Id of message to read
    :param subset_id: Id of subset in message to read
                      If None, all subsets are read
    :return: (dict) Read message
    """

    if isinstance(msg_id, int):
        msg_id = set([msg_id])

    if msg_id is None:
        msg_id = set(range(1, msg_count(file_name) + 1))

    if isinstance(subset_id, int):
        subset_id = set([subset_id])

    msg = {}
    cnt = 0
    with open(file_name, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)

            if bufr is None:
                break

            if cnt in msg_id:
                msg_id.remove(cnt)
                #
                compressed = get_val(bufr, 'compressedData') == 1
                fun = read_compressed_msg if compressed \
                    else read_uncompressed_msg
                m = fun(bufr, cnt, subset_id)
                msg[cnt] = m

            _ec.codes_release(bufr)
            if len(msg_id) == 0:
                break
    return(msg)


def print_list(x, key=''):
    if isinstance(x, list):
        y = pformat(_arr(x))
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


def main():
    file_py = _os.path.basename(__file__)
    description = 'Print content of a BUFR file.\n' + \
                  'Optional arguments can be used to filter output.\n'
    epilog = 'Example of use:\n' + \
             ' {0} input.bufr\n' + \
             ' {0} input1.bufr input2.bufr input3.bufr\n' + \
             ' {0} input*.bufr\n' + \
             ' {0} input.bufr -c 91 -dc 0 -b 17\n' + \
             ' {0} input*.bufr -c 91 -dc 0 -b 17 -d 20180324\n'
    args = [['-s', '--subset_id', int, 'N', 'Subset Id'],
            ['-m', '--msg_id', int, 'N', 'Message Id (Mandatory)']]

    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=RawTextHelpFormatter)
    for a in args:
        p.add_argument(a[0], a[1], type=a[2], nargs='?', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('-i', '--ignore', help="Ignore Missing/None values",
                   action="store_true")
    p.add_argument('bufr_file', type=str, nargs='?',
                   help='BUFR file to process')

    args = p.parse_args()
    if args.msg_id is None:
        print('msg_id is required')
        return(1)

    try:
        t = _time.clock()
        messages = read_msg(args.bufr_file, args.msg_id, args.subset_id)
        elapsed_time = _time.clock() - t
        print_msg(messages, args.bufr_file, args.ignore)
        print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except _ec.CodesInternalError as err:
        eprint(err.msg)

    return(1)


if __name__ == "__main__":
    _exit(main())
