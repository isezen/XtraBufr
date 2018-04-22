#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" check_synop.py : List content of a BUFR file

       USAGE: ./check_synop.py --help
      AUTHOR: Ismail SEZEN (isezen)
       EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
ORGANIZATION: Turkish State Meteorological Service
     CREATED: 16/04/2018 22:33:00 PM

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
#         FILE: check_synop.py
#        USAGE: ./check_synop.py --help
#  DESCRIPTION: Cehck synop bufr messages
#       AUTHOR: Ismail SEZEN (isezen)
#        EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
# ORGANIZATION: Turkish State Meteorological Service
#      CREATED: 16/04/2018 22:33:00 PM
# =============================================================================

from __future__ import print_function
import os as _os
from sys import stderr as _stderr
from sys import exit as _exit
import time as _time
import re as _re
import argparse as _argparse
from argparse import RawTextHelpFormatter
from numpy import ndarray as _nd
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


def read_msg(file_name):
    """ Read content of a bufr file

    :param file_name: BUFR file name
    :param args: Arguments to filter results
    :return: List of read values
    """

    header_keys = ['numberOfSubsets', 'unexpandedDescriptors']
    subset_keys = ['blockNumber', 'stationNumber', 'stationOrSiteName',
                   'latitude', 'longitude', 'presentWeather', 'pastWeather1',
                   'pastWeather2']

    unique_keys = []
    cnt = 0
    with open(file_name, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)
            if bufr is None:
                break

            try:
                h = {k: get_val(bufr, k) for k in header_keys}
            except Exception as e:
                eprint('GETVAL ERROR: MSG #{} {}'.format(cnt, e.msg))
                continue

            try:
                _ec.codes_set(bufr, 'unpack', 1)
            except Exception as e:
                eprint('UNPACK ERROR: MSG #{} {}'.format(cnt, e.msg))
                continue

            keys = {_re.sub('#.*?#', '', k) for k in get_keys(bufr)}
            res = [k in keys for k in subset_keys]
            if all(res):
                if h['unexpandedDescriptors'] not in unique_keys:
                    unique_keys.append(h['unexpandedDescriptors'])
            _ec.codes_release(bufr)
    return(unique_keys)


def main():
    file_py = _os.path.basename(__file__)
    description = 'List messages and details of a bufr file.\n' + \
                  'Optional arguments can be used to filter output.\n'
    epilog = 'Example of use:\n' + \
             ' {0} input.bufr\n' + \
             ' {0} input1.bufr input2.bufr input3.bufr\n' + \
             ' {0} input*.bufr\n'

    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=RawTextHelpFormatter)

    p.add_argument('bufr_files', type=str, nargs='+',
                   help='BUFR files to process\n' +
                        '(at least a single file required)')

    args = p.parse_args()

    try:
        unique_keys = []
        t = _time.clock()
        for fn in args.bufr_files:
            uk = read_msg(fn)
            if len(uk) > 0:
                print(fn)
            for k in uk:
                if k not in unique_keys:
                    unique_keys.append(k)
        elapsed_time = _time.clock() - t
        print('')
        for k in unique_keys:
            print(k)
        print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except _ec.CodesInternalError as err:
        eprint(err.msg)

    return(1)


if __name__ == "__main__":
    _exit(main())
