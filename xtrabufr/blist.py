#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" blist.py : List content of a BUFR file

       USAGE: ./blist.py --help
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
#         FILE: blist.py
#        USAGE: ./blist.py --help
#  DESCRIPTION: List content of a bufr file
#       AUTHOR: Ismail SEZEN (isezen)
#        EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
# ORGANIZATION: Turkish State Meteorological Service
#      CREATED: 03/27/2018 06:00:00 AM
# =============================================================================

from __future__ import print_function
import os as _os
from sys import stderr as _stderr
from sys import exit as _exit
import time as _time
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


def check_args(bufr, args):
    if len(args) == 0:
        return(True)
    ret = []
    for k, lv in args.items():
        try:
            val = _ec.codes_get(bufr, k)
        except _ec.CodesInternalError as e:
            continue
        ret.append(val in lv)
    if all(ret):
        return(True)
    return(False)


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


def read_msg(file_name, args={}):
    """ Read content of a bufr file by args parameter

    :param file_name: BUFR file name
    :param args: Arguments to filter results
    :return: List of read values
    """

    header_keys = ['dataCategory', 'dataSubCategory',
                   'internationalDataSubCategory', 'bufrHeaderCentre',
                   'bufrHeaderSubCentre', 'compressedData', 'numberOfSubsets',
                   'typicalDate', 'typicalTime', 'typicalYear', 'typicalMonth',
                   'typicalDay', 'typicalHour', 'typicalMinute',
                   'typicalSecond', 'unexpandedDescriptors']
    subset_keys = ['blockNumber', 'stationNumber', 'stationOrSiteName',
                   'latitude', 'longitude']
    header_args = {k: v for k, v in args.items() if k in header_keys}
    subset_args = {k: v for k, v in args.items() if k in subset_keys}

    msg = {}
    cnt = 0
    with open(file_name, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)
            if bufr is None:
                break
            if not check_args(bufr, header_args):
                continue
            h = {k: get_val(bufr, k) for k in header_keys}

            try:
                _ec.codes_set(bufr, 'unpack', 1)
            except _ec.DecodingError as e:
                eprint('ERROR: MSG #{} {}'.format(cnt, e.msg))
                continue

            subset = {}
            for i in range(1, h['numberOfSubsets'] + 1):
                try:
                    bufr2 = extract_subset(bufr, i)
                except _ec.CodesInternalError as e:
                    err = 'ERROR: MSG #{} - Subset #{} "{}"'
                    eprint(err.format(cnt, i, e.msg))
                    continue
                if check_args(bufr2, subset_args):
                    s = {k: get_val(bufr2, k) for k in subset_keys}
                    s['stationOrSiteName'] = s['stationOrSiteName'].title()
                    if s['latitude'] == _ec.CODES_MISSING_DOUBLE:
                        s['latitude'] = None
                    if s['longitude'] == _ec.CODES_MISSING_DOUBLE:
                        s['longitude'] = None
                    subset[i] = s
                _ec.codes_release(bufr2)
            _ec.codes_release(bufr)
            msg[cnt] = {'header': h, 'subset': subset}
    return(msg)


def print_msg(msg, filename=''):
    for i, m in msg.items():
        h = m['header']
        subset = m['subset']
        if len(subset) == 0:
            continue
        nos = h['numberOfSubsets']
        str_date = '{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}'
        str_date = str_date.format(h['typicalYear'], h['typicalMonth'],
                                   h['typicalDay'], h['typicalHour'],
                                   h['typicalMinute'], h['typicalSecond'])
        msg1 = 'MSG #{} ({} Subsets) [{}]'.format(i, nos, str_date)
        if h['compressedData'] == 1:
                msg1 += ' [Compressed Data]'
        msg1 += '\n  [HC:{} HsC:{}, DCAT:{}, DsCAT:{}, iDsCAT:{}]'.format(
            h['bufrHeaderCentre'],
            h['bufrHeaderSubCentre'],
            h['dataCategory'],
            h['dataSubCategory'],
            h['internationalDataSubCategory'])
        msg1 += '\nunexpDesc: {}'.format(h['unexpandedDescriptors'])
        str_subset = '{{: >{}}}'.format(len(str(nos)) + 1)
        print(msg1)
        for k, s in subset.items():
            sta_name = '"{}"'.format(s['stationOrSiteName'])
            statid = s['blockNumber'] * 1000 + s['stationNumber']
            lat = '' if s['latitude'] is None \
                else '{:6.2f}'.format(s['latitude'])
            lon = '' if s['longitude'] is None \
                else '{:6.2f}'.format(s['longitude'])
            msg = '    {} - {} {:>22} {} {}'
            msg = msg.format(str_subset.format('#' + str(k)),
                             statid, sta_name, lat, lon)
            print(msg)


def main():
    file_py = _os.path.basename(__file__)
    description = 'List messages and details of a bufr file.\n' + \
                  'Optional arguments can be used to filter output.\n\n' + \
                  ' N       : An integer Numeric value\n' + \
                  ' YYYMMDD : Year, Month and day (adjacent)\n' + \
                  ' HHMMSS  : Hour, minute and second (adjacent)'
    epilog = 'Example of use:\n' + \
             ' {0} input.bufr\n' + \
             ' {0} input1.bufr input2.bufr input3.bufr\n' + \
             ' {0} input*.bufr\n' + \
             ' {0} input.bufr -c 91 -dc 0 -b 17\n' + \
             ' {0} input*.bufr -c 91 -dc 0 -b 17 -d 20180324\n'
    args = [['-dc', '--dataCategory', int, 'N', 'Data Category'],
            ['-id', '--internationalDataSubCategory', int, 'N',
             'International Data Sub-Category'],
            ['-ds', '--dataSubCategory', int, 'N', 'Data Sub-Category'],
            ['-cd', '--compressedData', int, 'N', 'Compressed Data'],
            ['-hc', '--bufrHeaderCentre', int, 'N', 'Header Centre'],
            ['-td', '--typicalDate', str, 'YYYYMMDD', 'Typical Date'],
            ['-y', '--typicalYear', int, 'N', 'Typical Year'],
            ['-m', '--typicalMonth', int, 'N', 'Typical Month'],
            ['-d', '--typicalDay', int, 'N', 'Typical Day'],
            ['-tt', '--typicalTime', str, 'HHMMSS', 'Typical Time'],
            ['-th', '--typicalHour', int, 'N', 'Typical Hour'],
            ['-tm', '--typicalMinute', int, 'N', 'Typical Minute'],
            ['-bn', '--blockNumber', int, 'N', 'Block Number'],
            ['-sn', '--stationNumber', int, 'N', 'Station Number']]

    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=RawTextHelpFormatter)
    for a in args:
        p.add_argument(a[0], a[1], type=a[2], nargs='+', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('bufr_files', type=str, nargs='+',
                   help='BUFR files to process\n' +
                        '(at least a single file required)')

    args = p.parse_args()
    args = {a: v for a, v in sorted(vars(args).items())
            if v is not None}

    file_names = args['bufr_files']
    args.pop('bufr_files', None)

    try:
        for fn in file_names:
            t = _time.clock()
            messages = read_msg(fn, args)
            elapsed_time = _time.clock() - t
            print_msg(messages, fn)
            print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except _ec.CodesInternalError as err:
        eprint(err.msg)

    return(1)


if __name__ == "__main__":
    _exit(main())
