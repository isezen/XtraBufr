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
import sys as _sys
import time as _time
import argparse as _argparse
from argparse import RawTextHelpFormatter
import eccodes as _ec

__author__ = 'ismail sezen'
__contact__ = 'sezenismail@gmail.com'
__copyright__ = "Copyright (C) 2018, Ismail SEZEN"
__credits__ = []
__license__ = "AGPL 3.0"
__version__ = "0.0.1"
__status__ = "Production"


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


def blist(file_name, args={}):
    """ List content of a bufr file

    :param file_name: BUFR file name
    :param args: Arguments to filter results
    :return: List of print results
    """

    header_keys = ['dataCategory', 'dataSubCategory', 'bufrHeaderCentre',
                   'typicalDate', 'typicalTime', 'typicalYear', 'typicalMonth',
                   'typicalDay', 'typicalHour', 'typicalMinute',
                   'compressedData']
    subset_keys = ['blockNumber', 'stationNumber']
    header_args = {k: v for k, v in args.items() if k in header_keys}
    subset_args = {k: v for k, v in args.items() if k in subset_keys}

    ret = []
    cnt = 0
    with open(file_name, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)
            if bufr is None:
                break
            if not check_args(bufr, header_args):
                continue
            dataCategory = _ec.codes_get(bufr, 'dataCategory')
            dataSubCategory = _ec.codes_get(bufr, 'dataSubCategory')
            idataSubCategory = _ec.codes_get(bufr,
                                             'internationalDataSubCategory')
            bufrHeaderCentre = _ec.codes_get(bufr, 'bufrHeaderCentre')
            bufrHeaderSubCentre = _ec.codes_get(bufr, 'bufrHeaderSubCentre')

            # if dataCategory == 0:
            comp = _ec.codes_get(bufr, 'compressedData')
            nos = _ec.codes_get(bufr, 'numberOfSubsets')
            year = _ec.codes_get(bufr, 'typicalYear')
            month = _ec.codes_get(bufr, 'typicalMonth')
            day = _ec.codes_get(bufr, 'typicalDay')
            hour = _ec.codes_get(bufr, 'typicalHour')
            minute = _ec.codes_get(bufr, 'typicalMinute')
            second = _ec.codes_get(bufr, 'typicalSecond')
            str_date = '{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}'
            str_date = str_date.format(year, month, day, hour, minute,
                                       second)
            len_subset = len(str(nos)) + 1
            str_subset = '{{: >{}}}'.format(len_subset)
            msg1 = 'MSG #{} ({} Subsets) [{}]'.format(cnt, nos, str_date)
            if comp == 1:
                msg1 += ' [Compressed Data]'
            msg1 += '\n  [HC:{} HsC:{}, DCAT:{}, DsCAT:{}, iDsCAT:{}]'.format(
                bufrHeaderCentre,
                bufrHeaderSubCentre,
                dataCategory,
                dataSubCategory,
                idataSubCategory)
            try:
                _ec.codes_set(bufr, 'unpack', 1)
            except _ec.DecodingError as e:
                print('ERROR: MSG #{} {}'.format(cnt, e.msg),
                      file=_sys.stderr)
                continue
            for i in range(1, nos + 1):
                try:
                    bufr2 = extract_subset(bufr, i)
                except _ec.CodesInternalError as e:
                    msg = 'ERROR: MSG #{} - Subset #{} "{}"'
                    print(msg.format(cnt, i, e.msg), file=_sys.stderr)
                    continue
                if check_args(bufr2, subset_args):
                    bn = _ec.codes_get(bufr2, 'blockNumber')
                    sta_num = _ec.codes_get(bufr2, 'stationNumber')
                    sta_name = _ec.codes_get(bufr2, 'stationOrSiteName')
                    sta_name = '"{}"'.format(sta_name.title())
                    lat = _ec.codes_get(bufr2, 'latitude')
                    lon = _ec.codes_get(bufr2, 'longitude')
                    lat = '' if lat == _ec.CODES_MISSING_DOUBLE \
                        else '{:6.2f}'.format(lat)
                    lon = '' if lon == _ec.CODES_MISSING_DOUBLE \
                        else '{:6.2f}'.format(lon)
                    statid = bn * 1000 + sta_num
                    msg = '    {} - {} {:>22} {} {}'
                    msg = msg.format(str_subset.format('#' + str(i)),
                                     statid, sta_name.title(),
                                     lat, lon)
                    ret.append([msg1, msg])
                _ec.codes_release(bufr2)
            _ec.codes_release(bufr)
    return(ret)


def print_results(filename, ret):
    if len(ret) > 0:
        prev = 0
        print('File : {}'.format(filename))
        for i in ret:
            if i[0] != prev:
                print(i[0])
                prev = i[0]
            print(i[1])


def main():

    description = 'Print messages and details of a bufr file.\n' + \
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
                                 epilog=epilog.format(__file__),
                                 formatter_class=RawTextHelpFormatter)
    for a in args:
        p.add_argument(a[0], a[1], type=a[2], nargs='+', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('bufr_files', type=str, nargs='*',
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
            ret = blist(fn, args)
            elapsed_time = _time.clock() - t
            print_results(fn, ret)
            print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except _ec.CodesInternalError as err:
        _sys.stderr.write(err.msg + '\n')

    return(1)


if __name__ == "__main__":
    _sys.exit(main())
