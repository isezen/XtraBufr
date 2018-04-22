#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" bsynop2csv.py : List content of a BUFR file

       USAGE: ./bsynop2csv.py --help
      AUTHOR: Ismail SEZEN (isezen)
       EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
ORGANIZATION: Turkish State Meteorological Service
     CREATED: 18/04/2018 00:00:00 AM

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
import csv as _csv
import eccodes as _ec

import bufr_definitions as _bd


_synop_keys_ = ['masterTablesVersionNumber', 'bufrHeaderCentre',
                'updateSequenceNumber', 'blockNumber', 'stationNumber',
                'stationType', 'stationOrSiteName', 'year', 'month', 'day',
                'hour', 'minute', 'latitude', 'longitude',
                'heightOfStationGroundAboveMeanSeaLevel',
                'heightOfBarometerAboveMeanSeaLevel', 'nonCoordinatePressure',
                'pressureReducedToMeanSeaLevel', '3HourPressureChange',
                'characteristicOfPressureTendency', 'pressure',
                'airTemperature', 'dewpointTemperature', 'relativeHumidity',
                'horizontalVisibility',
                'cloudCoverTotal', 'heightOfBaseOfCloud',
                '#1#cloudType', '#2#cloudType', '#3#cloudType',
                'presentWeather', 'pastWeather1',
                'pastWeather1', 'windSpeed', 'windDirection']


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


def get_attr(bufr, key):
    attrs = ['code', 'units', 'scale', 'reference', 'width']
    attributes = _od.fromkeys(attrs)
    for a in attrs:
        k = key + '->' + a
        try:
            attributes[a] = _ec.codes_get(bufr, k)
        except _ec.CodesInternalError as e:
            # eprint('Error with key="%s" : %s' % (k, e.msg))
            continue
    return(attributes)


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
            else:
                v = round(v, 6)
        if isinstance(v, int):
            if v == _ec.CODES_MISSING_LONG:
                v = None
    else:
        v = _ec.codes_get_array(bufr, key)
        if not isinstance(v, list):
            v = v.tolist()
        if isinstance(v[0], int):
            for i, j in enumerate(v):
                if j == _ec.CODES_MISSING_LONG:
                    v[i] = None
        if isinstance(v[0], float):
            for i, j in enumerate(v):
                if j == _ec.CODES_MISSING_DOUBLE:
                    v[i] = None
                else:
                    v[i] = round(v[i], 6)
    return(v)


def read_synop_compressed(msg):
    """Read compressed message from BUFR file
    :param msg: Handle to BUFR file
    :returns: (dict) Read message
    """
    ret = _od([(k, get_val(msg, k)) for k in _synop_keys_])
    for k in ret.keys():
        if not isinstance(ret[k], list):
            ret[k] = [ret[k]]
    return(ret)


def read_synop_uncompressed(msg):
    """Read uncompressed message from BUFR file
    :param bufr: Handle to BUFR file
    :returns: (dict) Read message
    """

    ret = _od([(k, []) for k in _synop_keys_])
    for i in range(1, get_val(msg, 'numberOfSubsets') + 1):
        try:
            subset = extract_subset(msg, i)
        except _ec.CodesInternalError as e:
            eprint('MSG #{} - Subset #{} "{}"'.format(1, i, e.msg))
            continue
        for k in _synop_keys_:
            ret[k].append(get_val(subset, k))
        _ec.codes_release(subset)
    return(ret)


def read_synop(file_name, decode_code_table=True):

    synop_identifier = [[307086, 1023, 4025, 2177, 101000, 31001, 20003,
                         103000, 31001, 5021, 20001, 5021, 101000, 31000,
                         302056, 103000, 31000, 33041, 20058, 22061, 101000,
                         31000, 302022, 101000, 31001, 302023, 103000, 31001,
                         20054, 20012, 20090, 4025, 13012, 4025, 11042,
                         104000, 31001, 8021, 4025, 11042, 8021, 115000,
                         31001, 8021, 4015, 8021, 4025, 11001, 11002, 8021,
                         4015, 8021, 4025, 11001, 11002, 8021, 4025, 4015,
                         103000, 31001, 4025, 4025, 20003, 111000, 31001,
                         4025, 4025, 5021, 5021, 20054, 20024, 20025, 20026,
                         20027, 20063, 8021],
                        [301090, 302031, 302035, 302036, 302047, 8002, 302048,
                         302037, 302043, 302044, 101002, 302045, 302046],
                        [307096, 22061, 20058, 4024, 13012, 4024],
                        [307079, 4025, 11042],
                        307080, 307086, 307096]

    ret = _od([(k, []) for k in _synop_keys_])
    cnt = 0
    with open(file_name, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)

            if bufr is None:
                break

            unexpDesc = get_val(bufr, 'unexpandedDescriptors')

            if unexpDesc in synop_identifier:
                try:
                    _ec.codes_set(bufr, 'unpack', 1)
                except _ec.DecodingError as e:
                    eprint('MSG #{} {}'.format(cnt, e.msg))
                    continue
                attrib = _od([(k, get_attr(bufr, k)) for k in _synop_keys_])
                compressed = get_val(bufr, 'compressedData') == 1
                print(cnt, compressed)
                read = read_synop_compressed if compressed \
                    else read_synop_uncompressed
                r = read(bufr)
                if decode_code_table:
                    mtvn = get_val(bufr, 'masterTablesVersionNumber')
                    for k in r.keys():
                        if attrib[k]['units'] == 'CODE TABLE':
                            r[k] = _bd.get_value_from_code_table(
                                r[k], attrib[k]['code'], mtvn)

                for k in _synop_keys_:
                    ret[k].extend(r[k])
            _ec.codes_release(bufr)
    return(attrib, ret)


def main():
    file_name = 'mss_0_0_20180417_0.bufr4'
    file_name = '../sample_data/mss_0_2_20180328_0.bufr4'
    attrib, ret = read_synop(file_name)
    # ret['latitude'] = _round_list(ret['latitude'])
    # ret['longitude'] = _round_list(ret['longitude'])
    # ret['airTemperature'] = _round_list(ret['airTemperature'])
    # ret['airTemperature'] = _round_list(ret['airTemperature'])
    units = [k['units'] for _, k in attrib.items()]
    # codes = [k['code'] for _, k in attrib.items()]
    li = [v for _, v in ret.items()]
    li = map(list, zip(*li))
    # li.insert(0, ret.keys())
    # print(';'.join(ret.keys()))
    with open("output.csv", "wb") as f:
        writer = _csv.writer(f, delimiter=';')
        # writer = _csv.DictWriter(f, fieldnames=ret.keys(), delimiter=';')
        writer.writerow(ret.keys())
        writer.writerow(units)
        writer.writerows(li)


if __name__ == "__main__":
    _exit(main())


_decode_code_table(22, 'presentWeather', 20003, 'CODE TABLE', 509)
