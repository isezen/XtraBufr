#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" bufr_definitions.py : List content of a BUFR file

       USAGE: ./bufr_definitions.py --help
      AUTHOR: Ismail SEZEN (isezen)
       EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
ORGANIZATION: Turkish State Meteorological Service
     CREATED: 18/04/2018 20:56:00 PM

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
from platform import system as _system
from subprocess import check_output as _chekout
from sys import stderr as _stderr
from sys import exit as _exit
import time as _time
import argparse as _argparse
from argparse import RawTextHelpFormatter as _rtformatter
# from collections import OrderedDict as _od
# from numpy import ndarray as _nd
# from numpy import array as _arr
# from pprint import pformat
import re as _re
# import csv as _csv
import ctypes as _ct
import eccodes as _ec


_def_catch_ = {}


class _entry(_ct.Structure):
    _fields_ = [("path", _ct.c_char_p),
                ("content", _ct.POINTER(_ct.c_ubyte)),
                ("length", _ct.c_size_t)]


def eprint(*args, **kwargs):
    print('ERROR: ', *args, file=_stderr, **kwargs)


def _get_lib_path():
    system_name = _system()
    if system_name == 'Linux':
        return('/usr/local/lib/libeccodes_memfs.so')
    elif system_name == 'Darwin':
        return('/opt/local/lib/libeccodes_memfs.dynlib')


def parse_codes_info():
    try:
        codes_info_out = _chekout('codes_info')
    except Exception as e:
        eprint('install ecCodes')
        _exit(1)

    version = codes_info_out.split('\n')[1].split(' ')[2]
    regex = r'(\/.*?\/)((?:[^\/]|\\\/)+?)(?:(?<!\\)\s|$)'
    paths = _re.findall(regex, codes_info_out)
    def_path = ''.join(paths[0])
    sample_path = ''.join(paths[1])
    return({'version': version, 'def': def_path,
            'sample': sample_path})


def codes_get_definitions_path():
    try:
        eccodes_def_path = _os.environ['ECCODES_DEFINITION_PATH']
    except KeyError as e:
        eccodes_def_path = parse_codes_info()['def']
    return(eccodes_def_path)

# ----------------------


def _get_entry(path):
    if 'MEMFS' in codes_get_definitions_path():
        lib = _ct.cdll.LoadLibrary(_get_lib_path())
        entries = _ct.POINTER(_entry)
        table = entries.in_dll(lib, "entries")
        content = ''
        size = _ct.sizeof(table._type_)
        a = _ct.addressof(table)
        while True:
            t = (table._type_).from_address(a)
            if t.path is None:
                break
            if t.path == path:
                content = _ct.string_at(t.content)
            a += size
    else:
        with open(path, 'r') as f:
            content = f.read()
    return(content)


def get_element_table(masterTableVersionNumber='latest', by_code=True):
    def_path = codes_get_definitions_path()
    path = def_path + '/bufr/tables/0/wmo/{}/element.table'
    path = path.format(masterTableVersionNumber)
    if path in _def_catch_.keys():
        return(_def_catch_[path])
    content = _get_entry(path)
    table = {}
    for line in content.split('\n'):
        if line != '':
            s = line.split('|')
            if s[0] != '#code':
                if by_code:
                    table[int(s[0])] = s[1:]
                else:
                    table[int(s[1])] = s[0] + s[2:]
    _def_catch_[path] = table
    return(table)


def get_sequence_def(masterTableVersionNumber='latest'):
    def_path = codes_get_definitions_path()
    path = def_path + '/bufr/tables/0/wmo/{}/sequence.def'
    path = path.format(masterTableVersionNumber)
    if path in _def_catch_.keys():
        return(_def_catch_[path])
    content = _get_entry(path)
    ls = _re.split(r" = \[| \]\n", content)
    d = {}
    for i in range(0, len(ls), 2):
        if ls[i] != '':
            k = int(ls[i].replace(' ', '').replace('"', ''))
            v = [int(j) for j in ls[i + 1].replace(' ', '').split(',')]
            d[k] = v
    _def_catch_[path] = d
    return(d)


def get_code_table(code, masterTableVersionNumber='latest'):
    def_path = codes_get_definitions_path()
    path = def_path + '/bufr/tables/0/wmo/{}/codetables/{}.table'
    path = path.format(masterTableVersionNumber, int(code))
    if path in _def_catch_.keys():
        return(_def_catch_[path])
    content = _get_entry(path)
    d = {}
    for line in content.split('\n'):
        if line != '':
            s = line.split(' ')
            v = ' '.join(s[2:])
            if 'MISSING VALUE' == v:
                v = 'MISSING'
            d[int(s[0])] = v
    _def_catch_[path] = d
    return(d)


def def_is_in(code, search_in, masterTableVersionNumber='latest'):
    seq = get_sequence_def(masterTableVersionNumber)

    def def_is_in_internal(masterTableVersionNumber, code, search_in):
        if not isinstance(search_in, list):
            search_in = [search_in]
        if code in search_in:
            return(True)
        for k in search_in:
            if k in seq.keys():
                for j in seq[k]:
                    if def_is_in_internal(masterTableVersionNumber, code, j):
                        return(True)
        return(False)
    return(def_is_in_internal(masterTableVersionNumber, code, search_in))


def def_lookup(code, masterTableVersionNumber='latest', show_description=True):
    seq = get_sequence_def(masterTableVersionNumber)
    et = get_element_table(masterTableVersionNumber)

    def def_lookup_internal(masterTableVersionNumber, code, tab):
        if not isinstance(code, list):
            code = [code]
        for i in code:
            str_tab = ' ' * tab
            s = '{:06d}'.format(i)
            # if tab == 0:
            s = '[{}]'.format(s)
            print(str_tab + s, end='')
            if i in et.keys():
                a1 = et[i][0]
                a2 = et[i][2] if show_description else et[i][3]
                print(' ' + a1 + ' (' + a2 + ')', end='')
            print('')
            if i in seq.keys():
                for j in seq[i]:
                    def_lookup_internal(masterTableVersionNumber, j, tab + 4)
    def_lookup_internal(masterTableVersionNumber, code, 0)


def get_value_from_code_table(value, code, masterTableVersionNumber='latest'):
    if value is None:
        return(None)
    if isinstance(value, list):
        return([get_value_from_code_table(v, code, masterTableVersionNumber)
                for v in value])
    ct = get_code_table(code, masterTableVersionNumber)
    if value not in ct.keys():
        return(value)
    return(ct[value])


def _main_():
    file_py = _os.path.basename(__file__)
    description = 'BUFR Definition operations\n'
    epilog = 'Example of use:\n' + \
             ' {0} 307080\n' + \
             ' {0} -m 14 307096\n' + \
             ' {0} -m 22 20010 1003'
    args = [['-m', '--mastertable', str, 'N', 'Master Table Version Number']]

    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=_rtformatter)
    for a in args:
        p.add_argument(a[0], a[1], type=a[2], nargs='?', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('-d', '--description', help="Show description",
                   action="store_true")
    p.add_argument('lookup', type=int, nargs='*', help='Descriptor value(s)')
    args = p.parse_args()
    if args.mastertable is None:
        args.mastertable = 'latest'
    try:
        t = _time.clock()
        def_lookup(args.lookup, args.mastertable, args.description)
        elapsed_time = _time.clock() - t
        print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except Exception as e:
        eprint(e)


if __name__ == "__main__":
    _exit(_main_())

