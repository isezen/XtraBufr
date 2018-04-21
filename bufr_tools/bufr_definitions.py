#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" bufr_definitions.py : BUFR definition operations

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
from collections import OrderedDict as _od
import re as _re
from collections import MutableSequence as _MS
import ctypes as _ct

_def_catch_ = {}


def eprint(*args, **kwargs):
    print('ERROR: ', *args, file=_stderr, **kwargs)


def codes_info(args):
    if not isinstance(args, list):
        args = [args]
    try:
        ret = _od([(p, _chekout(['codes_info', '-' + p]).strip())
                   for p in args])
    except Exception as e:
        eprint('install ecCodes')
        _exit(1)
    if len(ret) == 1:
        return(ret.values()[0])
    return(ret)


def codes_get_definitions_path():
    try:
        eccodes_def_path = _os.environ['ECCODES_DEFINITION_PATH']
    except KeyError as e:
        eccodes_def_path = codes_info('d')
    return(eccodes_def_path)


_codes_definition_path_ = codes_get_definitions_path()


def _get_lib_path():
    system_name = _system()
    if system_name == 'Linux':
        return('/usr/local/lib/libeccodes_memfs.so')
    elif system_name == 'Darwin':
        return('/opt/local/lib/libeccodes_memfs.dynlib')


class _entry(_ct.Structure):
    _fields_ = [("path", _ct.c_char_p),
                ("content", _ct.POINTER(_ct.c_ubyte)),
                ("length", _ct.c_size_t)]


def _get_entry(path):
    if 'MEMFS' in _codes_definition_path_:
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
    path = _codes_definition_path_ + '/bufr/tables/0/wmo/{}/element.table'
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


def get_bufr_template_def():
    path = _codes_definition_path_ + '/bufr/templates/BufrTemplate.def'
    if path in _def_catch_.keys():
        return(_def_catch_[path])
    content = _re.sub(r'[ {\[;"\]}]', '', _get_entry(path))
    ls = [i for i in content.split('\n') if i != '']
    d = {}
    for i in range(0, len(ls)):
        j = ls[i].split('=')
        v = [int(k) for k in j[2].split(',')]
        if len(v) == 1:
            v = v[0]
        d[str(v)] = j[0]
    _def_catch_[path] = d
    return(d)


def get_sequence_def(masterTableVersionNumber='latest'):
    path = _codes_definition_path_ + '/bufr/tables/0/wmo/{}/sequence.def'
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
    def_path = _codes_definition_path_
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


class descriptor(_MS):

    def __init__(self, code, masterTableVersionNumber='latest'):
        self.__dict__ = {'code': code, 'key': '', 'type': '',
                         'name': '', 'unit': '', 'descriptors': [],
                         '_list': []}
        seq = get_sequence_def(masterTableVersionNumber)
        et = get_element_table(masterTableVersionNumber)
        bt = get_bufr_template_def()

        if isinstance(code, list):
            self.descriptors = code
            self._list = [descriptor(j) for j in code]
            self.code = 0

        if code in et.keys():
            self.key = et[code][0]
            self.type = et[code][1]
            self.name = et[code][2]
            self.unit = et[code][3]
        elif code in seq.keys():
            if str(code) in bt.keys():
                self.key = bt[str(code)]
            self.descriptors = seq[code]
            self._list = [descriptor(j) for j in seq[code]]

    def _check(self, v):
        if not isinstance(v, descriptor):
            raise(TypeError(v))

    def __repr__(self):
        s = '\n[{:06d}] {} {}\n' + \
            'Descriptors = {}\n'
        return(s.format(self.code, self.name, self.unit, self.descriptors))

    def __str__(self, show_desc=False, tab=0, leading=''):
        if self.code != 0:
            str_tab = ' ' * tab
            sc = '{:06d}'.format(self.code)
            s = '[{}]'.format(sc, self.key)
            if leading != '':
                s = ('\b' * len(leading)) + leading + s
            if show_desc and self.name != '':
                    s += ' {}'.format(self.name)
            else:
                if self.key != '':
                    s += ' {}'.format(self.key)
            if self.unit != '':
                s += ' ({})'.format(self.unit)
            if sc[0] == '1':
                s += ' ({}x{})'.format(int(sc[1:3]), int(sc[3:6]))
        else:
            str_tab = ''
            s = ''
            tab -= 4
        i = 0
        while i < len(self._list):
            sc2 = '{:06d}'.format(self._list[i].code)
            s += '\n' + self._list[i].__str__(show_desc, tab + 4)
            if sc2[0] == '1':
                sc3 = '{:06d}'.format(self._list[i + 1].code)
                if sc3[0:3] == '031':
                    i += 1
                    s += '\n' + self._list[i].__str__(show_desc, tab + 8,
                                                      '****')
                n = int(sc2[1:3])
                while n > 0:
                    i += 1
                    n -= 1
                    s += '\n' + self._list[i].__str__(show_desc, tab + 8,
                                                      '....')
            i += 1
        return(str_tab + s)

    def __len__(self):
        """List length"""
        return(len(self._list))

    def __getitem__(self, i):
        """Get a list item"""
        return(self._list[i])

    def __delitem__(self, i):
        """Delete an item"""
        del self._list[i]

    def __setitem__(self, i, val):
        self._check(val)
        self._list[i] = val

    def insert(self, i, val):
        self._check(val)
        self._list.insert(i, val)

    def insert_code(self, i, code, masterTableVersionNumber='latest'):
        d = descriptor(code, masterTableVersionNumber)
        self._check(d)
        self._list.insert(i, d)


# ----------------------


def def_is_in(code, search_in, masterTableVersionNumber='latest'):
    seq = get_sequence_def(masterTableVersionNumber)

    def def_is_in_internal(masterTableVersionNumber, code, search_in):
        if not isinstance(search_in, list):
            search_in = [search_in]
        if not isinstance(code, list):
            code = [code]
        ret = [False] * len(code)
        for i in range(len(code)):
            if code[i] in search_in:
                ret[i] = True
            for k in search_in:
                if k in seq.keys():
                    for j in seq[k]:
                        if def_is_in_internal(masterTableVersionNumber,
                                              code[i], j):
                            ret[i] = True
                            break
        if len(ret) == 1:
            ret = ret[0]
        return(ret)
    return(def_is_in_internal(masterTableVersionNumber, code, search_in))


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
        d = descriptor(args.lookup)
        elapsed_time = _time.clock() - t
        print(d.__str__(show_desc=args.description))
        print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except Exception as e:
        eprint(e)


if __name__ == "__main__":
    _exit(_main_())

