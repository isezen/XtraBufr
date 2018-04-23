"""
xtrabufr.definitions
~~~~~~~~~~~~~~~~~~
Additional functions to to work on definitions
"""

from __future__ import generators
import os as _os
import re as _re
import ctypes as _ct
from copy import deepcopy as _dcopy
from platform import system as _system
from collections import OrderedDict as _od
from subprocess import check_output as _chekout
from ._extra_ import codes_get_definitions_path as _codes_def_path


__all__ = ['get_element_table', 'get_bufr_template_def', 'get_sequence_def',
           'get_code_table', 'get_value_from_code_table',
           'shrink_descriptors', 'expand_descriptors']

_def_catch_ = {}
_codes_definition_path_ = _codes_def_path()


class _entry_(_ct.Structure):
    _fields_ = [("path", _ct.c_char_p),
                ("content", _ct.POINTER(_ct.c_ubyte)),
                ("length", _ct.c_size_t)]


def _get_lib_path_(lib_name='libeccodes'):
    """Return libeccodes_memfs lib file
    """
    system_name = _system()
    codes_info_path = _chekout(['which', 'codes_info']).strip()
    if system_name == 'Linux':
        q = ['ldd']
        ext = 'so'
    elif system_name == 'Darwin':
        q = ['otool', '-L']  # this reqires xcode to be installed
        ext = 'dylib'
        q.append(codes_info_path)
    lib_path = None
    lib_name_ext = lib_name + '.' + ext
    for i in _chekout(q).strip().split('\n'):
        i = i.strip()
        if lib_name_ext in i:
            lib_path = i.split(' ')[0]
            break

    if not _os.path.exists(lib_path):
        env_var = lib_name.upper() + '_PATH'
        try:
            lib_path = _os.environ[env_var]
        except KeyError:
            raise KeyError(lib_name_ext + ' file can not be found. ' +
                           'Please, set ' + env_var + ' environment variable.')
    return(lib_path)


def _get_entry_(path):
    """Get entry from file system or MEMFS

    :path: A valid path to entry/ definition file
    """
    if 'MEMFS' in _codes_definition_path_:
        lib = _ct.cdll.LoadLibrary(_get_lib_path_('libeccodes_memfs'))
        entries = _ct.POINTER(_entry_)
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


def _knuth_morris_pratt_(text, pattern):
    '''Yields all starting positions of copies of the pattern in the text.
    Calling conventions are similar to string.find, but its arguments can be
    lists or iterators, not just strings, it returns all matches, not jus
    the first one, and it does not need the whole text in memory at once.
    Whenever it yields, it will have read the text exactly up to and including
    the match that caused the yield.

    see: http://code.activestate.com/recipes/117214/
    '''

    # allow indexing into pattern and protect against change during yield
    pattern = list(pattern)

    # build table of shift amounts
    shifts = [1] * (len(pattern) + 1)
    shift = 1
    for pos in range(len(pattern)):
        while shift <= pos and pattern[pos] != pattern[pos - shift]:
            shift += shifts[pos - shift]
        shifts[pos + 1] = shift

    # do the actual search
    startPos = 0
    matchLen = 0
    for c in text:
        while (matchLen == len(pattern) or
               matchLen >= 0 and pattern[matchLen] != c):
            startPos += shifts[matchLen]
            matchLen -= shifts[matchLen]
        matchLen += 1
        if matchLen == len(pattern):
            yield startPos


def get_element_table(masterTableVersionNumber='latest', by_code=True):
    """Get element table

    :masterTableVersionNumber: WMO master table version Number
    :return: Element table as dict
    """
    path = _codes_definition_path_ + '/bufr/tables/0/wmo/{}/element.table'
    path = path.format(masterTableVersionNumber)
    if path + str(by_code) in _def_catch_.keys():
        return(_def_catch_[path + str(by_code)])
    content = _get_entry_(path)
    table = {}
    for line in content.split('\n'):
        if line != '':
            s = line.split('|')
            if s[0] != '#code':
                if by_code:
                    table[int(s[0])] = s[1:]
                else:
                    table[s[1]] = [s[0]] + s[2:]
    _def_catch_[path + str(by_code)] = table
    return(table)


def get_bufr_template_def():
    """Get bufr_template.def
    :return: bufr_template.def as dict
    """
    path = _codes_definition_path_ + '/bufr/templates/BufrTemplate.def'
    if path in _def_catch_.keys():
        return(_def_catch_[path])
    content = _re.sub(r'[ {\[;"\]}]', '', _get_entry_(path))
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
    """Get sequence.def table

    :masterTableVersionNumber: WMO master table version Number
    :return: sequence.def as dict
    """
    path = _codes_definition_path_ + '/bufr/tables/0/wmo/{}/sequence.def'
    path = path.format(masterTableVersionNumber)
    if path in _def_catch_.keys():
        return(_def_catch_[path])
    content = _get_entry_(path)
    ls = _re.split(r" = \[| \]\n", content)
    d = _od()
    for i in range(0, len(ls), 2):
        if ls[i] != '':
            k = int(ls[i].replace(' ', '').replace('"', ''))
            v = [int(j) for j in ls[i + 1].replace(' ', '').split(',')]
            d[k] = v
    # this is required to run shrink method properly.
    d = _od(sorted(d.iteritems(), key=lambda x: len(x[1])))
    _def_catch_[path] = d
    return(d)


def get_code_table(code, masterTableVersionNumber='latest'):
    """Get code.table

    :code: A valid WMO code
    :masterTableVersionNumber: WMO master table version Number
    :return: sequence.def as dict
    """
    def_path = _codes_definition_path_
    path = def_path + '/bufr/tables/0/wmo/{}/codetables/{}.table'
    path = path.format(masterTableVersionNumber, int(code))
    if path in _def_catch_.keys():
        return(_def_catch_[path])
    content = _get_entry_(path)
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


def get_value_from_code_table(value, code, masterTableVersionNumber='latest'):
    """Get string representation of a value from code.table

    :value: A valid value from a code.table
    :code: A valid WMO code
    :masterTableVersionNumber: WMO master table version Number
    :return: sequence.def as dict
    """
    if value is None:
        return(None)
    if isinstance(value, list):
        return([get_value_from_code_table(v, code, masterTableVersionNumber)
                for v in value])
    ct = get_code_table(code, masterTableVersionNumber)
    if value not in ct.keys():
        return(value)
    return(ct[value])


def shrink_descriptors(code, depth=99, masterTableVersionNumber='latest'):
    """Shrink descriptor(s)

    :code: A single integer or list of integers
    :depth: Shrink depth
    :masterTableVersionNumber: WMO master table version Number
    :return: A list of shrinked descriptors
    """
    seq = get_sequence_def(masterTableVersionNumber)
    # WARNING: order of sequence by length is important!
    if not isinstance(code, list):
        code = [code]
    code = _dcopy(code)
    for _ in range(depth):
        shrink_element = {}
        for k, v in seq.items():
            j = list(_knuth_morris_pratt_(code, v))
            for i in j:
                shrink_element[i] = [k, len(v)]
        if len(shrink_element) == 0:
            break
        elements = [n for k, v in shrink_element.items()
                    for n in list(range(k + 1, k + v[1]))]
        for k, v in shrink_element.items():
            code[k] = v[0]
        shrinked = []
        for l, val in enumerate(code):
            if l not in elements:
                shrinked.append(val)
        code = shrinked
    return(code)


def expand_descriptors(code, depth=99, masterTableVersionNumber='latest'):
    """Expand descriptor(s)

    :code: A single integer or list of integers
    :depth: Expansion depth
    :masterTableVersionNumber: WMO master table version Number
    :return: A list of expanded descriptors
    """
    seq = get_sequence_def(masterTableVersionNumber)
    if not isinstance(code, list):
        code = [code]
    expanded = []
    for c in code:
        for i in range(depth):
            if c in seq.keys():
                c = expand_descriptors(seq[c], depth - 1,
                                       masterTableVersionNumber)
            else:
                break
        if not isinstance(c, list):
            c = [c]
        expanded.extend(c)
    return(expanded)


def desc_is_in(code, search_in, masterTableVersionNumber='latest'):
    """Check a descriptor(s) wether is in another descriptor(s)

    :code: A single or list of integer(s)
    :search_in: A single or list of integer(s)
    :masterTableVersionNumber: WMO master table version Number
    :return: True or False. if code is a list, returns list of bool.
    """
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
