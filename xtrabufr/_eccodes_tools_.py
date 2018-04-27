"""
xtrabufr._eccodes_tools_
~~~~~~~~~~~~~~~~~~
Functions from ecCodes tools
"""

from __future__ import print_function
import os as _os
from collections import OrderedDict as _od
from subprocess import check_output as _chekout


__all__ = ['codes_info', 'codes_get_definitions_path']

_codes_definition_path_ = None


def bufr_dump(bufr_files, options=''):
    try:
        ret = _chekout(['bufr_dump', options, bufr_files])
    except OSError as e:
        raise OSError('bufr_dump tool was not found')
    return(ret)


def codes_info(args):
    if not isinstance(args, list):
        args = [args]
    try:
        ret = _od([(p, _chekout(['codes_info', '-' + p]).strip())
                   for p in args])
    except OSError as e:
        raise OSError('codes_info tool was not found')
    if len(ret) == 1:
        return(ret.values()[0])
    return(ret)


def codes_get_definitions_path():
    global _codes_definition_path_
    if _codes_definition_path_ is not None:
        return(_codes_definition_path_)
    try:
        _codes_definition_path_ = _os.environ['ECCODES_DEFINITION_PATH']
    except KeyError as e:
        _codes_definition_path_ = codes_info('d')
    return(_codes_definition_path_)


_codes_definition_path_ = codes_get_definitions_path()
