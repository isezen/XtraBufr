"""
xtrabufr._eccodes_tools_
~~~~~~~~~~~~~~~~~~
Functions from ecCodes tools
"""

import os as _os
from collections import OrderedDict as _od
from subprocess import check_output as _chekout


__all__ = ['codes_info', 'codes_get_definitions_path']

_codes_definition_path_ = None


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
