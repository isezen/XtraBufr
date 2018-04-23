"""
xtrabufr.extra
~~~~~~~~~~~~~~~~~~
Additional functions to ecCodes python package
"""

# from __future__ import print_function
# from __future__ import absolute_import

import os as _os
import eccodes as _ec
from numpy import ndarray as _nd
from collections import OrderedDict as _od
from subprocess import check_output as _chekout

__all__ = ['codes_info', 'codes_get_definitions_path', 'msg_count',
           'extract_subset', 'get_msg', 'copy_msg_from_file']

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


# _codes_definition_path_ = codes_get_definitions_path()


def get_keys(bufr_handle):
    """Get keys from a BUFR handle

    :param bufr: Handle to BUFR file
    :returns: List of keys
    """
    keys = []
    iterid = _ec.codes_bufr_keys_iterator_new(bufr_handle)
    while _ec.codes_bufr_keys_iterator_next(iterid):
        keys.append(_ec.codes_bufr_keys_iterator_get_name(iterid))
    _ec.codes_bufr_keys_iterator_delete(iterid)
    return(keys)


def get_val(bufr_handle, key):
    """Read value of a key in from a BUFR message

    If value is missing returns None

    :param bufr_handle: Handle to BUFR file
    :param key: Key value
    :returns: Read value
    """
    size = _ec.codes_get_size(bufr_handle, key)
    v = None
    if size == 1:
        v = _ec.codes_get(bufr_handle, key)
        if isinstance(v, float):
            if v == _ec.CODES_MISSING_DOUBLE:
                v = None
        if isinstance(v, int):
            if v == _ec.CODES_MISSING_LONG:
                v = None
    else:
        v = _ec.codes_get_array(bufr_handle, key)
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


def extract_subset(bufr_handle, subset):
    """Extract a subset from a BUFR handle

    :param bufr_handle: Handle to BUFR message
    :param subset: Number of subset
    :returns: Handle to BUFR message contains subset
    """
    _ec.codes_set(bufr_handle, 'extractSubset', subset)
    _ec.codes_set(bufr_handle, 'doExtractSubsets', 1)
    bufr_handle2 = _ec.codes_clone(bufr_handle)
    _ec.codes_set(bufr_handle2, 'unpack', 1)
    return(bufr_handle2)


def get_msg(file_handle, msg=1, subset=None):
    """Get message by id and/or subset from a BUFR file

    You have to open file first and send the file handle to this function

    :param file_handle: File handle to BUFR file
    :param msg: Number of message
    :param subset: Number of subset
    :returns: Handle to BUFR message or None
    """
    i = 0
    ret = None
    while True:
        i += 1
        bufr = _ec.codes_bufr_new_from_file(file_handle)
        if i == msg:
            if subset is None:
                ret = bufr
            else:
                nos = _ec.codes_get(bufr, 'numberOfSubsets')
                if subset < 1 and subset > nos:
                    raise ValueError('subset must between 1-{}'.format(nos))
                _ec.codes_set(bufr, 'unpack', 1)
                ret = extract_subset(bufr, subset)
            break
        _ec.codes_release(bufr)
    if ret is None:
        n = _ec.codes_count_in_file(file_handle)
        raise ValueError('msg must be between 1-{}'.format(n))
    return(ret)


def msg_count(bufr_file):
    """Return number of messages in a BUFR file

    :param bufr_file: Path to BUFR file
    :returns: Number of messages in a BUFR file
    """
    ret = None
    with open(bufr_file, 'rb') as f:
        ret = _ec.codes_count_in_file(f)
    return(ret)


def copy_msg_from_file(bufr_in, bufr_out, msg=1, subset=None):
    """Copy message and subset from a BUFR file into a new BUFR file

    Whole message is copied if subset was not defined.

    :param bufr_in: Path to BUFR file to read
    :param bufr_out: Path to BUFR file to save
    :param msg: Number of message
    :param subset: Number of subset
    :returns: None
    """
    with open(bufr_in, 'rb') as fin:
        bufr = get_msg(fin, msg, subset)
        if bufr is not None:
            with open(bufr_out, 'wb') as fout:
                _ec.codes_write(bufr, fout)
            _ec.codes_release(bufr)

