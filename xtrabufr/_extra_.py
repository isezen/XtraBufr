"""
xtrabufr.extra
~~~~~~~~~~~~~~~~~~
Additional functions to ecCodes python package
"""

from __future__ import print_function
# from __future__ import absolute_import

import os as _os
import eccodes as _ec
from numpy import ndarray as _nd
from sys import stderr as _stderr
from collections import OrderedDict as _od

__all__ = ['msg_count', 'extract_subset', 'get_msg', 'copy_msg_from_file']


def _eprint_(*args, **kwargs):
    print('ERROR: ', *args, file=_stderr, **kwargs)


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


def get_header_and_unpack(bufr_handle, msg=None):
    """Get header from BUFR handle and unpack

    :param bufr_handle: Handle to BUFR file
    :param msg: Id if message (Required only for Error handling)
    :returns: (OrderedDict) Header key and values
    """
    header_keys = get_keys(bufr_handle)
    h = _od([(k, get_val(bufr_handle, k)) for k in header_keys])

    try:
        _ec.codes_set(bufr_handle, 'unpack', 1)
    except _ec.DecodingError as e:
        _eprint_('MSG #{} {}'.format(msg, e.msg))
        return(None)

    return(h)


def read_compressed_msg(bufr_handle, msg=None, subset=None):
    """Read compressed message from BUFR file

    :param bufr_handle: Handle to BUFR file
    :param msg: Number of message to read
    :param subset: Number of subset in message to read
                   If None, all subsets are return
    :returns: (OrderedDict) Read message
    """
    h = get_header_and_unpack(bufr_handle, msg)
    if h is None:
        return(None)

    keys = [k for k in get_keys(bufr_handle) if k not in h.keys()]
    s = _od([(k, get_val(bufr_handle, k)) for k in keys])

    if subset is not None:
        if isinstance(subset, list):
            for k, v in s.items():
                if isinstance(v, list):
                    s[k] = [s[k][i - 1] for i in s]

    return({'header': h, 'subset': {'compressed': s}})


def read_uncompressed_msg(bufr_handle, msg=None, subset=None):
    """Read uncompressed message from BUFR file

    :param bufr_handle: Handle to BUFR file
    :param msg: Id of message to read
    :param subset: Id of subset in message to read
                      If None, all subsets are read
    :returns: (OrderedDict) Read message
    """
    h = get_header_and_unpack(bufr_handle, msg)
    if h is None:
        return(None)

    if subset is None:
        nos = get_val(bufr_handle, 'numberOfSubsets')
        subset = list(range(1, nos + 1))

    s = {}
    for i in subset:

        try:
            bufr2_handle = extract_subset(bufr_handle, i)
        except _ec.CodesInternalError as e:
            _eprint_('MSG #{} - Subset #{} "{}"'.format(msg, i, e.msg))
            break

        keys = [k for k in get_keys(bufr2_handle) if k not in h.keys()]
        s[i] = _od([(k, get_val(bufr2_handle, k)) for k in keys])
        _ec.codes_release(bufr2_handle)
    return({'header': h, 'subset': s})


def read_msg(bufr_file, msg=None, subset=None):
    """ Read a message at a time from BUFR file

    This is a generator function. Returns Number of message and
    message content each time. If you want result as a dictionary,

    {i: m for i, m in read_msg(bufr_file)}

    :param bufr_file: BUFR file name
    :param msg: Id of message to read
    :param subset: Id of subset in message to read
                   If None, all subsets are read
    :return: (OrderedDict) Read message
    """

    if isinstance(msg, int):
        msg = set([msg])

    if msg is None:
        msg = set(range(1, msg_count(bufr_file) + 1))

    if isinstance(subset, int):
        subset = set([subset])

    i = 0
    with open(bufr_file, 'rb') as f:
        while True:
            i += 1
            bufr_handle = _ec.codes_bufr_new_from_file(f)

            if bufr_handle is None:
                break

            if i in msg:
                msg.remove(i)
                #
                compressed = get_val(bufr_handle, 'compressedData') == 1
                fun = read_compressed_msg if compressed \
                    else read_uncompressed_msg
                yield(i, fun(bufr_handle, i, subset))

            _ec.codes_release(bufr_handle)
            if len(msg) == 0:
                break


def bufr_filter(bufr_files, **kwargs):
    """ Filter BUFR files(s) by condition

    This function works only for header keys.

    :param bufr_files: Path to bufr file(s)
    :param **kwargs: A valid key to filter
        edition
        masterTableNumber
        bufrHeaderCentre
        bufrHeaderSubCentre
        updateSequenceNumber
        dataCategory
        internationalDataSubCategory
        dataSubCategory
        masterTablesVersionNumber
        localTablesVersionNumber
        typicalYear
        typicalMonth
        typicalDay
        typicalHour
        typicalMinute
        typicalSecond
        observedData
        compressedData
        unexpandedDescriptor
        typicalTime
        typicalDate
    :return: Yields bufr_handle
    """

    def bufr_filter_file(bufr_file, filters):
        bufr_handles = []
        fltr = filters[:]
        fl = fltr[0]
        del fltr[0]
        with open(bufr_file, 'rb') as f:
            while True:
                bufr_handle = _ec.codes_bufr_new_from_file(f)
                if bufr_handle is None:
                    break
                val = get_val(bufr_handle, fl[0])
                if not isinstance(val, list):
                    val = [val]
                if any([v in val for v in fl[1]]):
                    bufr_handles.append(bufr_handle)
                    continue
                _ec.codes_release(bufr_handle)

        for fl in fltr:
            for i in range(len(bufr_handles) - 1, -1, -1):
                val = get_val(bufr_handles[i], fl[0])
                if not isinstance(val, list):
                    val = [val]
                if not any([v in val for v in fl[1]]):
                    _ec.codes_release(bufr_handles[i])
                    del bufr_handles[i]
        bufr_handles = [i for i in bufr_handles if i > -1]
        return(bufr_handles)

    if not isinstance(bufr_files, list):
        bufr_files = [bufr_files]

    kw = {k: v for k, v in kwargs.items() if v is not None}
    for k in kw.keys():
        if not isinstance(kw[k], list):
            kw[k] = [kw[k]]

    filters = [[k, v] for k, v in kw.items()]

    for f in bufr_files:
        handles = bufr_filter_file(f, filters)
        for h in handles:
            yield h
            _ec.codes_release(h)


def bufr_filter_dump(bufr_files, bufr_out, plain=False, **kwargs):
    n = 0
    with open(bufr_out, 'wb') as fout:
        for bufr_handle in bufr_filter(bufr_files, **kwargs):
            if bufr_handle is not None:
                n += 1
                _ec.codes_write(bufr_handle, fout)
    if n == 0:
        _os.remove(bufr_out)
    return(n)

