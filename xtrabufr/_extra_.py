"""
xtrabufr.extra
~~~~~~~~~~~~~~~~~~
Additional functions to ecCodes python package
"""

from __future__ import print_function
# from __future__ import absolute_import

import os as _os
import sys as _sys
import csv as _csv
import eccodes as _ec
import json as _json
from numpy import ndarray as _nd
from copy import deepcopy as _deepcopy
from collections import OrderedDict as _od
from types import GeneratorType as _GeneratorType
from contextlib import contextmanager as _contextmanager
from definitions import get_value_from_code_table as _get_value_from_code_table


__all__ = [
    'msg_count', 'extract_subset', 'get_msg', 'decode', 'copy_msg', 'header',
    'iter_subsets', 'iter_messages', 'iter_synop', 'dump', 'BufrHandle',
    'new_msg_from', 'nsub', 'to_csv', 'clone', 'synop_to_csv',
    'synop_to_json', 'json', 'iter_decode']

_header_keys_ = [
    'edition', 'masterTableNumber', 'bufrHeaderCentre', 'bufrHeaderSubCentre',
    'updateSequenceNumber', 'dataCategory', 'internationalDataSubCategory',
    'dataSubCategory', 'masterTablesVersionNumber', 'localTablesVersionNumber',
    'typicalYear', 'typicalMonth', 'typicalDay', 'typicalHour',
    'typicalMinute', 'typicalSecond', 'numberOfSubsets', 'observedData',
    'compressedData', 'unexpandedDescriptors']

_synop_keys_ = [
    'masterTablesVersionNumber', 'bufrHeaderCentre',
    'blockNumber', 'stationNumber', 'stationType', 'stationOrSiteName',
    'year', 'month', 'day', 'hour', 'minute', 'latitude', 'longitude',
    '#1#heightOfStationGroundAboveMeanSeaLevel',
    '#1#heightOfBarometerAboveMeanSeaLevel', '#1#nonCoordinatePressure',
    '#1#pressureReducedToMeanSeaLevel', '#1#3HourPressureChange',
    '#1#characteristicOfPressureTendency', '#1#pressure',
    '#1#airTemperature', '#1#dewpointTemperature', '#1#relativeHumidity',
    '#1#horizontalVisibility', '#1#cloudCoverTotal',
    '#1#heightOfBaseOfCloud', '#1#cloudType', '#2#cloudType',
    '#3#cloudType', '#1#presentWeather', '#1#pastWeather1',
    '#1#pastWeather2', '#1#windSpeed', '#1#windDirection']


class BufrHandle(object):
    """A wrapper class for handle to a BUFR message

    Hence, you don't have to release bufr handles. They are released
    automatically if you don't have any reference to a it.
    """

    def __init__(self, handle, id=None, file_name=None):
        self._handle = handle
        self._id = id
        self._file_name = file_name

    def __repr__(self):
        s = 'BufrHandle {{file: {} id: {} handle: {}}}'
        return(s.format(self.file_name, self.id, self.handle))

    def __eq__(self, other):
        if other is None:
            return(self.handle is None)
        return(isinstance(other, self.__class__) and
               self._handle == other._handle)

    def __copy__(self):
        cls = self.__class__
        new = cls.__new__(cls)
        new.__dict__.update(self.__dict__)
        return(new)

    def __deepcopy__(self, memo):
        cls = self.__class__
        new = cls.__new__(cls)
        memo[id(self)] = new
        for k, v in self.__dict__.items():
            if k == '_handle':
                setattr(new, k, _ec.codes_clone(v))
            else:
                setattr(new, k, _deepcopy(v, memo))
        return(new)

    def __del__(self):
        # print(self, 'deleted')
        _ec.codes_release(self._handle)

    def __exit__(self):
        print('EXIT')
        self.__del__()

    @property
    def handle(self):
        return(self._handle)

    @property
    def id(self):
        return(self._id)

    @property
    def file_name(self):
        return(self._file_name)

    @property
    def compressed(self):
        return(get_val(self, 'compressedData') == 1)


def _eprint_(*args, **kwargs):
    print('ERROR:', *args, file=_sys.stderr, **kwargs)


@_contextmanager
def _open_(filename, mode='Ur'):
    """Open a file or pipe to stdin/stdout"""
    f = (_sys.stdin if mode is None or mode == '' or 'r' in mode
         else _sys.stdout) if filename == '-' else open(filename, mode)
    try:
        yield f
    finally:
        if filename is not '-':
            f.close()


def get_attr(bufr_handle, key):
    """Get attributes of a key from BufrHandle object

    :param bufr_handle: BufrHandle Object
    :param key: A string key name
    :returns: (OrderedDict) attributes
    """
    attrs = ['code', 'units', 'scale', 'reference', 'width']
    attributes = _od.fromkeys(attrs)
    for a in attrs:
        k = key + '->' + a
        try:
            attributes[a] = _ec.codes_get(bufr_handle.handle, k)
        except _ec.CodesInternalError:
            continue
    return(attributes)


def get_attributes(bufr_handle, keys):
    """Get attributes of keys

    :param bufr_handle: BufrHandle Object
    :param key: A list of key names
    :returns: (OrderedDict) key names and attributes
    """
    return(_od([(k, get_attr(bufr_handle, k)) for k in keys]))


def get_size(bufr_handle):
    """Get message size in bytes"""
    return(_ec.codes_get_message_size(bufr_handle.handle))


def get_keys(bufr_handle):
    """Get keys from  BufrHandle object

    :param bufr_handle: BufrHandle Object
    :returns: List of keys
    """
    keys = []
    iterid = _ec.codes_bufr_keys_iterator_new(bufr_handle.handle)
    while _ec.codes_bufr_keys_iterator_next(iterid):
        keys.append(_ec.codes_bufr_keys_iterator_get_name(iterid))
    _ec.codes_bufr_keys_iterator_delete(iterid)
    return(keys)


def get_val(bufr_handle, key):
    """Read value of a key from BufrHandle object

    If value is missing returns None

    :param bufr_handle: BufrHandle object
    :param key: Key value
    :returns: Value of the key
    """
    v = None
    h = bufr_handle.handle
    try:
        size = _ec.codes_get_size(h, key)
        if size == 1:
            v = _ec.codes_get(h, key)
            # if isinstance(v, str):
            #     if '\xff' in v:
            #         v = v.replace('\xff', '?')
            if isinstance(v, float):
                if v == _ec.CODES_MISSING_DOUBLE:
                    v = None
                else:
                    v = round(v, 6)
            if isinstance(v, int):
                if v == _ec.CODES_MISSING_LONG:
                    v = None
        else:
            v = _ec.codes_get_array(h, key)
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
                        else:
                            v[i] = round(v[i], 6)

            return(v)
    except _ec.KeyValueNotFoundError:
        return('KeyNotFound')
    return(v)


def pack(bufr_handle):
    """Pack BufrHandle object"""
    _ec.codes_set(bufr_handle.handle, 'pack', 1)


def unpack(bufr_handle):
    """Unpack BufrHandle object
    :returns: True if operation is succeed else False
    """
    try:
        _ec.codes_set(bufr_handle.handle, 'unpack', 1)
        return(True)
    except _ec.DecodingError as e:
        s = '(UNPACK) FILE: {} MSG #{} - {}'
        _eprint_(s.format(bufr_handle.file_name,
                          bufr_handle.id, e.msg))
    return(False)


def clone(bufr_handle):
    """Clone BufrHandle Object (deepcopy)

    :param bufr_handle: BufrHandle Object
    :returns: BufrHandle Object
    """
    return(_deepcopy(bufr_handle))


def extract_subset(bufr_handle, subset):
    """Extract subset(s) from a BufrHandle Object

    subset can be a single integer or a list yields start
    and end of subsets.

    WARNING: This function modifies original message. Consider clone
             the original message before.

    :param bufr_handle: BufrHandle Object
    :param subset: Number of subset
    :returns: Handle to BUFR message contains subset
    """
    if isinstance(subset, list):
        if len(subset) == 1:
            subset = subset[0]

    if not unpack(bufr_handle):
        return(None)

    h = bufr_handle.handle
    try:
        if isinstance(subset, list):
            if min(subset) == max(subset):
                raise ValueError('min and max value of subset cannot be equal')
            _ec.codes_set(h, 'extractSubsetIntervalStart', min(subset))
            _ec.codes_set(h, 'extractSubsetIntervalEnd', max(subset))
        else:
            _ec.codes_set(h, 'extractSubset', subset)
        _ec.codes_set(h, 'doExtractSubsets', 1)
    except _ec.CodesInternalError as e:
        s = 'FILE: {} - MSG #{} - Subset #{} "{}"'
        _eprint_(s.format(bufr_handle.file_name, bufr_handle.id,
                          subset, e))
        return(None)
    return(clone(bufr_handle))


def header(bufr_handle):
    """Get header values from BufrHandle object

    :param bufr_handle: BufrHandle object
    :returns: (OrderedDict) Header key and values
    """
    return(_od([(k, get_val(bufr_handle, k)) for k in _header_keys_]))


def nsub(bufr_handle):
    """Number of subsets"""
    return(_ec.codes_get(bufr_handle.handle, 'numberOfSubsets'))


def iter_decode(x, keys=None, merge=True):
    if isinstance(x, BufrHandle):
        yield(decode(x, keys, merge))
    elif isinstance(x, _GeneratorType) or isinstance(x, list):
        for h in x:
            yield(decode(h, keys, merge))


def decode(x, keys=None, merge=False, decode_code_table=False):
    """Decode a BufrHandle object
    :param x: BufrHandle object
    :param keys: If defined, only values of defined keys are returned
    """

    if isinstance(x, _GeneratorType) or isinstance(x, list):
        if keys is None:
            d = [decode(h, keys, merge) for h in x]
        else:
            if merge:
                s = _od([(k, []) for k in keys])
                for d in iter_decode(x, keys, merge):
                    if d is not None:
                        for k in keys:
                            s[k].extend(d[k])
                d = s
            else:
                s = []
                for d in iter_decode(x, keys, merge):
                    if d is not None:
                        for i in d:
                            s.append(i)
                d = s

        return(d)

    if not unpack(x):
        return(None)

    mtvn = get_val(x, 'masterTablesVersionNumber')

    def gv(bh, k):
        v = get_val(bh, k)
        if decode_code_table:
            a = get_attr(bh, k)
            if a['units'] == 'CODE TABLE':
                v = _get_value_from_code_table(v, a['code'], mtvn)
        return(v)

    if keys is None:
        h = header(x)

        def decode_subset(bufr_handle):
            keys2 = [k for k in get_keys(bufr_handle)
                     if k not in _header_keys_]
            return(_od([(k, gv(x, k)) for k in keys2]))

        def decode_comp():
            return({'compressed': decode_subset(x)})

        def decode_uncomp():
            return([decode_subset(s) for s in iter_subsets(x)])

        fun = decode_comp if x.compressed else decode_uncomp
        return(_od([('header', h), ('subset', fun())]))
    else:
        if nsub(x) == 1:
            if merge:
                return(_od([(k, [gv(x, k)]) for k in keys]))
            else:
                return([_od([(k, gv(x, k)) for k in keys])])
        else:
            if merge:
                s = _od([(k, []) for k in keys])
                for subset in iter_subsets(x):
                    for k in keys:
                        s[k].append(gv(subset, k))
                return(s)
            else:
                return([_od([(k, gv(i, k)) for k in keys])
                        for i in iter_subsets(x)])


def msg_count(bufr_file):
    """Return number of messages in a BUFR file

    :param bufr_file: Path to BUFR file
    :returns: Number of messages in a BUFR file
    """
    ret = None
    with open(bufr_file, 'rb') as f:
        ret = _ec.codes_count_in_file(f)
    return(ret)


def new_msg_from(bufr_file):
    """Message generator for BUFR file
    :param bufr_file: Path to BUFR file
    :returns: BufrHandle Object
    """
    with _open_(bufr_file, 'rb') as f:
        i = 0
        while True:
            h = _ec.codes_bufr_new_from_file(f)
            i += 1
            if h is None:
                break
            yield(BufrHandle(h, i, bufr_file))


def dump(x, bufr_out=None):
    """Dump a BufrHandle object or results of a generator function

    If x is BufrHandle object, bufr_out is ignored
    If bufr_out is None, binary content of the message(s) is returned.
    If bufr_out is '-', binary content sent to stdout.

    :param x: A BufrHandle object or a function generates BufrHandle objects
    :param bufr_out: Path to output file
    :returns: Number of dumped messages or binary content of messages
    """
    if bufr_out is None:
        if isinstance(x, BufrHandle):
            return(_ec.codes_get_message(x.handle))
        elif isinstance(x, _GeneratorType) or isinstance(x, list):
            return(b''.join([dump(h) for h in x]))
    else:
        r = 0
        with _open_(bufr_out, 'wb') as f:
            if isinstance(x, BufrHandle):
                r = 1
                _ec.codes_write(x.handle, f)
            elif isinstance(x, _GeneratorType) or isinstance(x, list):
                for h in x:
                    r += 1
                    _ec.codes_write(h.handle, f)
            if r == 0 and bufr_out != '-':
                _os.remove(bufr_out)
        return(r)
    raise TypeError('x must be a BufrHandle object, or a list/generator \
        of BufrHandle objects')


def json(x, file_out=None, keys=None, merge=False, decode_code_table=False,
         indent=2):
    """Convert a BufrHandle object or results of a generator function to JSON

    If x is BufrHandle object, bufr_out is ignored
    If file_out is None, binary content of the message(s) is returned.
    If file_out is '-', binary content sent to stdout.

    :param x: A BufrHandle object or a function generates BufrHandle objects
    :param file_out: Path to output file
    :returns: Number of processed messages or json format of message(s).
    """
    if file_out is None:
        return(_json.dumps(decode(x, keys, merge, decode_code_table),
                           ensure_ascii=False, indent=indent))
    else:
        r = 0
        with _open_(file_out, 'w') as f:
            d = decode(x, keys, merge, decode_code_table)
            _json.dump(d, f, ensure_ascii=False, indent=indent)
            r = len(d[keys[0]]) if merge else len(d)
        if r == 0 and file_out != '-':
            _os.remove(file_out)
        return(r)
    raise TypeError('x must be a BufrHandle object, or a list/generator \
        of BufrHandle objects')


def iter_subsets(x):
    """Iterate over subsets in a BufrHandle or list or a Generator function
    :param x: BufrHandle/list of BufrHandles/Generator Function
    :returns: BufrHandle Object
    """
    if isinstance(x, BufrHandle):
        cx = clone(x)
        for i in range(1, nsub(x) + 1):
            subset = extract_subset(cx, i)
            if subset is None:
                break
            if unpack(subset):
                yield(subset)
    elif isinstance(x, _GeneratorType) or isinstance(x, list):
        for i in x:
            for j in iter_subsets(i):
                yield(j)
    else:
        raise TypeError('x must be a BufrHandle object, or a list/generator \
            of BufrHandle objects')


def iter_messages(bufr_files, **filters):
    """Iterate over messages in BUFR files(s)

    Also messages can be filtered by message id and header keys.
    if msg_id is defined other rules are ignored except subset.

    This is a generator function

    :param bufr_files: Path to bufr file(s)
    :param **filters: Dictionary of keys to filter
        msg (Message id(s))
        subset (Subset Id(s))
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

    def key_value_found(fl, bufr_handle):
        if fl[0] is None:
            return(True)
        if fl[0] == 'msg':
            return(bufr_handle.id in fl[1])
        else:
            val = get_val(bufr_handle, fl[0])
            if not isinstance(val, list):
                val = [val]
            ret = [v == val if isinstance(v, list) else v in val
                   for v in fl[1]]
            return(any(ret))

    def is_in(filters, bh):
        for fl in filters:
            if not key_value_found(fl, bh):
                return(False)
        return(True)

    def iter_file(bufr_file, filters):
        if len(filters) == 0:
            filters = [[None, None]]
        for bh in new_msg_from(bufr_file):
            if filters[0] == 'msg':
                if bh.id > max(filters[1]):
                    break
            if is_in(filters, bh):
                yield(bh)

    if not isinstance(bufr_files, list):
        bufr_files = [bufr_files]

    filters = {k: v for k, v in filters.items() if v is not None}
    for k in filters.keys():
        if not isinstance(filters[k], list):
            filters[k] = [filters[k]]

    subset = None
    if 'subset' in filters.keys():
        subset = filters['subset']
        del filters['subset']

    if 'msg' in filters.keys():
        filters = {'msg': filters['msg']}

    for f in bufr_files:
        for bh in iter_file(f, [[k, v] for k, v in filters.items()]):
            if subset is not None:
                bh = extract_subset(clone(bh), subset)
            if bh is not None:
                yield(bh)


def iter_synop(bufr_files, **filters):
    """Iterates synop messages in BUFR file(s)

    This is a generator function

    :param bufr_files: BUFR file(s)
    :return: yields handle to synop BUFR message
    """

    filters['dataCategory'] = 0
    filters['unexpandedDescriptors'] = [
        307080, 307086, 307096,
        [307086, 1023, 4025, 2177, 101000, 31001, 20003,
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
        [307079, 4025, 11042]]
    return(iter_messages(bufr_files, **filters))

    # for s in iter_subsets(iter_messages(bufr_files, **filters)):
    #     if get_val(s, 'latitude') is not None:
    #         yield(s)


def get_msg(bufr_files, msg=1, subset=None):
    """Get handle(s) to the message(s)

    This is a wrapper around iter_messages function and only for completeness.
    WARNING: Do not try to load all messages from a file by this method.
             Instead, use generator functions. Otherwise, you will likely get
             a memory bump.

    :param bufr_files: BUFR files to read
    :param msg: Id of message or a list contains Ids
    :param subset: Subset Number or interval to extract subsets
    :returns: BufrHandle object or list of BufrHandle objects
    """
    handles = [h for h in iter_messages(bufr_files,
                                        msg=msg, subset=subset)]
    if len(handles) == 1:
        return(handles[0])
    return(handles)


def copy_msg(bufr_files, bufr_out, msg=1, subset=None):
    """Copy message and subset from BUFR file(s) into a new file

    Whole message is copied if subset was not defined.

    :param bufr_files: Path to BUFR file(s) to read
    :param bufr_out: Path to BUFR file to save
    :param msg: Id's of message(s)
    :param subset: Id's subset(s) (or an interval)
    :returns: Number of copied messages
    """
    return(dump(iter_messages(bufr_files, **{'msg': msg, 'subset': subset}),
                bufr_out))


def to_csv(keys, gen_fun, bufr_out='-', decode_code_table=False):
    """Save values of keys to a csv file

    You must define keys, so each key will be saved as column into the csv.

    :param keys: Keys to save to csv
    :param gen_fun: A function generates BufrHandle object(s)
    :param bufr_out: Output file name (default is stdout)
    :param decode_code_table: If True, CODE TABLE values are saved
    :returns: None"""
    n = 0
    with _open_(bufr_out, 'w') as f:
        writer = _csv.writer(f, delimiter=';')
        writer.writerow(keys)
        for s in gen_fun:
            r = [get_val(s, k) for k in keys]
            if decode_code_table:
                mtvn = get_val(s, 'masterTablesVersionNumber')
                attrib = get_attributes(s, keys)
                for i, k in enumerate(keys):
                    if attrib[k]['units'] == 'CODE TABLE':
                        r[i] = _get_value_from_code_table(
                            r[i], attrib[k]['code'], mtvn)
            writer.writerow(r)
            n += 1
    return(n)


def synop_to(bufr_files, bufr_out='-', decode_code_table=False, fmt='bufr',
             **filters):

    def iter():
        for s in iter_subsets(iter_synop(bufr_files, **filters)):
            if get_val(s, 'latitude') is not None:
                yield(s)

    n = 0
    if fmt == 'bufr':
        n = dump(iter_synop(bufr_files, **filters), bufr_out)
    elif fmt == 'csv':
        n = to_csv(_synop_keys_, iter(), bufr_out, decode_code_table)
    elif fmt == 'json':
        n = json(iter(), bufr_out, _synop_keys_, True, decode_code_table)
    return(n)


def synop_to_csv(bufr_files, bufr_out='-', decode_code_table=False, **filters):
    """Save SYNOP messages to a csv file"""
    def iter():
        for s in iter_subsets(iter_synop(bufr_files, **filters)):
            if get_val(s, 'latitude') is not None:
                yield(s)
    return(to_csv(_synop_keys_, iter(), bufr_out, decode_code_table))


def synop_to_json(bufr_files, bufr_out='-',
                  decode_code_table=False, **filters):

    def iter():
        for s in iter_subsets(iter_synop(bufr_files, **filters)):
            if get_val(s, 'latitude') is not None:
                yield(s)

    return(json(iter(), bufr_out, _synop_keys_))

