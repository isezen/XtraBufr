"""
xtrabufr.__command_tools__
~~~~~~~~~~~~~~~~~~
Methods for command line tools
"""
from __future__ import print_function
import os as _os
import sys as _sys
import argparse as _argparse
import traceback as _traceback
from sys import stderr as _stderr
from argparse import RawTextHelpFormatter as _rtformatter

from . import (__version__, __name__, __author__, __license__, __year__)
from .objects import Descriptors
from ._extra_ import copy_msg_from_file
from ._extra_ import read_msg
from ._extra_ import bufr_filter_dump
from ._helper_ import print_msg

# See: https://stackoverflow.com/questions/20165843/argparse-how-to-handle-variable-number-of-arguments-nargs?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa

def _eprint_(*args, **kwargs):
    print('ERROR: ', *args, file=_stderr, **kwargs)


def _create_argparser_(description, epilog):
    file_py = _os.path.basename(_sys.argv[0])
    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=_rtformatter)
    p.add_argument('-v', '--version', help="Version", action="version",
                   version='{} {}\n{} (c) {} {}'.format(__name__, __version__,
                                                        __license__, __year__,
                                                        __author__))
    return(p)


def _xbfilter_():
    # _sys.argv.append('out.bufr')
    description = 'Filter messages of a BUFR file by header keys.\n' + \
                  'Optional arguments can be used to filter output.\n\n' + \
                  ' N       : An integer Numeric value\n' + \
                  ' YYYMMDD : Year, Month and day (adjacent)\n' + \
                  ' HHMMSS  : Hour, minute and second (adjacent)'
    epilog = 'Example of use:\n' + \
             ' %(prog)s out.bufr in.bufr\n' + \
             ' %(prog)s out.bufr in1.bufr in2.bufr in3.bufr\n' + \
             ' %(prog)s out.bufr *.bufr\n' + \
             ' %(prog)s out.bufr in.bufr -hc 91 -dc 0 -y 2018\n' + \
             ' %(prog)s out.bufr in*.bufr -hc 91 -dc 0 -td 20180324\n'
    p = _create_argparser_(description, epilog)
    for a in [['-ed', '--edition', int, 'N', 'Edition'],
              ['-dc', '--dataCategory', int, 'N', 'Data Category'],
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
              ['-ts', '--typicalSecond', int, 'N', 'Typical Second'],
              ['-ud', '--unexpandedDescriptors', int, 'N',
               'Unexpanded Descriptors']]:
        p.add_argument(a[0], a[1], type=a[2], nargs='+', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('-p', '--plain', help="Plain dump",
                   action="store_true")
    p.add_argument('bufr_out', type=str, nargs='?',
                   help='Output BUFR file\n' +
                        'Save messages to the file')
    p.add_argument('bufr_files', type=str, nargs='+',
                   help='BUFR files to process\n' +
                        '(at least a single file required)')
    args = p.parse_args()
    bufr_files = args.bufr_files
    bufr_out = args.bufr_out
    del args.bufr_files, args.bufr_out
    try:
        n = bufr_filter_dump(bufr_files, bufr_out, **args.__dict__)
        print(n, 'messages filtered out.')
        return(0)
    except KeyboardInterrupt:
        print("Process stopped")
    except Exception:
        _traceback.print_exc(file=_stderr)
    return(1)


def _xbprint_():
    description = 'Print content of a BUFR file.\n' + \
                  'Optional arguments can be used to filter output.\n'
    epilog = 'Example of use:\n' + \
             ' %(prog)s in.bufr\n' + \
             ' %(prog)s input.bufr -i -m 12 -s 17\n'

    p = _create_argparser_(description, epilog)

    for a in [['-s', '--subset', int, 'N', 'Subset Id'],
              ['-m', '--msg', int, 'N', 'Message Id (Mandatory)']]:
        p.add_argument(a[0], a[1], type=a[2], nargs='*', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('-i', '--ignore', help="Ignore Missing/None values",
                   action="store_true")
    p.add_argument('bufr_file', type=str, nargs='?',
                   help='BUFR file to process')

    args = p.parse_args()

    try:
        for i, m in read_msg(args.bufr_file, args.msg, args.subset):
            print_msg({i: m}, args.bufr_file, args.ignore)
        return(0)
    except KeyboardInterrupt:
        print("Process stopped")
    except Exception:
        _traceback.print_exc(file=_stderr)
    return(1)


def _xbcopy_():
    """Copy a specified message from a BUFR file

    Copy a specified message from a BUFR file by msg number or subset number.
    """
    description = 'Copy a message from a BUFR file\n' + \
                  'Also you can copy/extract a subset.\n\n'
    epilog = 'Example of use:\n' + \
             ' %(prog)s -m 5 in.bufr out.bufr\n' + \
             ' %(prog)s -m 10 -s 5 in.bufr out.bufr\n'

    p = _create_argparser_(description, epilog)

    for a in [['-m', '--msg', int, 'N', 'Message Id (Mandatory)'],
              ['-s', '--subset', int, 'N', 'Subset Id']]:
        p.add_argument(a[0], a[1], type=a[2], nargs='?', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('bufr_in', type=str, help='BUFR file to process')
    p.add_argument('bufr_out', type=str, help='Output BUFR file')
    args = p.parse_args()

    if args.msg is None:
        print('msg_id is required')
        return(1)

    try:
        copy_msg_from_file(args.bufr_in, args.bufr_out,
                           args.msg, args.subset)
        return(0)
    except KeyboardInterrupt:
        print("Process stopped")
    except Exception:
        _traceback.print_exc(file=_stderr)
    return(1)


def _xbdef_():
    description = 'BUFR Definition operations\n'
    epilog = 'Example of use:\n' + \
             ' %(prog)s 307080\n' + \
             ' %(prog)s 307079 4025 11042\n' + \
             ' %(prog)s -m 14 307096\n' + \
             ' %(prog)s -m 22 301004 302031 20010'

    p = _create_argparser_(description, epilog)

    p.add_argument('-m', '--mastertable', type=str, nargs='?', metavar='N',
                   default='latest', help='Master Table Version Number')
    p.add_argument('-d', '--description', help="Show description",
                   action="store_true")
    p.add_argument('lookup', type=int, nargs='*', metavar='FFXXYYY',
                   help='Descriptor value(s)')
    args = p.parse_args()

    try:
        print(Descriptors(args.lookup).__str__(show_desc=args.description))
        return(0)
    except KeyboardInterrupt:
        print("Process stopped")
    except Exception:
        _traceback.print_exc(file=_stderr)
    return(1)
