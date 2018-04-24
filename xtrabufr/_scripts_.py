"""
xtrabufr.__command_tools__
~~~~~~~~~~~~~~~~~~
Methods for command line tools
"""
from __future__ import print_function
import os as _os
import sys as _sys
import traceback as _traceback
# import time as _time
import argparse as _argparse
from sys import stderr as _stderr
from argparse import RawTextHelpFormatter as _rtformatter

from . import (__version__, __name__)
from .objects import Descriptors
from ._extra_ import copy_msg_from_file
from ._extra_ import read_msg
from ._helper_ import print_msg


def _eprint_(*args, **kwargs):
    print('ERROR: ', *args, file=_stderr, **kwargs)


def _create_argparser_(description, epilog):
    file_py = _os.path.basename(_sys.argv[0])
    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=_rtformatter)
    p.add_argument('-v', '--version', help="Version", action="version",
                   version='{} {}'.format(__name__, __version__))
    return(p)


def _xbprint_():
    description = 'Print content of a BUFR file.\n' + \
                  'Optional arguments can be used to filter output.\n'
    epilog = 'Example of use:\n' + \
             ' %(prog)s input.bufr\n' + \
             ' %(prog)s input1.bufr input2.bufr input3.bufr\n' + \
             ' %(prog)s input*.bufr\n' + \
             ' %(prog)s input.bufr -c 91 -dc 0 -b 17\n' + \
             ' %(prog)s input*.bufr -c 91 -dc 0 -b 17 -d 20180324\n'

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
             ' %(prog)s 5 input.bufr out.bufr\n' + \
             ' %(prog)s -s 10 5 input.bufr out.bufr\n'

    p = _create_argparser_(description, epilog)

    for a in [['-s', '--subset', int, 'N', 'Subset Id'],
              ['-m', '--msg', int, 'N', 'Message Id (Mandatory)']]:
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
