"""
xtrabufr.__command_tools__
~~~~~~~~~~~~~~~~~~
Methods for command line tools
"""
from __future__ import print_function
import os as _os
import sys as _sys
import time as _time
import argparse as _argparse
from sys import stderr as _stderr
from argparse import RawTextHelpFormatter as _rtformatter

from . import (__version__, __name__)
from .objects import Descriptors
from ._extra_ import copy_msg_from_file


def eprint(*args, **kwargs):
    print('ERROR: ', *args, file=_stderr, **kwargs)


def _xbcopy_():
    """Copy a specified message from a BUFR file

    Copy a specified message from a BUFR file by msg number or subset number.
    """
    file_py = _os.path.basename(_sys.argv[0])
    description = 'Copy a message from a BUFR file\n' + \
                  'Also you can copy/extract a subset.\n\n'
    epilog = 'Example of use:\n' + \
             ' {0} 5 input.bufr out.bufr\n' + \
             ' {0} -s 10 5 input.bufr out.bufr\n'

    args = [['-s', '--subset', int, 'N', 'Subset Id'],
            ['-m', '--msg', int, 'N', 'Message Id (Mandatory)']]

    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=_rtformatter)

    for a in args:
        p.add_argument(a[0], a[1], type=a[2], nargs='?', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('bufr_in', type=str, help='BUFR file to process')
    p.add_argument('bufr_out', type=str, help='Output BUFR file')
    args = p.parse_args()

    if args.version:
        print(__name__, __version__)

    if args.msg is None:
        print('msg_id is required')
        return(1)

    try:
        copy_msg_from_file(args.bufr_in, args.bufr_out,
                           args.msg_id, args.subset_id)
    except Exception as e:
        eprint(e)
        return(1)
    return(0)


def _xbdef_():
    file_py = _os.path.basename(_sys.argv[0])
    description = 'BUFR Definition operations\n'
    epilog = 'Example of use:\n' + \
             ' {0} 307080\n' + \
             ' {0} 307079 4025 11042\n' + \
             ' {0} -m 14 307096\n' + \
             ' {0} -m 22 301004 302031 20010'

    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=_rtformatter)

    p.add_argument('-v', '--version', help="Version",
                   action="store_true")
    p.add_argument('-m', '--mastertable', type=str, nargs='?', metavar='N',
                   default='latest', help='Master Table Version Number')
    p.add_argument('-d', '--description', help="Show description",
                   action="store_true")
    p.add_argument('lookup', type=int, nargs='*', metavar='FFXXYYY',
                   help='Descriptor value(s)')
    args = p.parse_args()

    if args.version:
        print(__name__, __version__)

    try:
        t = _time.clock()
        d = Descriptors(args.lookup)
        elapsed_time = _time.clock() - t
        print(d.__str__(show_desc=args.description))
        print('Elapsed: {:0.2f} sec.'.format(elapsed_time))
        return(0)
    except Exception as e:
        eprint(e)
        return(1)
    return(0)
