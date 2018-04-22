#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#

""" xbcopy.py : Copy a message from a BUFR file

Also you can copy/extract a subset.

       USAGE: ./xbcopy.py --help
      AUTHOR: Ismail SEZEN (isezen)
       EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
ORGANIZATION: Turkish State Meteorological Service
     CREATED: 03/27/2018 06:00:00 AM

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

# =============================================================================
#         FILE: xbcopy.py
#        USAGE: ./xbcopy.py [msg_id] [bufr_in] [bufr_out]
#  DESCRIPTION: Copy a single message from a bufr file
#       AUTHOR: Ismail SEZEN (isezen)
#        EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
# ORGANIZATION: Turkish State Meteorological Service
#      CREATED: 03/27/2018 03:30:00 AM
# =============================================================================


from __future__ import print_function
from sys import stderr as _stderr
from sys import exit as _exit
import os as _os
import argparse as _argparse
from argparse import RawTextHelpFormatter
import eccodes as _ec


__author__ = 'ismail sezen'
__contact__ = 'sezenismail@gmail.com'
__copyright__ = "Copyright (C) 2018, Ismail SEZEN"
__credits__ = []
__license__ = "AGPL 3.0"
__version__ = "0.0.1"
__status__ = "Production"


def eprint(*args, **kwargs):
    print(*args, file=_stderr, **kwargs)


def msg_count(bufr_file):
    """Return number of messages in a BUFR file

    :param bufr_file: Name of BUFR file
    :returns: Number of messages in a BUFR file
    """
    ret = None
    with open(bufr_file, 'rb') as f:
        ret = _ec.codes_count_in_file(f)
    return(ret)


def extract_subset(bufr, subset_number):
    """Extract a subset from a BUFR handle

    :param bufr: Handle to BUFR file
    :param subset_number: Number of subset
    :returns: Handle to BUFR message contains subset
    """
    _ec.codes_set(bufr, 'extractSubset', subset_number)
    _ec.codes_set(bufr, 'doExtractSubsets', 1)
    bufr2 = _ec.codes_clone(bufr)
    _ec.codes_set(bufr2, 'unpack', 1)
    return(bufr2)


def get_msg_by_id(bufr_file, msg_id=1, subset_id=None):
    """Get message by Id

    :param bufr_file: Name of BUFR file
    :param msg_id: Id Number of message
    :param subset_id: Id number of subset
    :returns: Handle to BUFR message
    """
    cnt = 0
    ret = None
    with open(bufr_file, 'rb') as f:
        while True:
            cnt += 1
            bufr = _ec.codes_bufr_new_from_file(f)
            if cnt == msg_id:
                if subset_id is None:
                    ret = bufr
                else:
                    _ec.codes_set(bufr, 'unpack', 1)
                    ret = extract_subset(bufr, subset_id)
                break
            _ec.codes_release(bufr)
    return(ret)


def extract_msg_by_id(bufr_in, bufr_out, msg_id=1, subset_id=None):
    """Extract message from BUFR file

    :param bufr_in: BUFR file name to read
    :param bufr_out: BUFR file name to save
    :param msg_id:Id numbber if message
    :param subset_id: Id number of subset in message
    :returns: None
    """
    bufr = get_msg_by_id(bufr_in, msg_id, subset_id)
    if bufr is not None:
        with open(bufr_out, 'wb') as f:
            _ec.codes_write(bufr, f)
        _ec.codes_release(bufr)
    else:
        n = msg_count(bufr_in)
        print('msg_id must be between 1-{}'.format(n))


def main():
    file_py = _os.path.basename(__file__)
    description = 'Copy a message from a BUFR file\n' + \
                  'Also you can copy/extract a subset.\n\n'
    epilog = 'Example of use:\n' + \
             ' {0} 5 input.bufr out.bufr\n' + \
             ' {0} -s 10 5 input.bufr out.bufr\n'

    args = [['-s', '--subset_id', int, 'N', 'Subset Id'],
            ['-m', '--msg_id', int, 'N', 'Message Id (Mandatory)']]

    p = _argparse.ArgumentParser(description=description,
                                 epilog=epilog.format(file_py),
                                 formatter_class=RawTextHelpFormatter)

    for a in args:
        p.add_argument(a[0], a[1], type=a[2], nargs='?', metavar=a[3],
                       default=None, help=a[4])
    p.add_argument('bufr_in', type=str, help='BUFR file to process')
    p.add_argument('bufr_out', type=str, help='Output BUFR file')
    args = p.parse_args()

    if args.msg_id is None:
        print('msg_id is required')
        return(1)

    try:
        extract_msg_by_id(args.bufr_in, args.bufr_out,
                          args.msg_id, args.subset_id)
    except _ec.CodesInternalError as err:
        eprint(err.msg)
        return(1)

    return(0)


if __name__ == "__main__":
    _exit(main())
