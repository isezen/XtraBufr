#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# =============================================================================
#         FILE: bcopy_msg.py
#        USAGE: ./bcopy_msg.py [msg_id] [bufr_in] [bufr_out]
#  DESCRIPTION: Copy a single message from a bufr file
#       AUTHOR: Ismail SEZEN (isezen)
#        EMAIL: isezen@mgm.gov.tr, sezenismail@gmail.com
# ORGANIZATION: Turkish State Meteorological Service
#      CREATED: 03/17/2018 12:30:49 PM
# =============================================================================

from __future__ import print_function
import sys
from eccodes import *

__author__ = 'ismail sezen'


def msg_count(bufr_file):
    ret = None
    with open(bufr_file, 'rb') as f:
        ret = codes_count_in_file(f)
    return(ret)


def get_msg_by_id(bufr_file, msg_id=1):
    cnt = 0
    ret = None
    with open(bufr_file, 'rb') as f:
        while True:
            cnt += 1
            bufr = codes_bufr_new_from_file(f)
            if cnt == msg_id:
                ret = bufr
                break
            codes_release(bufr)
    return(ret)


def extract_msg_by_id(bufr_in, bufr_out, msg_id=1):
    bufr = get_msg_by_id(bufr_in, msg_id)
    if bufr is not None:
        with open(bufr_out, 'wb') as f:
            codes_write(bufr, f)
        codes_release(bufr)
    else:
        n = msg_count(bufr_in)
        print('msg_id must be between 1-{}'.format(n))


def main():
    if len(sys.argv) < 3:
        print('Usage: ', sys.argv[0], '[msg_id] [bufr_in] [bufr_out]',
              file=sys.stderr)
        sys.exit(1)

    msg_id = sys.argv[1]
    bufr_in = sys.argv[2]
    bufr_out = sys.argv[3]

    try:
        extract_msg_by_id(bufr_in, bufr_out, int(msg_id))
    except CodesInternalError as err:
        sys.stderr.write(err.msg + '\n')
        return(1)

    return(0)


if __name__ == "__main__":
    sys.exit(main())
