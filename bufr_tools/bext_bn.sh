#!/bin/bash - 
#===============================================================================
#
#          FILE: bext_bn.sh
# 
#         USAGE: ./bext_bn.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Ismail SEZEN (isezen), isezen@mgm.gov.tr, sezenismail@gmail.com
#  ORGANIZATION: Turkish State Meteorological Service
#       CREATED: 03/17/2018 04:49:33 PM
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

bufr_filter -f - $2 2>/dev/null <<EOF
set unpack=1;
if (blockNumber == $1) {
	write "mss_$1.bufr4";
}
EOF
