#!/bin/bash - 
#===============================================================================
#
#          FILE: cp2bin.sh
# 
#         USAGE: ./cp2bin.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Ismail SEZEN (isezen), isezen@mgm.gov.tr, sezenismail@gmail.com
#  ORGANIZATION: Turkish State Meteorological Service
#       CREATED: 03/17/2018 02:52:11 PM
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

DIR_BIN="/usr/local/bufr_tools_bin"
DIR_BUFR_TOOLS="$HOME/bufr_tools"

mkdir -p $DIR_BIN

mapfile -t files < <(find $DIR_BUFR_TOOLS -maxdepth 1 -type f \( -iname \*.py -o -iname \*.sh \))
for i in "${files[@]}"; do
	fname=${i##*/}
	fn=${fname%.*}
	cp $i $DIR_BIN/$fn
	chmod +x $DIR_BIN/$fn
done

