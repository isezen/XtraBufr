#!/bin/bash - 
#===============================================================================
#
#          FILE: bsplit.sh
# 
#         USAGE: ./bsplit.sh 
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

eval LAST=\$$#
file=$LAST
length=$(($#-1))
array=${@:1:$length}

# echo "${@: -1}"

query=""
unpack=0
for i in "${@:1:$(($#-1))}"; do
	if [[ !  -z  $query  ]]; then
		query="$query"_
	fi
	if [ $i == "bn" ]; then
		query=$query"bn[blockNumber:i]"
	fi
	if [ $i == "hc" ]; then
		query=$query"hc[bufrHeaderCentre:i]"
	fi
	if [ $i == "sn" ]; then
		query=$query"sn[stationNumber:i]"
	fi
	if [ $i == "tt" ]; then
		query=$query"tt[typicalTime]"
	fi
	if [ $i == "dc" ]; then
		query=$query"dc[dataCategory]"
	fi
	if [ $i == "ds" ]; then
		query=$query"ds[dataSubCategory]"
	fi
done

if [[ $query = *"blockNumber"* ]]; then
	unpack=1
fi

if [[ $query = *"stationNumber"* ]]; then
	unpack=1
fi


echo "Split by : $query"
# echo $unpack

if [ $unpack -eq 0 ]; then
	bufr_copy "$file" "mss_$query.bufr[editionNumber]"
else
bufr_filter -f - $file 2>/dev/null <<EOF
set unpack=1;
write "mss_$query.bufr[editionNumber]";
EOF
fi
