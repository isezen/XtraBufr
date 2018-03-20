#!/bin/bash - 
#===============================================================================
#
#          FILE: blist.sh
# 
#         USAGE: ./blist.sh 
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

nss=$(bufr_get -p numberOfSubsets $1)

ssn="subsetNumber"
bn="blockNumber"
sn="stationNumber"
lat="lat"
lon="lon"

str1="unpack=1;"

ss_str=""
for i in $(seq 1 $nss); do
	ss_str=$ss_str"print \"$ssn=$i $bn=[/$ssn=$i/$bn] $sn=[/$ssn=$i/$sn] Name=[/$ssn=$i/stationOrSiteName]\";"
done

read -r -d '' rule << EOM
set unpack=1;
if (blockNumber == 17) {
	print "--------------------------------";
	print "time=[typicalTime]";
	print "nSubSets=[numberOfSubsets]";
$ss_str
	print "--------------------------------";
}
EOM

# echo -e "$rule"
# bufr_filter -f - $1 2>/dev/null <<< echo "$rule"


# bufr_filter -f - $1 2>/dev/null <<EOF

# cat <<EOF

bufr_filter -f - $1 2>/dev/null <<EOF
set unpack=1;
if (blockNumber == 17) {
	print "--------------------------------";
	print "time=[typicalTime]";
	print "nSubSets=[numberOfSubsets]";
$ss_str
	print "--------------------------------";
}
EOF
