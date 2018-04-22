#!/bin/bash - 
#===============================================================================
#
#          FILE: bsort.sh
# 
#         USAGE: ./bsort.sh 
# 
#   DESCRIPTION: 
#     This script sorts out bufr files from a directory into temp directory
#   and concats them with files assicated bufr files in mesbank. bufr3 files are
#   converted to bufr4 format before sort out.
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Ismail SEZEN (isezen), isezen@mgm.gov.tr, sezenismail@gmail.com
#  ORGANIZATION: Turkish State Meteorological Service
#       CREATED: 03/17/2018 12:30:49 PM
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

# Modify this section if needed
FILE_SCRIPT=$(basename "$0")
DIR_MESBANK=$HOME/mesbank
DIR_TEMP=/tmp/mss
DIR_MSS=/home/mss/ftp/files/bufr
DIR_LOG=$HOME/.metcap/log
FILE_LOG=$DIR_LOG/bsort.log
OUT_PATTERN=$DIR_TEMP/mss_[dataCategory]_[internationalDataSubCategory]_[typicalDate]_[typicalHour].bufr[editionNumber]

# -----------------------------
# DO NOT MODIFY AFTER THIS LINE
# -----------------------------

function timestamp { echo $(date +"%y-%m-%d %T"); }

function log_msg { echo "[$(timestamp)] "[$FILE_SCRIPT]" ($$): $1" >> $FILE_LOG; }

function bufed3to4 {
	local x="$1[@]"
	for i in "${!x}"; do
		if [[ $i == *.bufr3 ]]; then			
			fname=${i##*/}
			bufr_set -s edition=4 $i $i
			log_msg "WARNING : $fname converted to BUFR edition 4"
		fi
	done
}

function singleton {
	pids=$(pidof -x $FILE_SCRIPT)
	for pid in $pids; do
	if [ $pid != $$ ]; then
		log_msg "Process is already running with PID ($pid)"
		exit 1
	fi
	done
}

function sort {
	# Create required directories
	# this might need proper credentials 
	mkdir -p $DIR_MESBANK
	mkdir -p $DIR_TEMP
	mkdir -p $DIR_LOG

	while :; do
		mapfile -t files < <(find $DIR_MSS -name 'mw_mss*' -maxdepth 1 -type f)
		len=${#files[@]}

		if [ $len -eq 0 ]; then break; fi

		if [ $len -gt 30000 ]; then
			files=("${files[@]:0:30000}")
			len=${#files[@]}
		fi

		bufr_copy -f "${files[@]}" $OUT_PATTERN >/dev/null # >> $FILE_LOG
		rm "${files[@]}"

		mapfile -t tmp_files < <(find $DIR_TEMP -maxdepth 1 -type f)

		bufed3to4 tmp_files  # convert bufr3 file to bufr4

		for i in "${tmp_files[@]}"; do
			fname=${i##*/}
			fname2=$fname
			fn=${fname%.*}
			typicaldate=$(cut -d'_' -f4 <<<"$fn")
			year=${typicaldate:0:4}
			month=${typicaldate:4:2}
			day=${typicaldate:6:2}
			path_mesbank="$DIR_MESBANK/$year/$month/$day"
			mkdir -p $path_mesbank

			fname="$path_mesbank/$fn.bufr4"
			len1=$(codes_count "$i")
			if [ -f "$fname" ]; then
				len2=$(codes_count "$fname")
				cat $i >> "$fname"
				len3=$(codes_count "$fname")
				tot=$((len1 + len2))
				if [ $len3 -eq $tot ]; then
					log_msg "$fname2 ($len1 msg)"
				else
					log_msg "$fname2 {WARNING : $len1 + $len2 = $len3 != $tot}"
				fi
			else
				mv $i "$fname"
			fi
		done
		rm -f $DIR_TEMP/*
	done
}

diff=$( TIMEFORMAT="%R"; { time sort; } 2>&1 )

log_msg "-------{$diff sec}-------"

# keep last 1000 lines in log file
echo "$(tail -1000 $FILE_LOG)" > $FILE_LOG

