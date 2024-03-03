#!/usr/bin/env bash

# Iterate over a text file containing a list of genera
fname=$1
logfile="$0.log"

# Verify the type of input and number of values
# Display an error message if no filename is provided
[ $# -eq 0 ] && { echo "Usage: $0 filename"; exit 1; }

echo "---- AUTOMATED GENERA CHECK LOG FILE ----" > ${logfile}
while read line; do
	results=`./genusCheck.py -v -o --parentage "$line" 2>&1`
	ecode=$?

	if [ $ecode -eq 65 ]; then 
		echo "REVISED:    $line" | tee -a ${logfile}
		echo "$results" >> $logfile
		echo -e "\n\n" >> $logfile
	elif [ $ecode -eq 66 ]; then
		echo "DEPRECATED: $line" | tee -a ${logfile}
	elif [ $ecode -ne 0 ]; then
		echo "ERROR:      $line" | tee -a ${logfile}
	else
		echo "UNCHANGED:  $line" | tee -a ${logfile}
	fi
done < $1
