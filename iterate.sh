#!/usr/bin/env bash

# Iterate over a text file containing a list of genera
fname=$1
logfile="$0.log"

# Verify the type of input and number of values
# Display an error message if no filename is provided
[ $# -eq 0 ] && { echo "Usage: $0 filename"; exit 1; }

echo "---- AUTOMATED GENERA CHECK LOG FILE ----" > ${logfile}
echo "Genera requiring changes:"
while read line; do
	results=`./genusCheck.py -v -o --parentage "$line" 2>&1`
	ecode=$?

	if [ $ecode -eq 65 ]; then 
		echo "$line"
		echo "$line: CHANGES REQUIRED" >> $logfile
		echo "$results" >> $logfile
		echo -e "\n\n" >> $logfile
	else
		echo "$line" >> $logfile
	fi
done < $1
