#!/usr/bin/env bash

# This script iterates through a list of genera in list.txt

while read line
do 
	echo ""
	echo "Processing $line"
	python3 genusCheck.py -o -g "$line"
done < list.txt
