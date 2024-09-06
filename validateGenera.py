#! /usr/bin/python3

"""This script is designed to validate the hybrid genera list in the spreadsheet at:
https://docs.google.com/spreadsheets/d/1IU4wC-F2dElVpOTrA458KUy3uymNQb5HLGjt8Y_rmhM/pub?output=xlsx
https://docs.google.com/spreadsheets/d/1IU4wC-F2dElVpOTrA458KUy3uymNQb5HLGjt8Y_rmhM/pubhtml"""

__version__ = "1.0"
__author__ = "Joshua White"
__copyright__ = "Copyright 2024"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

import os
import sys
import re
import argparse
import pandas as pd
import numpy as np
import nga # Custom module for NGA and other resources


def initParser():
	'''Set up CLI.'''

	parser = argparse.ArgumentParser()
	parser.add_argument("filename", type=str, help="XLSX workbook containing the Genera spreadsheet")

	cli_args = parser.parse_args()
	return cli_args


if __name__ == '__main__':
	args = initParser()

	# Check if the source file exists
	if not os.path.isfile(args.filename):
		print(f'{args.filename} not found')
		sys.exit(1)

	# Read the source XLSX
	df = pd.read_excel(args.filename, sheet_name='Genera')

	print(df)