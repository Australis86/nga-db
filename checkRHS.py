#! /usr/bin/python3

"""This script is a sample snippet to demonstrate checking the RHS to see if a cross has been registered. It uses a CSV file (crosses.csv) as an input with (Pod Parent, Pollen Parent)."""

__version__ = "1.0"
__author__ = "Joshua White"
__copyright__ = "Copyright 2024"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

import os
import sys
import re
import pandas as pd
import nga # Custom module for NGA and other resources

def genusName(abbrev):
	if abbrev == 'Cym.':
		return 'Cymbidium'
	else:
		return abbrev


def extractName(rhs_obj, plant):
	'''Parse a plant name to extract genus and grex.'''
	
	reg = True
	
	# Extract genus
	genus = ''
	matches = re.search('^\w{1,5}\.', plant, flags=re.IGNORECASE)
	if matches is not None:
		genus = matches.group()
		plant = plant[len(genus):].strip()

	# Remove any ploidy references
	matches = re.search('\(.*\dn\)', plant, flags=re.IGNORECASE)
	if matches is not None:
		ploidy = matches.group()
		plant = plant.replace(ploidy, '').replace('  ',' ').strip()

	# Remove numbered selections
	matches = re.search(' #\d+?', plant, flags=re.IGNORECASE)
	if matches is not None:
		selection = matches.group()
		plant = plant.replace(selection, '').replace('  ',' ').strip()

	# Remove named selections
	matches = re.search(' \'.+\'$', plant, flags=re.IGNORECASE)
	if matches is not None:
		selection = matches.group()
		plant = plant.replace(selection, '').replace('  ',' ').strip()

	# Remove forms
	matches = re.search(' f. \w+', plant, flags=re.IGNORECASE)
	if matches is not None:
		selection = matches.group()
		plant = plant.replace(selection, '').replace('  ',' ').strip()

	# Check if this plant is also an unnamed cross
	matches = re.findall(' x ', plant, flags=re.IGNORECASE)
	if matches is not None and len(matches) > 0:
		if len(matches) > 1:
			print("WARNING: Hybrid parent detected")
			reg = False

		else:
			parents = plant.strip('(').strip(')').split(matches[0])
			results = checkRegistration(rhs_obj, genus + ' ' + parents[0], genus + ' ' + parents[1])
			reg = results['matched']
			if reg:
				plant = reg['epithet']
				genus = reg['genus']

	return {'registered':reg, 'genus':genus, 'grex':plant}


def checkRegistration(rhs_obj, pod_parent, pollen_parent):
	'''TBA'''

	pod = extractName(rhs_obj, pod_parent)
	pol = extractName(rhs_obj, pollen_parent)

	if pod['registered'] and pol['registered']:
		pod_genus = genusName(pod['genus'])
		pollen_genus = genusName(pol['genus'])
		req = my_rhs.searchParentage(pod_genus,pod['grex'],pollen_genus,pol['grex'],True)
	else:
		req = {'matched':False}

	req['parents'] = {'pod':pod,'pollen':pol}
	return req


if __name__ == '__main__':

	# TODO: Make this use argparse
	fname = 'crosses.csv'

	# Check if the source file exists
	if not os.path.isfile(fname):
		print("%s not found" % fname)
		sys.exit(1)

	# Read the source CSV
	df = pd.read_csv(fname)
	df.drop_duplicates(inplace=True)

	# Connect to the RHS
	my_rhs = nga.RHS.Register()
	my_rhs.dbConnect('RHS.db')

	# Iterate through the source CSV
	n = 0
	for idx, row in df.iterrows():
		results = checkRegistration(my_rhs, row['Pod Parent'], row['Pollen Parent'])
		if results is not None:
			if results['matched']:
				if results['parents_reversed']:
					print(results['genus'],results['epithet'],'=', results['parents']['pollen']['grex'], 'X', results['parents']['pod']['grex'])
				else:
					print(results['genus'],results['epithet'],'=', results['parents']['pod']['grex'], 'X', results['parents']['pollen']['grex'])
			else:
				print('N/R', results['parents']['pod']['grex'], 'X', results['parents']['pollen']['grex'])
		else:
			print("Error")

	my_rhs.dbClose()
	sys.exit()
