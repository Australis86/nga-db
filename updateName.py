#! /usr/bin/python3

"""This script is a sample snippet to demonstrate updating a botanical name (i.e. replacing an invalid taxon with the genus)."""

__version__ = "1.0"
__author__ = "Joshua White"
__copyright__ = "Copyright 2020"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

import nga # Custom module for NGA and other resources

# Old and new botanical names
OLD_BOTANICAL_NAME = 'Geranium x oxonianum'
NEW_BOTANICAL_NAME = 'Geranium'

ngadb = nga.NGA.NGA()
results = ngadb.search(OLD_BOTANICAL_NAME)
if OLD_BOTANICAL_NAME in results:
	results = results[OLD_BOTANICAL_NAME]
	for plant in results:
		entry = results[plant]
		entry['new_bot_name'] = NEW_BOTANICAL_NAME
		entry['rename'] = True
		ngadb.proposeNameChange(entry)
