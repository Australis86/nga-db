#! /usr/bin/python

"""This script is a sample snippet to demonstrate updating a botanical name (i.e. replacing an invalid taxon with the genus)."""

__version__ = "1.0"
__author__ = "Joshua White"
__copyright__ = "Copyright 2020"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

import nga # Custom module for NGA and other resources

# Old and new botanical names
old_botanical_name = 'Geranium x oxonianum'
new_botanical_name = 'Geranium'

ngadb = nga.NGA.NGA()
results = ngadb.search(old_botanical_name)
if old_botanical_name in results:
	results = results[old_botanical_name]
	for plant in results:
		entry = results[plant]
		entry['new_bot_name'] = new_botanical_name
		entry['rename'] = True
		ngadb.proposeNameChange(entry)
