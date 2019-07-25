#! /usr/bin/python

"""Module for fetching search results from the Catalogue of Life.

This script is designed for Python 3 and Beautiful Soup 4 with the lxml parser."""

__version__ = "1.1"
__author__ = "Joshua White"
__copyright__ = "Copyright 2019"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"


# Module imports
import os
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class COL:
	
	def __init__(self):
		"""Create an instance and set up a requests session to the KEW WCSP."""
		
		self._search_url = 'http://www.catalogueoflife.org/col/search/all'
		self._session = requests.Session()
		self._session.get(self._search_url)
	
	
	def search(self, search_term, fetchSynonyms=False):
		"""Search the COL for a particular entry and returned the accepted name or synonyms."""
		
		def checkRow(result_cells, checkname):
			"""Method to check the contents of a row."""
			
			result = {'accepted': False, 'name': None }
			
			# The COL search results only have one italic tag; 
			# the "var" or other notation is actually a non-italic span.
			italics = result_cells[0].find('i')
			if italics is not None:
				result_name = italics.text.replace("  ", " ")
				
				if result_name == checkname:
					status = result_cells[2].text
					accepted = 'accepted' in status
					result['accepted'] = accepted
					
					if accepted:
						result['name'] = result_name
					else:
						accepted_name = result_cells[2].find('i')
						if accepted_name is not None:
							new_name = accepted_name.text.replace('  ', ' ')
							result['name'] = new_name
							
					return result
					
			return None
		
		
		# Prepare the search parameters
		search_key = search_term.replace('nothosubsp. ','').replace('subsp. ','').replace('var. ','').replace('f. ','').replace('  ',' ')
		
		# Remove 'subsp.', 'var.' and 'f.' as these sometimes stuff up the search
		params = {
			'fossil':0, 
			'key':search_key, 
			'search':'Search', 
			'match':1 # Should give us an exact match (species and subspecies)
		}
		
		
		try:
			r = self._session.get(self._search_url, params=params)
		except requests.exceptions.RequestException as e:
			print(str(e))
			return None
		else:
			soup = BeautifulSoup(r.text, "lxml")
			table = soup.find('table')
			tr = soup.findAll('tr')
			last_match = None
			
			for row in tr:
				cells = row.findAll('td')
				if len(cells) > 0:
					
					# Ensure this isn't an illegal name or a mismatch
					cell_text = cells[0].text
					if 'illeg.' not in cell_text and search_term in cell_text:
						if fetchSynonyms:
							link = cells[0].find('a')
							status = cells[2].text
							
							# Only query further if the entry is an accepted name
							if link is not None and 'accepted' in status:
								rel_url = link['href']
								abs_url = urljoin(self._search_url, rel_url)
								
								try:
									r = self._session.get(abs_url)
								except requests.exceptions.RequestException as e:
									print(str(e))
									return None
								else:
									soup = BeautifulSoup(r.text, "lxml")
									th = soup.find('th', string=re.compile('Synonym')) # This looks for the row with the 'Synonym' header in it
									
									# If there are synonyms, then an adjacent cell will have a table of them
									if th is not None:
										syn_row = th.parent
										table = syn_row.find('table')
										
										# Not every plant has synonyms
										if table is not None:
											rows = table.findAll('tr')
											synonyms = []
											
											# Each row will be a different synonym
											for row in rows: 
												cells = row.findAll('td') # First cell contains the synonym name
												cell_contents = cells[0].contents
												cell_text = cells[0].text
												
												# Do not consider illegal synonyms
												if 'illeg.' not in cell_text:
													synonym_tags = cell_contents[:-1] # Exclude the last element, as this is the discoverer's name
													synonym = ''.join([getattr(x, 'text', x) for x in synonym_tags]) # Convert to plain text
													synonyms.append(synonym)
												
											return synonyms
									
									return None
						
						else:
							# Only continue if the name is exact
							row_data = checkRow(cells, search_term)
							if row_data is not None:
								if row_data['accepted']:
									return row_data['name']
								else:
									last_match = row_data
			
			if last_match is not None:
				return last_match['name']
			
		return None
	
	
	
def testModule(search_term='Cymbidium iansonii'):
	"""A simple test to check that all functions are working correctly."""
	
	myCOL = COL()
	print(myCOL.search(search_term))
	print(myCOL.search(search_term, True))