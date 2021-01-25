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
import getpass
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class COL:

	def __init__(self):
		"""Create an instance and set up a requests session to the COL API."""
		
		self._search_url = 'https://api.catalogueoflife.org/dataset/3LR/nameusage/search'
		self._synonym_url = 'https://api.catalogueoflife.org/dataset/%s/taxon/%s/synonyms'
		self._session = requests.Session()
		self.__cache = None
		self.__cache_age = datetime.now() - timedelta(days=5) # Default value

		# Set the path to the GBIF auth file
		if 'GBIF_PATH' in globals():
			self._authpath = GBIF_PATH
		else:
			self._authpath = os.path.join(os.path.expanduser('~'), '.gbif')

		# This will look for the GBIF credentials
		self.__loadAuthFile(self._authpath)


	def __loadAuthFile(self, auth_file):
		"""Load a file containing the user's GBIF account details."""
	
		if not os.path.exists(auth_file):
			self.__createAuthFile(auth_file)

		gbif = open(auth_file, 'r')
		gbif_auth = gbif.read()
		gbif.close()
	
		gbif_account = gbif_auth.split(':')
		if len(gbif_account) > 1:
			username = gbif_account[0]
			password = gbif_account[1]
		
		self.__auth = HTTPBasicAuth(username, password) # GBIF account
	
	
	def __createAuthFile(self, auth_file):
		"""Store a set of authentication parameters."""
		
		# Ask the user for their credentials
		user = input("Username: ")
		pwd = getpass.getpass()

		# Test the credentials
		r = requests.get("https://api.catalogueoflife.org/user/me", auth=HTTPBasicAuth(user, pwd), headers={'accept': 'application/json'})
		if r.status_code != 200:
			raise PermissionError("Failed to authenticate with the COL API.")
		else:
			print("Successfully tested authentication.")
		
		# Store the credentials
		gbif = open(auth_file, 'w')
		os.chmod(auth_file, 0o0600) # Try to ensure only the user can read it
		gbif.write('%s:%s' % (user,pwd))
		gbif.close()
	
	
	def search(self, search_term, fetchSynonyms=False):
		"""Search the COL for a particular entry and returned the accepted name or synonyms."""
	
		# Query parameters
		params = {'q':search_term, 'content': 'SCIENTIFIC_NAME', 'maxRank':'SPECIES', 'type': 'EXACT', 'offset':0, 'limit':10}
		
		# First step is to get the taxon ID, then fetch the synonyms if required
		try:
			r = self._session.get(self._search_url, params=params, headers={'accept': 'application/json'})
		except requests.exceptions.RequestException as e:
			return (None, 'Unable to retrieve taxon.')
		else:
			rdata = r.json()
			if rdata['empty']:
				# No match found
				return None
			
			# Closest match is the first entry
			closest = rdata['result'][0]
			
			if fetchSynonyms:
				# Get the taxon ID so that we can get the synonyms
				taxonID = closest['id']
				datasetKey = closest['usage']['datasetKey']
				
				# Post to the asynchronous API (this requests a build of an export)
				try:
					r = self._session.get(self._synonym_url % (datasetKey, taxonID), auth=self.__auth, headers={"Content-Type": "application/json"})
				except requests.exceptions.RequestException as e:
					return (None, 'Unable to retrieve synonyms.')
				else:
					synonyms = []
					rdata = r.json()
					
					# Check if there are any synonyms
					if not rdata:
						return None
					
					# Iterate through the types of synonyms and collect the botanical names
					for synonym_type in rdata:
						for synonym in rdata[synonym_type]:
							synonyms.append(synonym[0]['scientificName'])
					
					synonyms.sort()
					return synonyms
			
			else:
				# If we don't need the synonyms, then everything we need is in this result dataset
				usage = closest['usage']
				status = usage['status'].lower()
				if 'accepted' in status:
					acceptedname = usage['name']
				elif 'synonym' in status:
					acceptedname = usage['accepted']['name']
				else:
					return None

				return acceptedname['scientificName']


def testModule(search_term='Cymbidium iansonii'):
	"""A simple test to check that all functions are working correctly."""
	
	myCOL = COL()
	print(myCOL.search(search_term))
	print(myCOL.search(search_term, True))
