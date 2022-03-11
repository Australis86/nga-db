#! /usr/bin/python

"""Module for searching the RHS Orchid Register and caching registration details.

Parentheses in grex names are replaced with square brackets due to parentheses causing problems in parentage fields.

This script is designed for Python 3 and Beautiful Soup 4 with the lxml parser."""

__version__ = "1.2"
__author__ = "Joshua White"
__copyright__ = "Copyright 2020"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

# Module imports
import sqlite3
import re
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


class Register:
	"""Create a user-friendly API for the RHS Orchid Register webpage and local cache database."""

	def __init__(self):
		"""Create an instance and set up a requests session
		to the RHS Orchid Register."""

		self._search_url = 'https://apps.rhs.org.uk/horticulturaldatabase/orchidregister/orchidresults.asp'
		self.__dbconn = None

		self._session = requests.Session()
		self._session.get(self._search_url)
		self._max_attempts = 5


	def dbConnect(self, dbpath):
		"""Initialise a SQLite DB for use with the cache."""

		self.__dbconn = sqlite3.connect(dbpath)
		self.__columns = ['uid','genus','epithet','synonym_genus','synonym_epithet',
			'registrant_name','originator_name','date_of_registration',
			'pod_parent_genus','pod_parent_epithet','pollen_parent_genus','pollen_parent_epithet']

		sql = '''CREATE TABLE IF NOT EXISTS registrations(
			uid INTEGER PRIMARY KEY,
			genus TEXT,
			epithet TEXT,
			synonym_genus TEXT,
			synonym_epithet TEXT,
			registrant_name TEXT,
			originator_name TEXT,
			date_of_registration TEXT,
			pod_parent_genus TEXT,
			pod_parent_epithet TEXT,
			pollen_parent_genus TEXT,
			pollen_parent_epithet TEXT);'''

		self.__dbconn.execute(sql)
		self.__dbconn.commit()

		sql = '''CREATE TABLE IF NOT EXISTS invalid(
			genus TEXT,
			grex TEXT,
			attempts INTEGER,
			PRIMARY KEY(genus, grex));'''

		self.__dbconn.execute(sql)
		self.__dbconn.commit()


	def dbClose(self):
		"""Close any hanging database connection."""

		if self.__dbconn:
			self.__dbconn.commit()
			self.__dbconn.close()


	def _getGrex(self, url):
		"""Given a RHS URL, retrieve the RHS entry from the website."""

		try:
			req = self._session.get(url)
		except requests.exceptions.RequestException as err:
			print("Unable to retrieve RHS entry.")
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(req.text, "lxml")
			tables = soup.findAll('table', {'class':'results'})
			grex = {}

			if len(tables) == 2:
				# Process the first table containing the epithet and registration info
				entry = tables[0]
				tr = entry.findAll('tr')

				# Iterate through the rows of the table
				# First column should be the field name and second is the value
				for row in tr:
					cells = row.findAll('td')
					grex[cells[0].text] = cells[1].text

				# Process the second table containing the parentage information
				parentage = tables[1]
				tbody = parentage.find('tbody')
				tr = tbody.findAll('tr')

				# Iterate through the rows of the table
				# First column is the field name, second is the pod parent info and third is the pollen parent info
				for row in tr:
					fieldname = row.find('th')
					fieldvalues = row.findAll('td')
					grex['Pod Parent %s' % fieldname.text] = fieldvalues[0].text.replace('{var}','var.').replace('{subsp}','subsp.').replace('(','[').replace(')',']')
					grex['Pollen Parent %s' % fieldname.text] = fieldvalues[1].text.replace('{var}','var.').replace('{subsp}','subsp.').replace('(','[').replace(')',']')

				# Finally, extract the RHS ID number from the URL
				matches = re.findall(r'\d+', url)
				if matches is not None and len(matches) > 0:
					grex['uid'] = int(matches[0])

				return grex

	def cacheGrex(self, url):
		"""Given a RHS URL, cache the entry in the database."""

		if self.__dbconn is not None:
			grex = self._getGrex(url)

			# If we have a response, we need to parse it and insert it into the database
			if grex is not None and 'uid' in grex:
				dataset = {}

				# Format the column names
				for key in grex:
					newkey = key.lower().replace(' ','_')
					dataset[newkey] = grex[key]

				# Make sure all necessary columns are present
				for column in self.__columns:
					if column not in dataset:
						dataset[column] = ''

				# Insert this into the database
				# Since the ID is the primary key, if there is a conflict only that row should be replaced
				sql = '''INSERT OR REPLACE INTO registrations(%s) VALUES (%s)''' % (', '.join(self.__columns), ', '.join([':%s' % x for x in self.__columns]))
				self.__dbconn.execute(sql, dataset)
				self.__dbconn.commit()

				return dataset

		else:
			print("No active connection to the SQLite database.")

		return None


	def cacheInvalidSearch(self, genus, grex):
		"""Record invalid search terms so that we don't need to keep hitting the RHS database."""

		if self.__dbconn is not None:

			# Check if there is an existing entry in the database
			sql = '''SELECT attempts FROM invalid WHERE genus=? AND grex=?'''
			cur = self.__dbconn.execute(sql, (genus, grex))
			results = cur.fetchall()

			if results is not None and len(results) > 0:
				attempts = results[0][0] + 1
			else:
				attempts = 1

			# Update the database
			sql = '''INSERT OR REPLACE INTO invalid(genus, grex, attempts) VALUES (?, ?, ?)'''
			self.__dbconn.execute(sql, (genus, grex, attempts))
			self.__dbconn.commit()

		else:
			print("No active connection to the SQLite database.")


	def __parseSearchResults(self, soup, genus, grex, results):
		"""Parse a result page from the RHS search."""

		tr = soup.findAll('tr')
		if results['matches'] is None:
			results['matches'] = {}

		# For each row in the table of results, looking for hybrid entries
		page_genus = None
		for row in tr:
			cells = row.findAll('td')
			if len(cells) == 2:
				# Check what genus the result entry belongs to
				result_grex = cells[0].text.strip()
				if len(result_grex) > 0:
					page_genus = result_grex
				hybrid = cells[1]

				# Tidy up the name
				name = hybrid.text.strip() # Some RHS entries have extraneous whitespace
				name = name.replace('(','[').replace(')',']')

				# If the resultant grex is in the correct genus
				if page_genus in genus:
					relpath = hybrid.find('a')['href']
					results['matches'][name] = urljoin(self._search_url, relpath)

		return results


	def search(self, genus, grex, force=False):
		"""Search the register for a registration matching
		the supplied genus and grex names."""

		# Create the URL parameters object
		db_params = {'genus': genus, 'grex': grex.replace('(','[').replace(')',']')} # Substitute any parentheses in the grex for brackets
		url_params = {'genus': genus, 'grex': grex.replace('[','(').replace(']',')')} # Substitute any brackets in the grex for parentheses

		# First check to see if this entry is currently in the database cache
		if self.__dbconn is not None and not force:
			sql = '''SELECT genus, epithet, pod_parent_genus, pod_parent_epithet, pollen_parent_genus, pollen_parent_epithet FROM registrations WHERE genus=:genus AND epithet=:grex'''
			cur = self.__dbconn.execute(sql, db_params)
			rows = cur.fetchall()

			if rows is not None and len(rows) > 0:
				return {'matched':True, 'matches':None, 'source':'db', 'pod_parent':(rows[0][2], rows[0][3]), 'pollen_parent':(rows[0][4], rows[0][5])}

			# If this isn't registered and the number of search attempts exceeds the maximum, abort the attempt
			sql = '''SELECT genus, grex, attempts FROM invalid WHERE genus=:genus AND grex=:grex'''
			cur = self.__dbconn.execute(sql, db_params)
			rows = cur.fetchall()

			if rows is not None and len(rows) > 0:
				attempts = rows[0][2]
				if attempts >= self._max_attempts:
					return {'matched':False, 'matches':None, 'source':'db', 'pod_parent':None, 'pollen_parent':None}

		try:
			req = self._session.get(self._search_url, params=url_params)
		except requests.exceptions.RequestException as err:
			return None
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(req.text, "lxml")
			page_nav = soup.find('div', {'class':'pagination'})

			# The first page
			results = {'matched':False, 'matches':None, 'source':'web', 'pod_parent':None, 'pollen_parent':None}
			results = self.__parseSearchResults(soup, genus, grex, results)
			results['matched'] = grex in results['matches']

			# If it's not matched and there are more pages, fetch those, too
			if not results['matched']  and page_nav is not None:
				pages = page_nav.findAll('li')
				for page in pages:
					anchor = page.find('a')
					if anchor is not None: # The first page won't have a link
						link = anchor['href']

						try:
							req = self._session.get(urljoin(self._search_url, link))
						except requests.exceptions.RequestException as err:
							return None
						else:
							# Parse the returned HTML
							soup = BeautifulSoup(req.text, "lxml")
							results = self.__parseSearchResults(soup, genus, grex, results)
							results['matched'] = grex in results['matches']
							if results['matched']:
								break # No need to keep looping

			# Automatically cache results
			if self.__dbconn is not None:
				if results['matched'] :
					dataset = self.cacheGrex(results['matches'][grex])

					if dataset is not None:
						results['pod_parent'] = (dataset['pod_parent_genus'],dataset['pod_parent_epithet'])
						results['pollen_parent'] = (dataset['pollen_parent_genus'],dataset['pollen_parent_epithet'])
				else:
					self.cacheInvalidSearch(genus, grex)

			return results


def testModule(database='./RHS.db'):
	"""A simple test to check that all functions are working correctly.
	Uses a known registered and valid grex."""
	myRHS = Register()
	print("Creating database file %s" % database)
	myRHS.dbConnect(database)
	req = myRHS.search('Cymbidium','Pearl',True)
	myRHS.dbClose()
	if req is not None:
		if req['matched']:
			print("You may now examine the contents of the test database.")
		else:
			print("Failed to find a known valid grex in the RHS database.")
	else:
		print("Failed to retrieve results; check if RHS database is down.")
