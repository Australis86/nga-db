#! /usr/bin/python

"""Module for exporting a genus from the Catalogue of Life's Darwin Core Archive.

This script is designed for Python 3 and Beautiful Soup 4 with the lxml parser."""

__version__ = "1.1"
__author__ = "Joshua White"
__copyright__ = "Copyright 2019"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

# Module imports
import csv
import os
import requests
import shutil
import sqlite3
import zipfile
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta
from sys import stdout

class DCA:
	
	def __init__(self):
		"""Create an instance and set up a requests session to the KEW WCSP."""
		
		self._search_url = 'http://www.catalogueoflife.org/DCA_Export/index.php'
		self._session = requests.Session()
		self._session.get(self._search_url)
		self.__cache = None
		self.__cache_age = datetime.now() - timedelta(days=5) # Default value
	
	
	def setCache(self, cache_path):
		"""Specify the cache path to be used."""
		
		if os.path.exists(cache_path):
			self.__cache = cache_path
		else:
			print("Error: invalid path supplied")
	
	
	def setCacheAge(self, cache_age_td):
		"""Specify the maximum age of files in the cache as a timedelta."""
		
		self.__cache_age = datetime.now() - cache_age_td
	
	
	def _exportGenus(self, genus, keepZIP=False):
		"""Download the genus ZIP file from the Darwin Core Archive
		and extract it into the cache folder."""
		
		if self.__cache is None:
			raise ValueError("DCA cache directory has not been set using setCache().")

		# Prepare the filename and path for the ZIP file
		zname = '%s.zip' % genus
		zpath = os.path.join(self.__cache, zname)
		gpath = None
		
		# Form data to POST
		data = {'kingdom':'Plantae', 'genus':genus, 'block':3}
		
		try:
			r = self._session.post(self._search_url, data=data)
		except requests.exceptions.RequestException as e:
			return None
		else:
			# Parse the response HTML here for the link to the ZIP file
			soup = BeautifulSoup(r.text, "lxml")
			links = soup.findAll('a')
			target = None
			
			for link in links:
				if 'zip' in link['href']:
					target = urljoin(self._search_url, link['href'])
					break
			
			# If we were able to detect a link
			if target is not None:
				r = self._session.get(target, stream=True)

				# Write out the file stream received
				with open(zpath, 'wb') as output:
					for chunk in r.iter_content(1024):
						output.write(chunk)
		
			# If the zip file successfully downloaded and is a valid zipfile, extract it
			if os.path.exists(zpath):
				if zipfile.is_zipfile(zpath):
					zfile = zipfile.ZipFile(zpath)
					gpath = os.path.join(self.__cache, genus) # Create a subfolder in the cache directory using the genus name
					zfile.extractall(gpath) # Extract the zip file into the new subfolder
				else:
					print("Invalid zip archive found. Removing.")
					keepZIP = False
					
				if not keepZIP:
					os.remove(zpath)
		
		return gpath
	
	
	def fetchGenus(self, genus):
		"""Download the genus from the DCA and import it into a SQLite DB."""
		
		if self.__cache is None:
			raise ValueError("DCA cache directory has not been set using setCache().")
		
		# Check for recent data
		fname = '%s.db' % genus
		fpath = os.path.join(self.__cache, fname)
		
		# Check the age of the cached data
		if os.path.exists(fpath) and datetime.fromtimestamp(os.path.getmtime(fpath)) > self.__cache_age:
			print("Recent SQLite DB for %s found. Skipping download and DB build." % genus)
			return fpath
		else:
			stdout.write('Fetching Catalogue of Life Darwin Core Archive Export... ')
			stdout.flush()
			gpath = self._exportGenus(genus)
			
			# Attempt to build the DB
			if gpath is not None:
				tmpdir = os.path.join(gpath, 'tmp')
				sqldir = os.path.join(gpath, 'import-scripts/sqlite3')
				
				# Only continue if the import script directory exists
				if os.path.exists(sqldir):
					
					# Clean up an existing folder structure
					if os.path.exists(tmpdir):
						shutil.rmtree(tmpdir)
					
					# Create the temporary folder
					os.mkdir(tmpdir)
					
					# Build the SQLite command file
					cf = open(os.path.join(sqldir, 'create.sql'), 'r')
					cs = cf.read()
					cf.close()
					
					# Replace references to @TABLEPREFIX@
					# Save a copy of the script (for reference only)
					cs = cs.replace('@TABLEPREFIX@','')
					cf = open(os.path.join(tmpdir, 'create.sql'), 'w')
					cf.write(cs)
					cf.close()
					
					# Prepare the commands
					tables = [
						('distribution.txt','Distribution'),
						('description.txt','Description'),
						('reference.txt','Reference'),
						('taxa.txt','Taxon'),
						('vernacular.txt','VernacularName'),
					]
					
					commands = [
						".read %s/tmp/create.sql" % genus,
						".mode tabs",
					]
					
					for t in tables:
						commands.append('.import "%s/%s" %s' % (genus, t[0], t[1]))
					
					# Create the temporary SQL file (based on provided SQLite import script)
					sqlcat = os.path.join(tmpdir, 'sqlite3init.cat')
					cf = open(sqlcat, 'w')
					cf.writelines('%s\n' % c for c in commands)
					cf.close()
					
					# Create the SQL database
					conn = sqlite3.connect(fpath)
					cur = conn.cursor()
					
					# Try to create the tables
					queries = cs.split(';')
					for q in queries:
						cur.execute(q)
						conn.commit()
					
					# Import files
					for t in tables:
						tname = os.path.join(gpath, t[0])
						with open(tname, 'r', encoding='utf-8-sig') as f:
							reader = csv.reader(f, dialect=csv.excel_tab)
							
							# Get the column names from the header row
							columns = next(reader)
							columns = [h.strip() for h in columns]
							
							# Must quote column names, since keywords 'order' and 'references' are used
							query = 'INSERT INTO %s({0}) VALUES ({1})' % t[1]
							query = query.format(','.join(['"%s"' % c for c in columns]), ','.join('?' * len(columns)))
							
							# Import each row
							for row in reader:
								cur.execute(query, row)
									
						conn.commit()
					
					# Save and close the connection
					conn.close()
					
					# Cleanup the folder and zip
					if os.path.exists(gpath):
						shutil.rmtree(gpath)
					
					stdout.write('done.\r\n')
					stdout.flush()
					return fpath
					
				else:
					stdout.write('failed. Import script path does not exist.\r\n')
					stdout.flush()

		stdout.write('failed. Unknown error.\r\n')
		stdout.flush()
		return None
	
	
def testModule(genus='Cymbidium', cache_path="./"):
	"""A simple test to check that all functions are working correctly.
	The default is to create a cache database in the local directory."""
	
	myDCA = DCA()
	try:
		print("Testing exception handling...")
		myDCA.fetchGenus(genus) # Prove the exception works
	except ValueError as e:
		print(str(e))
	myDCA.setCache(cache_path)
	myDCA.setCacheAge(timedelta(seconds=1))
	myDCA.fetchGenus(genus)
	