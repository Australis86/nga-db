#! /usr/bin/python

"""Module for exporting a genus from the Catalogue of Life's Darwin Core Archive.

This script is designed for Python 3 and Beautiful Soup 4 with the lxml parser."""

__version__ = "2.0"
__author__ = "Joshua White"
__copyright__ = "Copyright 2021"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

# Module imports
import csv
import os
import json
import requests
import shutil
import sqlite3
import zipfile
import getpass
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timedelta
from sys import stdout
script_path = os.path.dirname(__file__)


class DCA:
	
	def __init__(self):
		"""Create an instance and set up a requests session to the COL API."""
		
		self._search_url = 'https://api.catalogueoflife.org/dataset/3LR/nameusage/search'
		self._export_request_url = 'https://api.catalogueoflife.org/dataset/%s/export'
		self._export_retrieve_url = 'https://api.catalogueoflife.org/export/%s'
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
		print("Please enter your GBIF credentials.")
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
		errmsg = None
		
		# Query parameters
		params = {'q':genus, 'minRank':'GENUS', 'maxRank':'GENUS', 'offset':0, 'limit':10}
		
		# First step is to get the taxon ID, then use that to retrieve the DwC-A export
		try:
			r = self._session.get(self._search_url, params=params, headers={'accept': 'application/json'})
		except requests.exceptions.RequestException as e:
			return (None, 'Unable to retrieve taxon ID.')
		else:
			rdata = r.json()
			taxonID = rdata['result'][0]['id']
			datasetKey = rdata['result'][0]['usage']['datasetKey']
			
			# Prepare the export data
			data = {"format":"DWCA", "taxonID":taxonID, "synonyms": True}
			
			# Post to the asynchronous API (this requests a build of an export)
			try:
				r = self._session.post(self._export_request_url % datasetKey, auth=self.__auth, data=json.dumps(data), headers={"Content-Type": "application/json"})
			except requests.exceptions.RequestException as e:
				return (None, 'Unable to request build of the Darwin Core Archive.')
			else:
				# This should return the export key that can be used to fetch the ZIP file
				rdata = r.json()
				
				# Fetch the export
				try:
					r = self._session.get(self._export_retrieve_url % rdata, auth=self.__auth, headers={"Accept": "application/octet-stream, application/zip"}, stream=True)
				except requests.exceptions.RequestException as e:
					return (None, None)
				else:
					if (r.status_code != 200):
						return (None, 'HTTP Error %s was returned when attempting to fetch the Darwin Core Archive.' % r.status_code)

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
						errmsg = "The downloaded Darwin Core Archive export was not a valid zip file."
						gpath = None
						keepZIP = False
						
					if not keepZIP:
						os.remove(zpath)
		
		return (gpath, errmsg)
	
	
	def fetchGenus(self, genus):
		"""Download the genus from the DCA and import it into a SQLite DB."""
		
		if self.__cache is None:
			raise ValueError("DCA cache directory has not been set using setCache().")
		
		errmsg = None
		
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
			(gpath, errmsg) = self._exportGenus(genus)
			
			# Attempt to build the DB
			if gpath is not None:
				tmpdir = os.path.join(gpath, 'tmp')
				sqldir = script_path
				
				# Only continue if the import script directory exists
				if os.path.exists(sqldir):
					
					# Clean up an existing folder structure
					if os.path.exists(tmpdir):
						shutil.rmtree(tmpdir)
					
					# Create the temporary folder
					os.mkdir(tmpdir)
					
					# File name : table name relationship
					tables = [
						('Distribution.tsv','Distribution'),
						('SpeciesProfile.tsv','SpeciesProfile'),
						('Taxon.tsv','Taxon'),
						('VernacularName.tsv','VernacularName'),
					]
					
					# Prepare the commands
					commands = [
						".read %s/create-DCA-tables.sql" % script_path,
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
					if os.path.exists(fpath):
						os.remove(fpath)
					
					conn = sqlite3.connect(fpath)
					cur = conn.cursor()
					
					# Try to create the tables
					c = open(os.path.join(script_path,'create-DCA-tables.sql'), 'r')
					cs = c.read()
					c.close()
					
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
							columns = [h.strip().split(':')[-1] for h in columns]
							
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

		if errmsg is None:
			stdout.write('failed. Unknown error.\r\n')
			stdout.flush()
		else:
			stdout.write('failed with the following error:\r\n')
			stdout.flush()
			print(errmsg)
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
	