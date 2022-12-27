#! /usr/bin/python3

"""Module for interacting with the Catalogue of Life API.

This script is designed for Python 3 and Beautiful Soup 4 with the lxml parser."""

# Module imports
import os
import sys
import csv
import json
import getpass
import shutil
import sqlite3
import zipfile
from sys import stdout
from datetime import datetime, timedelta
from time import sleep
import requests
from requests.auth import HTTPBasicAuth

script_path = os.path.dirname(__file__)


class GBIF:
	"""GBIF class for handling authentication."""

	def __init__(self, gbif_path=None):
		"""Create an instance and set up a requests session to the COL API."""

		self._session = requests.Session()

		# Set the path to the GBIF auth file
		if gbif_path is not None:
			self._authpath = gbif_path
		else:
			self._authpath = os.path.join(os.path.expanduser('~'), '.gbif')

		# This will look for the GBIF credentials
		self._loadAuthFile(self._authpath)


	def _loadAuthFile(self, auth_file):
		"""Load a file containing the user's GBIF account details."""

		if not os.path.exists(auth_file):
			_createAuthFile(auth_file)

		with open(auth_file, 'r', encoding='utf-8') as gbif:
			gbif_auth = gbif.read()

		gbif_account = gbif_auth.split(':')
		if len(gbif_account) > 1:
			username = gbif_account[0]
			password = gbif_account[1]

		self._auth = HTTPBasicAuth(username, password) # GBIF account



class COL(GBIF):
	"""COL class for working with the COL API."""

	def __init__(self):
		"""Create an instance and set up a requests session to the COL API."""

		GBIF.__init__(self)
		self._search_url = 'https://api.checklistbank.org/dataset/3LR/nameusage/search'
		self._synonym_url = 'https://api.checklistbank.org/dataset/%s/taxon/%s/synonyms'


	def search(self, search_term, fetch_synonyms=False):
		"""Search the COL for a particular entry and returned the accepted name or synonyms."""

		# Query parameters
		params = {'q':search_term, 'content': 'SCIENTIFIC_NAME', 'maxRank':'SPECIES', 'type': 'EXACT', 'offset':0, 'limit':10}

		# First step is to get the taxon ID, then fetch the synonyms if required
		try:
			req = self._session.get(self._search_url, params=params, headers={'accept': 'application/json'})
		except requests.exceptions.RequestException:
			return [None, 'Error retrieving taxon']
		else:
			rdata = req.json()
			#print(json.dumps(rdata, indent=4, sort_keys=True))

			if rdata['empty']:
				# No match found
				return [None, 'No match found in COL']

			illegal_status = []
			if len(rdata['result']) > 0:
				closest = None
				for result in rdata['result']:
					# Need to make sure we're only using entries from the plant kingdom
					kingdom = False
					for cls in result['classification']:
						if 'rank' in cls and cls['rank'] == 'kingdom':
							if cls['name'] == 'Plantae':
								kingdom = True

					rstatus = result['usage']['status'].lower()
					if kingdom:
						# Exclude illegal or ambiguous names
						if ('misapplied' not in rstatus) and ('ambiguous' not in rstatus):
							closest = result
							break
						if rstatus not in illegal_status:
							illegal_status.append(rstatus)
					else:
						illegal_status.append('not in the plant kingdom')
			else:
				closest = rdata['result'][0]

			if fetch_synonyms:
				# Get the taxon ID so that we can get the synonyms
				taxon_id = closest['id']
				dataset_key = closest['usage']['datasetKey']

				# Post to the asynchronous API (this requests a build of an export)
				try:
					req = self._session.get(self._synonym_url % (dataset_key, taxon_id), auth=self._auth, headers={"Content-Type": "application/json"})
				except requests.exceptions.RequestException:
					return [None, 'Unable to retrieve synonyms from COL']
				else:
					synonyms = []
					rdata = req.json()
					#print(json.dumps(rdata, indent=4, sort_keys=True))

					# Check if there are any synonyms
					if not rdata:
						return [None, 'No synonyms available in COL']

					# Iterate through the types of synonyms and collect the botanical names
					for synonym_type in rdata:
						for synonym in rdata[synonym_type]:
							# This will be a list for synonyms, dict for misapplied names
							try:
								if isinstance(synonym, list):
									syn = synonym[0]
									status = syn['status'].lower()
								else:
									syn = synonym['name']
									status = synonym['status'].lower()

								# Status field isn't always included for some reason
								if synonym_type == 'heterotypicGroups':
									sname = syn['name']['scientificName']
									if sname not in synonyms:
										synonyms.append(sname)
								elif synonym_type == 'heterotypic' or 'misapplied' not in status:
									sname = syn['scientificName']
									if sname not in synonyms:
										synonyms.append(sname)
							except KeyError:
								print()
								print("Warning: check synonym object - unhandled synonym type", synonym_type)
								print()
								print("Raw data as follows")
								for k in rdata.keys():
									print()
									print('Synonym Type:', k)
									print(rdata[k])

					synonyms.sort()
					return synonyms

			else:
				# If we don't need the synonyms, then everything we need is in this result dataset
				if closest is not None:
					usage = closest['usage']
					status = usage['status'].lower()
				else:
					status = ''

				if 'accepted' in status:
					acceptedname = usage['name']
				elif 'synonym' in status:
					acceptedname = usage['accepted']['name']
				else:
					if len(illegal_status) > 0:
						return [None, f'Invalid status: {("/".join(illegal_status))}']

					return [None, 'No accepted or synonym name available from COL']

				return [acceptedname['scientificName']]


class DCA(GBIF):
	"""Class for working with Darwin Core Archive exports from the COL API."""

	def __init__(self):
		"""Create an instance and set up a requests session to the COL API."""

		GBIF.__init__(self)
		self._search_url = 'https://api.checklistbank.org/dataset/3LR/nameusage/search'
		self._export_request_url = 'https://api.checklistbank.org/dataset/%s/export'
		self._export_retrieve_url = 'https://api.checklistbank.org/export/%s'
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


	def _exportGenus(self, genus, keep_zip=False):
		"""Download the genus ZIP file from the Darwin Core Archive
		and extract it into the cache folder."""

		if self.__cache is None:
			raise ValueError("DCA cache directory has not been set using setCache().")

		# Prepare the filename and path for the ZIP file
		zname = f'{genus}.zip'
		zpath = os.path.join(self.__cache, zname)
		gpath = None
		errmsg = None

		# Query parameters
		params = {'q':genus, 'minRank':'GENUS', 'maxRank':'GENUS', 'offset':0, 'limit':10}

		# First step is to get the taxon ID, then use that to retrieve the DwC-A export
		try:
			req = self._session.get(self._search_url, params=params, headers={'accept': 'application/json'})
		except requests.exceptions.RequestException:
			return (None, 'Unable to retrieve taxon ID.')
		else:
			rdata = req.json()
			taxon_id = None

			if 'total' in rdata and rdata['total'] == 0:
				return (None, 'No matches found in COL search.')

			# Iterate through the results
			for res in rdata['result']:
				# Iterate through the classification entries
				for cls in res['classification']:
					if 'rank' in cls and cls['rank'] == 'kingdom':
						# Make sure this is an accepted genus within the plant kingdom
						if cls['name'] == 'Plantae' and res['usage']['status'].lower() == 'accepted':
							dataset_key = res['usage']['datasetKey']
							taxon_id = res['id']
							break
				else:
					# If the inner loop does not break, continue
					continue
				# If the inner loop breaks, break here too
				break

			if taxon_id is None:
				return (None, 'No matches in the Plant kingdom found in COL search.')

			# Prepare the export data
			data = {"format":"DWCA", "root":{"id":taxon_id}, "synonyms": True}

			# Post to the asynchronous API (this requests a build of an export)
			try:
				req = self._session.post(self._export_request_url % dataset_key, auth=self._auth, data=json.dumps(data), headers={"Content-Type": "application/json"})
			except requests.exceptions.RequestException:
				return (None, 'Unable to request build of the Darwin Core Archive.')
			else:
				# This should return the export key that can be used to fetch the ZIP file
				rdata = req.json()

				# Check the status of the export
				finished = False
				delay = 15
				ecount = 0
				while not finished:
					try:
						# Get the status of the export
						req = self._session.get(self._export_retrieve_url % rdata, auth=self._auth, headers={"Accept": "application/json"})
					except requests.exceptions.RequestException as err:
						stdout.write('e')
						stdout.flush()
						if ecount < 3:
							ecount += 1
							sleep(30)
						else:
							return (None, str(err))
					else:
						if req.status_code != 200:
							# Something went wrong with the request
							return (None, f'HTTP Error {req.status_code} was returned when attempting to fetch the Darwin Core Archive.')

						# Extract the status field; valid responses are:
						# waiting, blocked, running, finished, canceled, failed
						qdata = req.json()
						status = qdata['status'].lower().strip()
						if status in ('canceled','failed'):
							return (None, f'Export job {status}.')

						if 'finished' in status:
							finished = True
						else:
							sleep(delay)
							stdout.write('.')
							stdout.flush()

				# Fetch the export
				try:
					req = self._session.get(self._export_retrieve_url % rdata, auth=self._auth, headers={"Accept": "application/octet-stream, application/zip"}, stream=True)
				except requests.exceptions.RequestException:
					return (None, None)
				else:
					if req.status_code != 200:
						return (None, f'HTTP Error {req.status_code} was returned when attempting to fetch the Darwin Core Archive.')

					# Write out the file stream received
					with open(zpath, 'wb') as output:
						for chunk in req.iter_content(1024):
							output.write(chunk)

				# If the zip file successfully downloaded and is a valid zipfile, extract it
				if os.path.exists(zpath):
					if zipfile.is_zipfile(zpath):
						with zipfile.ZipFile(zpath) as zfile:
							gpath = os.path.join(self.__cache, genus) # Create a subfolder in the cache directory using the genus name
							zfile.extractall(gpath) # Extract the zip file into the new subfolder
					else:
						errmsg = "The downloaded Darwin Core Archive export was not a valid zip file."
						gpath = None
						keep_zip = False

					if not keep_zip:
						os.remove(zpath)

		return (gpath, errmsg)


	def fetchGenus(self, genus):
		"""Download the genus from the DCA and import it into a SQLite DB."""

		if self.__cache is None:
			raise ValueError("DCA cache directory has not been set using setCache().")

		errmsg = None

		# Check for recent data
		fname = f'{genus}.db'
		fpath = os.path.join(self.__cache, fname)

		# Check the age of the cached data
		if os.path.exists(fpath) and datetime.fromtimestamp(os.path.getmtime(fpath)) > self.__cache_age:
			print(f'Recent SQLite DB for {genus} found. Skipping download and DB build.')
			return fpath

		stdout.write('Fetching Catalogue of Life Darwin Core Archive Export...')
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
					f'.read {script_path}/create-DCA-tables.sql',
					'.mode tabs',
				]

				for table in tables:
					commands.append(f'.import "{genus}/{table[0]}" {table[1]}')

				stdout.write(' done.\r\nBuilding database... ')
				stdout.flush()

				# Create the temporary SQL file (based on provided SQLite import script)
				sqlcat = os.path.join(tmpdir, 'sqlite3init.cat')
				with open(sqlcat, 'w', encoding='utf-8') as file_desc:
					file_desc.writelines(f'{comm}\n' for comm in commands)

				# Create the SQL database
				if os.path.exists(fpath):
					os.remove(fpath)

				conn = sqlite3.connect(fpath)
				cur = conn.cursor()

				# Try to create the tables
				with open(os.path.join(script_path,'create-DCA-tables.sql'), 'r', encoding='utf-8') as file_desc:
					contents = file_desc.read()

				queries = contents.split(';')
				for query in queries:
					cur.execute(query)
					conn.commit()

				# Import files
				for table in tables:
					tname = os.path.join(gpath, table[0])
					with open(tname, 'r', encoding='utf-8-sig') as file_desc:
						reader = csv.reader(file_desc, dialect=csv.excel_tab, quoting=csv.QUOTE_NONE)

						# Get the column names from the header row
						columns = next(reader)
						columns = [h.strip().split(':')[-1] for h in columns]

						# Must quote column names, since keywords 'order' and 'references' are used
						query = f'INSERT INTO {table[1]}({{0}}) VALUES ({{1}})'
						query = query.format(','.join([f'"{col}"' for col in columns]), ','.join('?' * len(columns)))

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

			stdout.write('failed. Import script path does not exist.\r\n')
			stdout.flush()

		if errmsg is None:
			stdout.write('failed. Unknown error.\r\n')
			stdout.flush()
		else:
			stdout.write('failed with the following error:\r\n')
			stdout.flush()
			print(errmsg)
			if os.path.exists(fpath):
				print("Using old dataset - entries may be out of date!")
			else:
				print("Unable to continue without COL DWA dataset.")
				sys.exit(1)

		return None


def _createAuthFile(auth_file):
	"""Store a set of authentication parameters."""

	# Ask the user for their credentials
	print("Please enter your GBIF credentials.")
	user = input("Username: ")
	pwd = getpass.getpass()

	# Test the credentials
	req = requests.get("https://api.checklistbank.org/user/me", auth=HTTPBasicAuth(user, pwd), headers={'accept': 'application/json'}, timeout=30)
	if req.status_code != 200:
		raise PermissionError("Failed to authenticate with the COL API.")

	print("Successfully tested authentication.")

	# Store the credentials
	with open(auth_file, 'w', encoding='utf-8') as gbif:
		os.chmod(auth_file, 0o0600) # Try to ensure only the user can read it
		gbif.write(f'{user}:{pwd}')


def testSearch(search_term='Cymbidium iansonii'):
	"""A simple test to check that all functions are working correctly."""

	my_col = COL()
	print(my_col.search(search_term))
	print(my_col.search(search_term, True))


def testExport(genus='Cymbidium', cache_path="./"):
	"""A simple test to check that all functions are working correctly.
	The default is to create a cache database in the local directory."""

	my_dca = DCA()
	try:
		print("Testing exception handling...")
		my_dca.fetchGenus(genus) # Prove the exception works
	except ValueError as err:
		print(str(err))
	my_dca.setCache(cache_path)
	my_dca.setCacheAge(timedelta(seconds=1))
	my_dca.fetchGenus(genus)
