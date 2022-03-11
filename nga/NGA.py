#! /usr/bin/python

"""Module for working with the NGA database.

You can set the global variable NGA_COOKIE to specify the file used when
creating an instance of the class, or you can reload the cookie archive later
using .regenerateSessionCookie(path-to-cookie)

This script is designed for Python 3 and Beautiful Soup 4 with the lxml parser."""

__version__ = "1.2"
__author__ = "Joshua White"
__copyright__ = "Copyright 2021"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

# Module imports
import os
import sys
import json
import re
import time
import getpass
from collections import OrderedDict
from urllib.parse import urljoin, urlparse, parse_qs
from sys import stdout
import requests
from bs4 import BeautifulSoup

class NGA:
	"""Create a user-friendly API for the NGA website's Plants Database."""

	def __createSession(self):
		"""Create a requests session."""

		self._session = requests.Session()

		# Requests user agent has been blocked by Garden.org, unfortunately
		self._session.headers.update({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0'})


	def __init__(self):
		"""Create an instance and set up a requests session to the NGA website."""

		self._auth_url = 'https://garden.org/login.php'
		self._home_url = 'https://garden.org/'
		self._search_url = 'https://garden.org/plants/search/text/'
		self._genus_url = 'https://garden.org/plants/browse/plants/genus/%s/'
		self._plant_name_url = 'https://garden.org/plants/propose/edit_name/%s/'
		self._plant_data_url = 'https://garden.org/plants/propose/databox/%s/'
		self._new_plant_url = 'https://garden.org/plants/propose/new_plant/'
		self._new_proposals_url = 'https://garden.org/plants/mod/plants/'
		self._new_approvals_url = 'https://garden.org/plants/mod/plants/approve/'
		self._merge_plant_url = 'https://garden.org/plants/propose/merge_plant/%s/'

		# Set the path to the NGA cookie
		if 'NGA_COOKIE' in globals():
			self._cookiepath = NGA_COOKIE
		else:
			self._cookiepath = os.path.join(os.path.expanduser('~'), '.nga')
		self.__NGAcookie = requests.cookies.RequestsCookieJar()

		# Create the session
		self.__createSession()

		# This will look for a JSON file containing authentication info for the website
		# If it exists, it will load it and use it for the session
		self.__loadCookieArchive(self._cookiepath)

		self._session.cookies = self.__NGAcookie
		self._session.get(self._home_url)

		# Delay to wait before hitting NGA servers again if the connection failed
		self._recursion_delay = 1


	def __loadCookieArchive(self, cookiepath):
		"""Load a JSON file containing cookies for the NGA website."""

		if os.path.exists(cookiepath):
			# Open the JSON file
			fd = open(cookiepath, 'r')

			# Try to read the JSON string and set the cookie parameters
			try:
				c = json.load(fd)
				self.__NGAcookie.set('gojwt', c['gojwt'], domain='garden.org', path='/')

			except Exception as err:
				# We must have a valid cookie file, or the NGA site will block us`
				print("Error loading cookie archive %s. A valid cookie file is required to use this script." % cookiepath)

				# Re-raise the exception
				raise err

			fd.close()
		else:
			# We must have a valid cookie file, or the NGA site will block us`
			print("Cookie archive %s not found. A valid cookie file is required to use this script." % cookiepath)
			self.__startAuthSession()


	def __storeCookieArchive(self, cookiejson):
		"""Store a JSON file containing cookies for the NGA website."""

		# Open the file for writing
		fd = open(self._cookiepath, 'w')
		os.chmod(self._cookiepath, 0o0600) # Try to ensure only the user can read it

		# Try to write the JSON string)
		try:
			json.dump(cookiejson, fd)

		except Exception as err:
			# We must have a valid cookie file, or the NGA site will block us`
			print("Error writing cookie archive %s. A valid cookie file is required to use this script." % cookiepath)

			# Re-raise the exception
			raise err

		fd.close()


	def __startAuthSession(self):
		"""Authenticate with the NGA website."""

		# Create the session
		self.__createSession()

		# Get the login page
		print("Fetching authentication page...")
		r = self._session.get(self._auth_url)

		if r.status_code != 200:
			# Failed to retrieve the login page
			raise ConnectionError("Unable to successfully retrieve authentication page.")

		# Successfully retrieved the login page
		target = urljoin(self._home_url, '/i/ajax/users/login_check.php')
		soup = BeautifulSoup(r.text, "lxml")

		# Find the redirect address
		redirect_url = soup.find(id='login_redirect')['value']
		redirect = urljoin(self._home_url, redirect_url)
		print("Retrieved authentication page. Please enter your garden.org login details:")

		# Build the object to post to the server
		form_data = {}

		# Ask the user for their credentials
		form_data['u'] = input("Username: ")
		form_data['p'] = getpass.getpass()

		# POST the form data
		r = self._session.post(target, data=form_data)

		# Successfully authenticated
		if r.status_code == 200 and "1" in r.text:

			# Redirect to the logged in page
			r = self._session.get(redirect)

			# Retrieve the cookies and ensure we have valid credentials
			c = self._session.cookies.get_dict()
			if 'gojwt' in c:
				self.__NGAcookie = self._session.cookies
				self.__storeCookieArchive(c)
				print("Successfully authenticated.")
			else:
				raise PermissionError("Failed to login: session cookie not found.")
		else:
			raise PermissionError("Failed to login: redirected to login page.")


	def regenerateSessionCookie(self, cookiepath):
		"""Regenerate the session cookie using a cookie file with existing auth info.
		May be used to re-authenticate if your cookie file is not in the working
		directory when you create an instance of the NGA class."""

		self._session.cookies = None # Clear the existing cookies first
		self.__loadCookieArchive(cookiepath)
		self._session.get(self._home_url)


	def _generatePlantObject(self, name, url=None):
		"""Generate a dictionary object to be used for plant entries."""

		# Default plant object assumes no change to common name
		obj = {'full_name': name.replace('  ',' '), 'url': url, 'pid': None, 'common_name': False, 'warning':False}

		# Extract the plant ID
		if url is not None:
			pid = re.search('(?:/)(\d+)(?:/)', url, re.DOTALL).group(1)
			obj['pid'] = pid

		return obj


	def __parseTableRow(self, row, cname_exclude=None):
		"""Extract the plant information from a row in the NGA genus or search results table."""

		# Extract the second column, as this contains the entry name
		entry = row.findAll('td')[1]

		# Link to the plant entry on the website
		anchor = entry.find('a')
		entry_link = anchor['href']
		anchor_text = anchor.text

		# Name components
		# If an entry has a common name, the botanic name and cultivar will be in parentheses
		if '(' in anchor_text:
			regex = '(?:\()(.+)(?:\))'
			entry_name = re.search(regex, anchor_text, re.DOTALL).group(1) # Remember to use group 1 here
		else:
			entry_name = anchor_text

		italics = entry.find('i')
		if italics is None:
			# This was probably a parent entry
			return (None, None, None)
		else:
			botanic_name = italics.text # Botanical part
			cultivar_name = entry_name.replace(botanic_name, '').strip() # Cultivar
			plant_data = self._generatePlantObject(entry_name, entry_link)

			# Check if there is a common name
			commonname = italics.previousSibling
			if commonname is not None:
				commonname = commonname.strip().strip('(').strip()
				if commonname in botanic_name:
					plant_data['common_name'] = True # Change common name
				elif commonname==cname_exclude:
					plant_data['common_name'] = True # Change common name
					plant_data['common_exclude'] = cname_exclude
			else:
				plant_data['common_name'] = None # Common name missing

			return (botanic_name, cultivar_name, plant_data)



	def _parseGenusPage(self, page_soup, genus=None):
		"""Parse a BeautifulSoup object returned by the _fetchGenusPage function."""

		# Get the table on the page
		if page_soup is not None:
			table = page_soup.find('table')

			# Iterate through the table
			if table is not None and len(table) > 0:

				# Iterate through all rows of the table
				for row in table.findAll('tr'):

					# Extract the contents of the row
					(botanic_name, cultivar_name, plant_data) = self.__parseTableRow(row, genus)

					if botanic_name is not None:
						botanic_name = botanic_name.replace('  ',' ')

						# Add entry to results
						if botanic_name not in self.__genus_results:
							self.__genus_results[botanic_name] = {}

						self.__genus_results[botanic_name][cultivar_name] = plant_data


	def _parseGenusPages(self, pages, genus=None):
		"""Parse a list of the BeautifulSoup object returned by the _fetchGenusPage function."""

		# Initialise the dataset
		self.__genus_results = {}

		stdout.write("\rParsing NGA dataset...")
		stdout.flush()

		n = len(pages)
		x = 1

		for page in pages:
			stdout.write("\rParsing NGA dataset... %d/%d" % (x, n))
			stdout.flush()

			self._parseGenusPage(page, genus)
			x += 1


		n_entries = len(list(self.__genus_results))
		stdout.write("\rParsing NGA dataset... done. %d botanic name(s) found.\r\n" % n_entries)
		stdout.flush()

		return self.__genus_results


	def _fetchGenusPage(self, genus, offset=None):
		"""Retrieve a single page of genus data from the NGA database."""

		if offset is not None:
			params = {'offset': offset}
		else:
			params = {}

		try:
			r = self._session.get(self._genus_url % genus, params=params)
		except requests.exceptions.RequestException as e:
			print("\nError retrieving NGA database page for genus %s. Cannot continue." % genus)
			sys.exit(1)
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(r.text, "lxml")
			return soup

		return None


	def fetchGenus(self, genus):
		"""Retrieve the list of entries for a genus from the NGA database."""

		stdout.write("\rRetrieving NGA dataset...")
		stdout.flush()

		genus_pages = []

		# Get the first page and find the number of plants and pages
		genus_pages.append(self._fetchGenusPage(genus))

		if genus_pages[0] is not None:

			# Determine number of pages of data in this genus
			pages = genus_pages[0].findAll('a', {'class':'page-link'})
			increment = None
			npages = 1

			if pages is not None:
				for pg in pages:
					pgnum = pg.text
					pgurl = urlparse(pg['href']) # Parse the URL
					pgquery = parse_qs(pgurl.query)
					if 'offset' in pgquery:
						pgoffset = pgquery['offset'][0] # Extract the offset

						try:
							# Extract the offset (based on the link to page 2)
							num = int(pgnum)
							offset = int(pgoffset)
							if num == 2:
								increment = offset

							# Get the number of pages (look for the highest number)
							if num > npages:
								npages = num

						except ValueError as e:
							pass

			# If increment is still None at this stage, then there is only one page
			# Otherwise fetch all the remaining pages
			if increment is not None:

				# Tested adding multithreading here, but it doesn't provide enough of a benefit
				for x in range(1, npages):
					stdout.write("\rRetrieving NGA dataset... %d/%d" % (x+1, npages))
					stdout.flush()
					genus_pages.append(self._fetchGenusPage(genus, x*increment))

		stdout.write("\rRetrieving NGA dataset... done.        \r\n")
		stdout.flush()

		# Parse the pages and extract the species and cultivars
		return self._parseGenusPages(genus_pages, genus)


	def search(self, search_term, recursed=False):
		"""Search the NGA plant database for a given entry.
		Returns a dictionary of matches grouped by botanical name."""

		params = {'q':search_term}

		try:
			r = self._session.get(self._search_url, params=params)
		except requests.exceptions.RequestException as e:
			if not recursed:
				time.sleep(self._recursion_delay)
				return self.search(search_term, True)
			else:
				print("Error retrieving NGA search results for %s." % search_term)
				print(str(e))
				return None
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(r.text, "lxml")
			caption = soup.find('caption',string="Search Results")

			# If there are search results, then the caption will exist
			if caption is not None:
				table = caption.parent
				rows = table.findAll('tr')

				botanic_entries = {}

				# Process all the rows and extract the botanic and cultivar names
				for row in rows:
					(botanic_name, cultivar_name, plant_data) = self.__parseTableRow(row)
					if botanic_name not in botanic_entries:
						botanic_entries[botanic_name] = {cultivar_name: plant_data}
					else:
						botanic_entries[botanic_name][cultivar_name] = plant_data

				return botanic_entries

			# No search results
			else:
				return None


	def formatParentage(self, genus, dataset):
		"""Format the parentage data to be consistent with the NGA database entry.
		Current database rules do not permit the use of parentage fields containing
		different genera unless the parent is a species."""

		def checkUnknown(parent):
			"""Method to check if the parentage provided is unknown."""

			# List of strings used to describe unknown parents
			unknown_list = ['na','uk','?','unknown']

			# Convert to lowercase and clean up
			genus = parent[0].lower().strip()
			taxon = parent[1].lower().strip()

			# Return true if either the genus or species/hybrid taxon is unknown
			return genus in unknown_list or taxon in unknown_list

		def checkIfSpecies(genus, parent):
			"""Method to check if the parent is a species or hybrid."""

			# Check if the parent is in the same genus as the hybrid
			different_genus = (genus != parent[0])

			# Check if is lowercase (and possibly symbols); i.e. a species
			species = all(c.islower() or not c.isalpha() for c in parent[1])

			if species or different_genus:
				rp = ' '.join(parent)
			else:
				rp = parent[1]

			return (different_genus, species, rp)

		def checkIfNaturalHybrid(parent):
			"""Method to check if an entry is a natural hybrid
			(i.e. contains the multiplication symbol). If so,
			substitute for x to be consistent with the database."""

			# TO DO: Check for the presence of symbols
			return parent


		# Make sure the fields are present
		if 'pod_parent' in dataset and 'pollen_parent' in dataset:
			pod_parent = dataset['pod_parent']
			pollen_parent = dataset['pollen_parent']

			# Check the fields
			if pod_parent is not None and pollen_parent is not None:

				# Check if unknown
				unk_mother = checkUnknown(pod_parent)
				unk_father = checkUnknown(pollen_parent)

				# If both parents are unknown, abort
				if unk_mother and unk_father:
					return None

				# Check if species (if so, we need the genus included)
				mother = checkIfSpecies(genus, pod_parent)
				father = checkIfSpecies(genus, pollen_parent)

				mother = checkIfNaturalHybrid(mother)
				father = checkIfNaturalHybrid(father)

				parentage = {
					'formula': ' X '.join([mother[2], father[2]]),
					'intergeneric': mother[0] or father[1],
					'violates_rules': (mother[0] and not mother[1]) or (father[0] and not father[1])
				}

				return parentage

		return None


	def checkPageFields(self, plant):
		"""Check which fields are populated on a plant database entry. Useful for determining if we can automatically merge entries."""

		planturl = urljoin(self._home_url, plant['url'])

		try:
			r = self._session.get(planturl)
		except requests.exceptions.RequestException as e:
			print("Error retrieving NGA plant entry for %s." % plant['full_name'])
			print(str(e))
			return None
		else:
			# Storage for relevant fields
			fields = {
				'cards': [],
				'databoxes': [],
			}

			# Parse the returned HTML
			soup = BeautifulSoup(r.text, "lxml")

			# Look for class "card-header", as this is used for the common names, botanical names, conservation status, images and comments
			cards = soup.findAll('div', {'class':'card-header'})
			if len(cards) > 0:
				for card in cards:
					contents = card.get_text().strip().strip(':')
					# TO DO: Exclude photo gallery and comments here, since they are migrated by the merge but the other sections are not
					fields['cards'].append(contents)

			# Look for the caption element, as this indicates data tables
			captions = soup.findAll('caption')
			if len(captions) > 0:
				for caption in captions:
					contents = caption.get_text().strip().split(' (')[0]
					fields['databoxes'].append(contents)

			return fields


	def checkParentageField(self, plant, recursed=False):
		"""Check if the parentage field exists for an entry."""

		planturl = urljoin(self._home_url, plant['url'])

		try:
			r = self._session.get(planturl)
		except requests.exceptions.RequestException as e:
			if not recursed:
				time.sleep(self._recursion_delay)
				return self.checkParentageField(plant, True)
			else:
				print("Error retrieving NGA plant entry for %s." % plant['full_name'])
				print(str(e))
				return None
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(r.text, "lxml")
			parentage = soup.find('b', string=re.compile('Parentage'))

			return parentage is not None

		return None


	def __submitProposal(self, url, data, auto_approve=True):
		"""Submit a proposal and try to approve it.

		Returns:
			- True if approved
			- False if submitted but not approved
			- None otherwise"""

		try:
			r = self._session.post(url, data=data)
		except requests.exceptions.RequestException as e:
			print("Failed to submit proposal", str(e))
			return None
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(r.text, "lxml")
			confirmation = soup.findAll('a', attrs={'href': re.compile("approve")})
			alert = soup.findAll('div', attrs={'class': 'alert-danger'})

			if confirmation is not None and len(confirmation) > 0:
				if auto_approve:
					subpage = confirmation[0]['href']
					suburl = urljoin(self._home_url, subpage)

					try:
						r = self._session.get(suburl)
					except requests.exceptions.RequestException as e:
						print("Failed to approve proposal", str(e))
						print(str(e))
						return False
					else:
						soup = BeautifulSoup(r.text, "lxml")
						approved1 = soup.findAll('div', {'class':'alert-success'}) # Used for approvals via queue
						approved2 = soup.findAll('a', attrs={'href': re.compile("/plants/view/")}) # Only used for direct path of new plant proposals

						if (approved1 and len(approved1) > 0) or (approved2 and len(approved2) > 0):
							print("\tProposal approved.")
							return True
						else:
							print("\tProposal not approved.")
							return False
				else:
					print("\tProposal submitted.")
					return False

			elif alert is not None and len(alert) > 0:
				print("\tFailed to submit proposal - name already in use.")
			else:
				print("\tFailed to submit proposal.")


	def fetchNewProposals(self, recursed=False):
		"""Check to see if a new plant proposal exists. Requires admin rights."""

		try:
			r = self._session.get(self._new_proposals_url)
		except requests.exceptions.ConnectionError as e:
			if not recursed:
				time.sleep(1)
				return self.fetchNewProposals(True)
			else:
				print(str(e))
				return None
		except requests.exceptions.RequestException as e:
			print(str(e))
			return None
		else:
			soup = BeautifulSoup(r.text, "lxml")
			ptable = soup.find("table", {"id":"table"})
			proposals = soup.findAll('tr')
			pending = {}

			for p in proposals:
				if p.find("th") is not None:
					continue

				cells = p.findAll("td")
				pid = cells[0].get_text().strip()
				genus = cells[1].get_text().strip()
				species = cells[2].get_text().strip()
				cultivar = cells[3].get_text().strip()
				tradename = cells[4].get_text().strip()
				series = cells[5].get_text().strip()

				if len(cultivar) == len(tradename) == len(series) == 0:
					botanical_name = '%s %s' % (genus, species)
					if botanical_name not in pending:
						pending[botanical_name] = []

					pending[botanical_name].append(pid)

			return pending


	def checkNewProposal(self, pending, botanic_name, common_name=None):
		"""Check to see if a new plant proposal exists. Requires admin rights."""

		if pending is not None:
			if botanic_name in pending:
				return pending[botanic_name][0]

		return None


	def approveNewProposal(self, pid):
		"""Approve an existing proposal based on id. Requires admin rights."""

		params = {
			'id':pid
		}

		try:
			r = self._session.post(self._new_approvals_url, data=params)
		except requests.exceptions.RequestException as e:
			print("\tFailed to approval proposal", str(e))
		else:
			# Parse the response
			soup = BeautifulSoup(r.text, "lxml")
			approved = soup.findAll('a', attrs={'href': re.compile("/plants/view/")})

			if approved and len(approved) > 0:
				print("\tProposal approved.")
			else:
				print("\tProposal not approved.")


	def proposeNewPlant(self, botanic_name, common_name=None):
		"""Propose a plant on the NGA site."""

		# The COL search results is applicable to both the DCA export data
		# and the COL search in this case, so no need to make it editable yet
		COL_URL = 'https://www.catalogueoflife.org/data/search?facet=rank&facet=issue&facet=status&facet=nomStatus&facet=nameType&facet=field&facet=authorship&facet=extinct&facet=environment&limit=50&offset=0&q=%s&sortBy=taxonomic' % botanic_name.split()[0]

		if common_name is None:
			common_name = ''

		params = {
			'common':common_name,
			'cultivar':'',
			'latin':botanic_name,
			'notes':COL_URL,
			'series':'',
			'submit':'Proceed to Step 2',
			'tradename':'',
		}

		# POST the data
		self.__submitProposal(self._new_plant_url, params)


	def proposeSynonymAddition(self, plant, synonym, common_name=None, auto_approve=True):
		"""Propose the addition of a synonym to a plant entry in the database.
				Expects 'plant' to be a dictionary:
			- new_bot_name = botanical name to add or replace
			- rename = replace the existing botanical name
			- pid = plant id
			- full_name = the full name for the plant (for debug purposes)
			- common_exclude = list of common names to exclude

		Returns:
			- True if name exists or proposal approved
			- False if proposal submitted but not approved
			- None otherwise"""

		# Prepare the url
		url = self._plant_name_url % plant['pid']

		try:
			r = self._session.get(url)
		except requests.exceptions.RequestException as e:
			print("Error retrieving NGA database name page for %s." % plant['full_name'])
			print(str(e))
			return None
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(r.text, "lxml")
			form = soup.find('form', attrs={'method': 'post'})
			data = OrderedDict() # This is crucial. New fields are processed server-side in the order that they are added.

			# Latin names
			latin_table = form.find('table', attrs={'id': 'latin-table'})
			lnames = latin_table.findAll(['input','select'])

			lparams = OrderedDict() # Must be ordered!

			# Iterate through all the botanical names and make sure existing entries are preserved
			for i in lnames:
				# Extract the id number for this latin name
				name = i['name']
				name_id = re.sub('[^0-9]','',name)
				if name_id not in lparams:
					lparams[name_id] = {}

				# Get the latin name
				if 'status' not in name:
					lparams[name_id]['latin'] = i['value']

					# Check if the synonym is already present
					# TO DO: Check for misspellings in future?
					if synonym in i['value'].strip():
						# Don't need to continue as the synonym is already there
						return True

				# Get the current status of the name
				else:
					selector = i.findAll('option', selected=True)
					svalue = selector[0]['value']
					lparams[name_id]['latin_status'] = svalue

			# Add the new name as a synonym
			lparams['new'] = {'latin':synonym, 'latin_status':'synonym'}

			# Add the latin names to the object
			for lp in lparams:
				if lp == 'new':
					data['latin[]'] = lparams[lp]['latin']
					data['latin_status[]'] = lparams[lp]['latin_status']
				else:
					data['latin[%s]' % lp] = lparams[lp]['latin']
					data['latin_status[%s]' % lp] = lparams[lp]['latin_status']

			# Common names
			common_table = form.find('table', attrs={'id': 'common-table'})
			cnames = common_table.findAll('input')

			cname_exclude = None
			if 'common_exclude' in plant:
				cname_exclude = plant['common_exclude'].strip().lower()

			if len(cnames) < 1:
				# If there are no common names and one has been provided, add it
				if common_name is not None:
					data['common[]'] = common_name
			else:
				# Prepare data for common name validation
				synonym_genus = None
				common_found = False
				if common_name is not None:
					common_lower = common_name.lower()
				else:
					common_lower = None

				synonym_genus = synonym.split(' ')[0].strip().lower()

				# Cycle through the existing common names and ensure they are included
				# (unless they are the genus)
				for c in cnames:
					common_tidied = c['value'].strip().lower()

					# Check if the common name is already present
					if common_lower == common_tidied:
						common_found = True

					# If the common name isn't the genus, copy it
					if common_tidied not in (synonym_genus, cname_exclude):
						data[c['name']] = c['value']

				# If the provided common name wasn't listed, add it
				if not common_found and common_name is not None:
					data['common[]'] = common_name

			# Tradename and series
			trade_table = form.find('table', attrs={'id': 'tradename-table'})
			trade_data = trade_table.findAll('input')

			# Copy any existing trade name data
			for t in trade_data:
				if t['name'] in 'tradename' and len(t['value'].strip()) < 1 and 'remove_quotes' in plant and plant['remove_quotes']:
					data[t['name']] = plant['cleaned_name']
				else:
					data[t['name']] = t['value']

			# Cultivars
			cultivar_table = form.find('table', attrs={'id': 'cultivar-table'})
			cultivars = cultivar_table.findAll('input')

			# Copy any existing cultivars
			for c in cultivars:
				data[c['name']] = c['value']

			# Also sold as
			asa_table = form.find('table', attrs={'id': 'asa-table'})
			aliases = asa_table.findAll('asa')

			# Copy any existing aliases
			for a in aliases:
				data[a['name']] = a['value']

			data['submit'] = 'Submit your proposed changes'

			# POST the data
			return self.__submitProposal(url, data, auto_approve)


	def proposeNameChange(self, plant, common_name=None, auto_approve=True):
		"""Propose a change to the name of a plant in the NGA database.
		Expects 'plant' to be a dictionary:
			- new_bot_name = botanical name to add or replace
			- rename = replace the existing botanical name
			- pid = plant id
			- full_name = the full name for the plant (for debug purposes)
			- common_exclude = list of common names to exclude

		Returns:
			- True if name exists or proposal approved
			- False if proposal submitted but not approved
			- None otherwise"""

		# Prepare the url
		url = self._plant_name_url % plant['pid']

		try:
			r = self._session.get(url)
		except requests.exceptions.RequestException as e:
			print("Error retrieving NGA database name page for %s." % plant['full_name'])
			print(str(e))
			return None
		else:
			# Parse the returned HTML
			soup = BeautifulSoup(r.text, "lxml")
			form = soup.find('form', attrs={'method': 'post'})
			data = OrderedDict() # This is crucial. New fields are processed server-side in the order that they are added.

			# Latin names
			latin_table = form.find('table', attrs={'id': 'latin-table'})
			lnames = latin_table.findAll(['input','select'])

			lparams = OrderedDict() # Must be ordered!
			accepted = None
			accepted_name = None

			for i in lnames:
				# Extract the id number for this latin name
				name = i['name']
				name_id = re.sub('[^0-9]','',name)
				if name_id not in lparams:
					lparams[name_id] = {}

				# Get the latin name
				if 'status' not in name:
					lparams[name_id]['latin'] = i['value']

				# Get the current status of the name
				else:
					selector = i.findAll('option', selected=True)
					svalue = selector[0]['value']
					lparams[name_id]['latin_status'] = svalue

					if 'accepted' in svalue.lower():
						accepted = name_id
						accepted_name = lparams[name_id]['latin']

			# Either replace the name (fix spelling) or add the new name as the accepted one
			if accepted is not None:

				# There is a new name for the plant
				if 'new_bot_name' in plant:

					# The name is misspelt or needs to be replaced
					if 'rename' in plant:
						lparams[accepted]['latin'] = plant['new_bot_name']

					# The existing name is now a synonym
					else:
						lparams[accepted]['latin_status'] = 'synonym'
						found = False

						# Check that the newly accepted name isn't a synonym already
						for lp in lparams:
							lentry = lparams[lp]
							if lentry['latin'] == plant['new_bot_name']:
								lentry['latin_status'] = 'accepted'
								found = True
								break

						if not found:
							lparams['new'] = {'latin':plant['new_bot_name'], 'latin_status':'accepted'}

			# Add the latin names to the object
			for lp in lparams:
				if lp == 'new':
					data['latin[]'] = lparams[lp]['latin']
					data['latin_status[]'] = lparams[lp]['latin_status']
				else:
					data['latin[%s]' % lp] = lparams[lp]['latin']
					data['latin_status[%s]' % lp] = lparams[lp]['latin_status']

			# Common names
			common_table = form.find('table', attrs={'id': 'common-table'})
			cnames = common_table.findAll('input')

			cname_exclude = None
			if 'common_exclude' in plant:
				cname_exclude = plant['common_exclude'].strip().lower()

			if len(cnames) < 1:
				# If there are no common names and one has been provided, add it
				if common_name is not None:
					data['common[]'] = common_name
			else:
				# Prepare data for common name validation
				accepted_genus = None
				common_found = False
				if common_name is not None:
					common_lower = common_name.lower()
				else:
					common_lower = None

				if accepted_name is not None:
					accepted_genus = accepted_name.split(' ')[0].strip().lower()

				# Cycle through the existing common names and ensure they are included
				# (unless they are the genus)
				for c in cnames:
					common_tidied = c['value'].strip().lower()

					# Check if the common name is already present
					if common_lower == common_tidied:
						common_found = True

					# If the common name isn't the genus, copy it
					if common_tidied not in (accepted_genus, cname_exclude):
						data[c['name']] = c['value']

				# If the provided common name wasn't listed, add it
				if not common_found and common_name is not None:
					data['common[]'] = common_name

			# Tradename and series
			trade_table = form.find('table', attrs={'id': 'tradename-table'})
			trade_data = trade_table.findAll('input')

			# Copy any existing trade name data
			for t in trade_data:
				if t['name'] in 'tradename' and len(t['value'].strip()) < 1 and 'remove_quotes' in plant and plant['remove_quotes']:
					data[t['name']] = plant['cleaned_name']
				else:
					data[t['name']] = t['value']

			# Cultivars
			cultivar_table = form.find('table', attrs={'id': 'cultivar-table'})
			cultivars = cultivar_table.findAll('input')

			# Copy any existing cultivars
			for c in cultivars:
				data[c['name']] = c['value']

			# Also sold as
			asa_table = form.find('table', attrs={'id': 'asa-table'})
			aliases = asa_table.findAll('asa')

			# Copy any existing aliases
			for a in aliases:
				data[a['name']] = a['value']

			data['submit'] = 'Submit your proposed changes'

			# POST the data
			return self.__submitProposal(url, data, auto_approve)


	def proposeDataUpdate(self, plant, genus=None):
		"""Propose an update to a plant's data fields. Currently only adds parentage.
		Expects 'X' to be used to denote crosses."""

		# Make sure we have valid data before continuing
		if 'parentage_exists' in plant and 'parentage' in plant:
			if plant['parentage_exists'] or plant['parentage'] is None or plant['parentage']['violates_rules']:
				return None

			# Prepare the url
			url = self._plant_data_url % plant['pid']

			try:
				r = self._session.get(url)
			except requests.exceptions.RequestException as e:
				print("Error retrieving NGA database databox page for %s." % plant['full_name'])
				print(str(e))
				return None
			else:
				# Parse the returned HTML
				soup = BeautifulSoup(r.text, "lxml")
				form = soup.find('form', attrs={'method': 'post'})
				data = OrderedDict() # This is crucial. New fields are processed server-side in the order that they are added.

				# Ensure all the existing values are kept
				table = form.find('table')
				fields = table.findAll(['input','select'])
				for f in fields:
					data[f['name']] = '' # Default is blank

					# Handle checkboxes
					if 'type' in f.attrs:
						if f['type'] == 'checkbox' and 'checked' in f.attrs:
							data[f['name']] = 'on'
						elif 'value' in f.attrs:
							data[f['name']] = f['value']

					# Handle dropdown options
					elif f.name == 'select':
						for option in f.children:
							if 'selected' in option.attrs:
								data[f['name']] = option['value']

				# Locate the parentage field
				parentage_cell = table.find(string=re.compile('Parentage')) # Due to the fact that this is next to a span, BS4 considers this a text node, so searching for the parent doesn't work in this case
				if parentage_cell is not None:
					parentage_field = parentage_cell.parents

					# Walk up through the parents until we hit the TR tag
					for p in parentage_field:
						if p.name == 'tr':
							# Locate the input element
							input_field = p.find('input')
							if input_field is not None:
								# Update the parentage field
								data[(input_field['name'])] = plant['parentage']['formula']
								break

				# Normally the page only offers a preview option first, but this value should skip that step
				data['submit'] = 'Save and submit the proposal'

				# POST the data and automatically approve the proposal
				self.__submitProposal(url, data)


	def proposeMerge(self, old_plant, new_plant, reversed=False, auto_approve=True):
		"""Propose the merge of the old plant into the new plant. Ensures that the
		name of the old plant is copied across to the new one as a synonym."""

		if reversed:
			# Update the name of the old plant, since this entry will be kept
			# This will fix misspellings and set the new accepted name if required
			name_update = self.proposeNameChange(old_plant, auto_approve=auto_approve)
		else:
			if not ('rename' in old_plant and old_plant['rename']):
				# Update the name of the new entry, adding the old as a synonym
				name_update = self.proposeSynonymAddition(new_plant, old_plant['full_name'], auto_approve=auto_approve)
			else:
				# Old entry is just a misspelling, so we don't need to keep the name
				name_update = True

		# If the name update was successful or the names are correct, propose the merge
		if name_update:
			if new_plant['pid'] == old_plant['pid']:
				print("Error: Unable to merge a plant with itself.")
				return

			if reversed:
				# New entry will be merged into old
				url = self._merge_plant_url % new_plant['pid']
				pid = old_plant['pid']
			else:
				# Old entry will be merged into new
				url = self._merge_plant_url % old_plant['pid']
				pid = new_plant['pid']

			params = {
				'newpid': pid,
				'submit':'Submit the proposal'
			}

			# POST the data
			self.__submitProposal(url, params, auto_approve=auto_approve)


def testModule(cookiepath=None):
	"""A simple test to check that all functions are working correctly."""

	# If there is a specified path to the cookie file, use it
	if cookiepath is not None:
		global NGA_COOKIE
		NGA_COOKIE = cookiepath

	myNGA = NGA()
	results = myNGA.fetchGenus('Cymbidium')
	print(results.keys())

	print("Testing for invalid entry...")
	searchResults = myNGA.search('Cymbidium xyz')
	if searchResults is None:
		print("Success.")
	else:
		print(searchResults)

	print("Testing for valid entry...")
	searchResults = myNGA.search('Cymbidium lowianum')
	print(searchResults)