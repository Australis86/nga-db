#! /usr/bin/python

"""Module for searching the KEW Word Checklist of Selected Plant Families (WCSP).

This script is designed for Python 3 and Beautiful Soup 4 with the lxml parser."""

__version__ = "1.1"
__author__ = "Joshua White"
__copyright__ = "Copyright 2019"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

# Module imports
import re
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


class WCSP:
	"""Create a user-friendly API for the KEW World Checklist of Selected Plants website."""

	def __init__(self):
		"""Create an instance and set up a requests session to the KEW WCSP."""

		self._home_url = 'http://wcsp.science.kew.org/home.do'
		self._search_url = 'http://wcsp.science.kew.org/qsearch.do'
		self._hybrid_symbol = '×'

		self._session = requests.Session()
		self._session.get(self._home_url)


	def nameSearch(self, synonym):
		"""Search the WCSP for a given name and return the current accepted name.
		Will return a dict with name and status to indicate if a name is unplaced."""

		def parseItalics(italics, link):
			"""Method to parse the italics in the name."""

			hybrid = False

			if len(italics) == 2:
				# This includes hybrid taxa; the hybrid symbol will be at the start of one of the two fields
				fields = [el.text.strip() for el in italics]

				# Check for a hybrid symbol; in this case it will be at the start of the field
				for idx, field in enumerate(fields):
					if field[1] == ' ':
						fields[idx] = fields[idx][2:]
						hybrid = True

			else:
				# Must be a subspecies or variety
				acceptednav = link.text
				acceptedname = acceptednav.split(' ')[:len(italics)+1]
				fields = [el.strip() for el in acceptedname]

				# Check for the hybrid symbol; in this case it will be a field with length 1
				for idx, field in enumerate(fields):
					if len(field) == 1:
						fields.pop(idx)
						hybrid = True
						break

			return (' '.join(fields), hybrid)


		def checkStatus(genus, soup, result):
			"""Method to check the status of an entry."""

			is_accepted = soup.find('p', string=re.compile('This name is accepted'))
			is_unplaced = soup.find('p', string=re.compile('This name is unplaced'))
			distribution = soup.find('th', string=re.compile('Distribution:'))
			formula = soup.find('th', string=re.compile('Hybrid Formula:'))

			# Check if this is an accepted name
			if is_accepted is not None:
				result['status'] = 'Accepted'
			elif is_unplaced is not None:
				result['status'] = 'Unplaced'

			# Check if there is a valid distribution
			if distribution is not None:
				dist_parent = distribution.parent
				location = dist_parent.find('td')
				if location is not None:
					lines = []
					for loc_text in location.stripped_strings:
						rows = loc_text.split('\n')
						rows = [row.strip() for row in rows]
						lines += rows

					result['distribution'] = '\n'.join(lines)

			# Check if there is a valid distribution
			if formula is not None:
				formula_parent = formula.parent
				parentage = formula_parent.find('td')
				if parentage is not None:

					# Get the parentage formula and remove the genus abbreviation
					parentage_formula = parentage.text.strip().replace(self._hybrid_symbol,'X')
					genus_abbrev = f'{genus[0]}.'
					#abbrev_count = parentage_formula.count(genus_abbrev)
					parentage_formula = parentage_formula.replace(genus_abbrev, genus)

					result['parentage'] = {
						'formula': parentage_formula,
						'intergeneric_hybrid_parents': False,
					}

			return result


		def findBotanicalName(genus, soup, result):
			"""Method to find the accepted botanical name in an entry."""

			# Check to see if this is the accepted name entry
			result = checkStatus(genus, soup, result)
			if result['status'] is not None:
				return result

			# Otherwise, look for the field pointing to the new accepted name
			links = soup.findAll('a', {'class':'acceptednav'})

			# Look for the botanical name
			if len(links) > 0:
				italics = links[0].findAll('i')
				if italics is not None:
					result['status'] = 'Accepted'
					(name, hybrid) = parseItalics(italics, links[0])
					result['name'] = name
					result['hybrid'] = hybrid
					return result

			return None

		data = {'plantName': synonym, 'page': 'quickSearch'}
		result = {'name': synonym, 'status': None, 'distribution': None, 'hybrid': False, 'parentage': None}
		genus = synonym.split(' ')[0]

		try:
			req = self._session.post(self._search_url, data=data)
		except requests.exceptions.RequestException:
			return result
		else:
			# Parse the response HTML here and check for an accepted name
			soup = BeautifulSoup(req.text, "lxml")
			status = findBotanicalName(genus, soup, result)
			if status is not None:
				return status

			# Check for multiple search results
			links = soup.findAll('a', {'class':'onwardnav'})
			for link in links:
				italics = link.findAll('i')
				if italics is not None:
					(name, hybrid) = parseItalics(italics, link)

					# Select the entry that matches our search term
					if name == synonym:
						result['hybrid'] = hybrid
						href = link['href']
						url = urljoin(self._search_url, href)

						try:
							req = self._session.get(url)
						except requests.exceptions.RequestException as err:
							print(str(err))
							return result
						else:
							# Parse the response HTML here and check for an accepted name
							soup = BeautifulSoup(req.text, "lxml")
							status = findBotanicalName(genus, soup, result)
							if status is not None:
								return status

		return result


def testModule(synonym='Cymbidium iansonii'):
	"""A simple test to check that all functions are working correctly."""

	my_kew = WCSP()
	print(synonym, '->', my_kew.nameSearch(synonym))
