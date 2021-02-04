#! /usr/bin/python

"""This script is intended to aid in checking and updating the contents of a 
genus in the NGA database."""

__version__ = "2.0"
__author__ = "Joshua White"
__copyright__ = "Copyright 2019"
__email__ = "jwhite88@gmail.com"
__licence__ = "GNU Lesser General Public License v3.0"

import os
import sys
import argparse
import sqlite3
import re
try:
	import Levenshtein
except ImportError as err:
	Levenshtein = None
	print("Levenshtein module not installed. Spellchecking will not be available.")
import nga # Custom module for NGA and other resources

# Absolute path to the script's current directory
PATH = os.path.dirname(os.path.abspath(__file__))


def initMenu():
	"""Initialise the command-line parser."""
	
	parser = argparse.ArgumentParser()
	
	parser.add_argument("-g", "--genus", help="genus name", required=True)
	parser.add_argument("-p", "--propose", help="automatically propose changes to the NGA database",
		action="store_true")
	
	# The orchid flag and the common name flag conflict, so we add them to a mutually exclusive group
	group = parser.add_mutually_exclusive_group()
	group.add_argument("-o", "--orchids", help="enable orchid-specific features (overrides common name)",
		action="store_true")
	group.add_argument("-n", "--name", help="use the specified common name for all members of the genus")
	
	parser.add_argument("-c", "--cache", help="specify a cache directory (default is ./nga_cache)", 
		default=os.path.join(PATH, 'nga_cache'), metavar="DIR")
	parser.add_argument("--parentage", help="check the parentage fields for hybrids", 
		action="store_true", default=False)
	
	# TO DO: Add functionality for authenticating with the NGA website
	
	return parser.parse_args()


def checkSynonym(nga_dataset, search_obj, search_term, nga_hyb_status=None):
	"""Check a synonym in a remote database."""
	
	# Remove the hybrid symbol, as it isn't used by the COL and KEW uses the proper symbol rather than an x
	if nga_hyb_status is None:
		nga_hyb_status = ' x ' in search_term
	
	non_hyb_search_term = search_term.replace(' x ',' ')
	results = search_obj.search(non_hyb_search_term)
	current_names = nga_dataset.keys()
	duplicate = results in current_names

	# Only update if it's not the same and the result is valid
	if (results != search_term and results != non_hyb_search_term) and results is not None:
		params_st = len(search_term.split())
		params_an = len(results.split())
				
		# Exclude results where the result matches the search term or the search term is only part of the result
		if params_an > params_st and search_term in results:
			print("\tWarning: COL may be incomplete --",search_term,'->',results)
		else:
			# Update the dictionary
			for cultivar in nga_dataset[search_term]:
				nga_dataset[search_term][cultivar]['new_bot_name'] = results
				nga_dataset[search_term][cultivar]['changed'] = True
				nga_dataset[search_term][cultivar]['duplicate'] = duplicate
				
				if nga_hyb_status:
					nga_dataset[search_term][cultivar]['warning'] = True
					nga_dataset[search_term][cultivar]['warning_desc'] = 'NGA-listed natural hybrid is now a synonym'
			
	return (results, duplicate)


def checkBotanicalEntries(genus, dca_db, nga_dataset, entries, nga_db=None, orchid_extensions=False):
	"""Compare the botanical entries in the NGA database with the DCA dataset."""
	
	def checkHybStatus(desc, distribution=None):
		'''Check for hybrid status.'''
		desc_l = desc.lower()
		col_hyb = False
		col_hyb_q = False
		nat_hyb = False
		
		if 'hybrid' in desc_l or 'hyrbrid' in desc_l:
			col_hyb = True
			col_hyb_q = '?' in desc_l
			
			# If a distribution is provided, it's a natural hybrid
			nat_hyb = distribution is not None and distribution != ''
			
		return (col_hyb, col_hyb_q, nat_hyb)
	
	# Create objects for future use
	COLengine = nga.COL.COL()
	KEWengine = nga.KEW.WCSP()
	
	# Check if genus is a hybrid genus or not
	hybrid_genus = False
	if orchid_extensions:
		genus_info = KEWengine.nameSearch(genus)
		if genus_info['hybrid']:
			if genus_info['parentage'] is not None:
				print("%s is a hybrid genus (%s)" % (genus, genus_info['parentage']['formula']))
			else:
				print("%s is a hybrid genus" % (genus))
			hybrid_genus = True
		
	# Storage for list of name changes to avoid double-ups when checking for missing accepted names
	updated_names = []
	
	# Open a connection to the SQLite database
	if dca_db is not None:
		conn = sqlite3.connect(dca_db)
		cur = conn.cursor()

	# Iterate through the botanical names
	bn = len(entries)
	x = 0
	
	for botanical_name in entries:
		x += 1
		percentage = 100.0*(x/bn)
		sys.stdout.write('\rChecking NGA botanical entries... %00.1f%%' % percentage)
		sys.stdout.flush()
		
		# Hybrid flags
		nga_hyb = False
		col_hyb = False
		col_hyb_q = False
		nat_hyb = False
		
		# This bit of code should never be required
		# if '' not in nga_dataset[botanical_name]:
			# print(nga_dataset[botanical_name].keys())
			# continue
		
		for cultivar in nga_dataset[botanical_name]:
			nga_dataset[botanical_name][cultivar]['changed'] = False # Default value
			nga_dataset[botanical_name][cultivar]['warning'] = False # Default value
		
		if '' not in nga_dataset[botanical_name]:
			full_name = botanical_name
		else:
			botanical_entry = nga_dataset[botanical_name]['']
			full_name = botanical_entry['full_name']
		
		# Split up the botanical name
		fields = full_name.split()
		if 'x' in fields:
			fields.remove('x') # Remove the hybrid flag, as the COL doesn't use this
			nga_hyb = True
		fn = len(fields)
		
		if dca_db is not None:
			# Prepare the SQL query
			if fn == 2:
				sql = "SELECT t.taxonomicStatus, d.locality, t.taxonRemarks FROM Taxon t LEFT JOIN Distribution d ON t.taxonID=d.taxonID WHERE genericName=? AND specificEpithet=? AND taxonRank='SPECIES' AND (infraspecificEpithet='' OR infraspecificEpithet IS NULL) GROUP BY taxonomicStatus ORDER BY taxonomicStatus LIMIT 1"
				params = (fields[0], fields[1])
			elif fn == 4:
				sql = "SELECT t.taxonomicStatus, d.locality, t.taxonRemarks FROM Taxon t LEFT JOIN Distribution d ON t.taxonID=d.taxonID  WHERE genericName=? AND specificEpithet=? AND taxonRank=? AND infraspecificEpithet=? GROUP BY taxonomicStatus ORDER BY taxonomicStatus LIMIT 1"
				params = (fields[0], fields[1], fields[2], fields[3])
			else:
				# Not sure what happened here...
				print(fields)
				break
			
			# Check the database for a result
			cur.execute(sql, params)
			results = cur.fetchall()
			
		else:
			results = []
		
		# If we have valid results...
		if len(results) > 0:
			status = results[0][0].lower()
			distribution = results[0][1]
			description = results[0][2]
			
			# Check if hybrid (make sure it's not in question)
			(col_hyb, col_hyb_q, nat_hyb) = checkHybStatus(description, distribution)
			
			# Name is accepted
			if 'accepted' in status:
				#print("Accepted",botanical_name)
				non_hyb_name = full_name.replace(' x ',' ')
				
				# Check if this is listed as a hybrid by the COL
				if col_hyb:
					
					# Check if it is a natural hybrid
					if nat_hyb and not col_hyb_q:
						
						# Variable for storing KEW data, should it be needed
						KEWresult = None
						hybrid_status = not nga_hyb and not hybrid_genus 
						
						# Update the name if this is supposed to be a natural hybrid but isn't listed as such
						if hybrid_status:
							hyb_name = full_name.replace(genus, '%s x' % genus)
							duplicate = hyb_name in entries
							
							if hyb_name not in updated_names:
								updated_names.append(hyb_name)
						
						# Iterate through the type entry and all selected clones
						for cultivar in nga_dataset[full_name]:
							nga_dataset[full_name][cultivar]['nat_hyb'] = True # Flag that this is a natural hybrid
							
							# Check if the name needs to be changed
							nga_dataset[full_name][cultivar]['changed'] = hybrid_status
							nga_dataset[full_name][cultivar]['rename'] = hybrid_status
							if hybrid_status:
								nga_dataset[full_name][cultivar]['new_bot_name'] = hyb_name
								nga_dataset[full_name][cultivar]['duplicate'] = duplicate
							
							# Check if the parentage field has been populated
							nga_dataset[full_name][cultivar]['parentage_exists'] = nga_db.checkParentageField(nga_dataset[full_name][cultivar])
							
							# No parentage data in database, so fetch parentage from KEW (orchids only)
							if orchid_extensions and not nga_dataset[full_name][cultivar]['parentage_exists']:
								
								# If there is no KEW data yet, retrieve it
								if KEWresult is None:
									KEWresult = KEWengine.nameSearch(non_hyb_name)
									
								# Add parentage information if available
								if KEWresult['parentage'] is not None:
									nga_dataset[full_name][cultivar]['parentage'] = KEWresult['parentage']
							
					# This has a question over its status and may be a hybrid
					elif col_hyb_q:
						for cultivar in nga_dataset[full_name]:
							nga_dataset[full_name][cultivar]['possible_hybrid'] = True
					
					# This may not be a natural hybrid
					else:
						if orchid_extensions:
							KEWresult = KEWengine.nameSearch(non_hyb_name)
							if KEWresult['distribution'] is not None:
								if not nga_hyb:
									if hybrid_genus:
										for cultivar in nga_dataset[full_name]:
											nga_dataset[full_name][cultivar]['nat_hyb'] = True
											nga_dataset[full_name][cultivar]['parentage_exists'] = nga_db.checkParentageField(nga_dataset[full_name][cultivar])
											if not nga_dataset[full_name][cultivar]['parentage_exists'] and KEWresult['parentage'] is not None:
												nga_dataset[full_name][cultivar]['parentage'] = KEWresult['parentage']
									else:
										hyb_name = full_name.replace(genus, '%s x' % genus)
										for cultivar in nga_dataset[full_name]:
											nga_dataset[full_name][cultivar]['new_bot_name'] = hyb_name
											nga_dataset[full_name][cultivar]['changed'] = True
											nga_dataset[full_name][cultivar]['rename'] = True
											nga_dataset[full_name][cultivar]['duplicate'] = hyb_name in entries
											nga_dataset[full_name][cultivar]['nat_hyb'] = True
											nga_dataset[full_name][cultivar]['parentage_exists'] = nga_db.checkParentageField(nga_dataset[full_name][cultivar])
											if not nga_dataset[full_name][cultivar]['parentage_exists'] and KEWresult['parentage'] is not None:
												nga_dataset[full_name][cultivar]['parentage'] = KEWresult['parentage']
											if hyb_name not in updated_names:
												updated_names.append(hyb_name)
							else:
								for cultivar in nga_dataset[full_name]:
									nga_dataset[full_name][cultivar]['not_nat_hybrid'] = not hybrid_genus
							
						else:
							for cultivar in nga_dataset[full_name]:
								nga_dataset[full_name][cultivar]['not_nat_hybrid'] = not hybrid_genus
						
				# Check for hybrids only listed on the NGA site
				elif nga_hyb:
				
					# Need to remove the hybrid symbol
					for cultivar in nga_dataset[full_name]:
						nga_dataset[full_name][cultivar]['new_bot_name'] = non_hyb_name
						nga_dataset[full_name][cultivar]['rename'] = True
						nga_dataset[full_name][cultivar]['changed'] = True
						nga_dataset[full_name][cultivar]['warning'] = True # We only want to warn/notify in this case
						nga_dataset[full_name][cultivar]['warning_desc'] = 'COL does not list this as a hybrid'
						if non_hyb_name not in updated_names:
							updated_names.append(non_hyb_name)
			
				for cultivar in nga_dataset[full_name]:
					nga_dataset[full_name][cultivar]['accepted'] = True
			
			# Not accepted
			elif 'synonym' in status:
				#print("Synonym",botanical_name)
				(new_bot_name, duplicate) = checkSynonym(nga_dataset, COLengine, full_name, nga_hyb)
				
				if new_bot_name is not None:
					if not duplicate and new_bot_name not in updated_names:
						updated_names.append(new_bot_name)
				else:
					# If it gets to here, then something went badly wrong with the search
					print("\tWarning: COL search failure -", full_name)
			
			# Illegal name?
			else:
				#print("Illegal",botanical_name)
				for cultivar in nga_dataset[full_name]:
					nga_dataset[botanical_name][cultivar]['warning'] = True # Default value
					nga_dataset[botanical_name][cultivar]['warning_desc'] = 'Illegal taxonomical name'
		
		# No match in DCA database or genus has been deprecated
		else:
			#print("Missing", botanical_name)
			
			# Remove hybrid symbol from the search term (COL doesn't use it and KEW uses a different one)
			search_name = full_name.replace(' x ', ' ')
			
			# Usually we only want to check the COL again if this entry isn't in the genus we're working on
			# But occasionally entries are missing from the DCA dataset (sigh)
			#if genus not in full_name:
			(accepted_name, duplicate) = checkSynonym(nga_dataset, COLengine, full_name, nga_hyb)
			
			if accepted_name is not None:
				if accepted_name != search_name and not duplicate and accepted_name not in updated_names:
					updated_names.append(accepted_name)
					
				# The COL check successfully found a match, so no need to check KEW or for misspellings
				continue
			
			# Check KEW database if we are looking at orchid genera
			if orchid_extensions:
				KEWresult = KEWengine.nameSearch(search_name)
				
				if KEWresult['status'] is not None:
					if KEWresult['name'] != search_name:
						# KEW database has a new name for the entry
						duplicate = KEWresult['name'] in entries
						
						for cultivar in nga_dataset[full_name]:
							nga_dataset[full_name][cultivar]['new_bot_name'] = KEWresult['name']
							nga_dataset[full_name][cultivar]['changed'] = True
							nga_dataset[full_name][cultivar]['duplicate'] = duplicate
							if KEWresult['status'] == 'Unplaced':
								nga_dataset[botanical_name][cultivar]['warning'] = True # Default value
								nga_dataset[botanical_name][cultivar]['warning_desc'] = 'Taxon is unplaced'
						
						if not duplicate and KEWresult['name'] not in updated_names:
							updated_names.append(KEWresult['name'])
						
					else:
						if KEWresult['status'] == 'Unplaced':
							for cultivar in nga_dataset[full_name]:
								nga_dataset[botanical_name][cultivar]['warning'] = True # Default value
								nga_dataset[botanical_name][cultivar]['warning_desc'] = 'Taxon is unplaced'

					# No need to check for misspellings
					continue
			
			# At this stage there has been no match in the COL or KEW databases, so check for misspellings
			# If the Levenshtein module is available, we can get the distance between an accepted species and the NGA entry
			# Allows us to check for typos/spelling mistakes
			if Levenshtein is not None and dca_db is not None:
				matched = False
				
				# Get the list of taxa
				sql = "SELECT genericName || ' ' || specificEpithet || ' ' || taxonRank || ' ' || infraspecificEpithet as epithet from Taxon GROUP BY epithet"
				cur.execute(sql)
				
				# Iterate through the names and compare to the entry from the NGA`
				closest_match = None
				last_ratio = 0
				
				for row in cur:
					nn = row[0].strip()
					ratio = Levenshtein.ratio(nn, search_name)
					
					if ratio > last_ratio:
						last_ratio = ratio
						closest_match = nn
				
				# Only accept nearest match if the ratio is high
				# Occasionally this can get it wrong!
				if last_ratio > 0.9:
					for cultivar in nga_dataset[full_name]:
						nga_dataset[full_name][cultivar]['new_bot_name'] = closest_match
						nga_dataset[full_name][cultivar]['rename'] = True
						nga_dataset[full_name][cultivar]['changed'] = True
						nga_dataset[full_name][cultivar]['duplicate'] = closest_match in entries # Ensure that the correct spelling doesn't already exist
						nga_dataset[full_name][cultivar]['warning'] = search_name != botanical_name # We only want to warn/notify in this case
						nga_dataset[full_name][cultivar]['warning_desc'] = 'Misspelt in NGA database'
						if closest_match not in updated_names:
							updated_names.append(closest_match)
						
					continue
			
			# If we reach this stage, no match has been found at all
			for cultivar in nga_dataset[full_name]:
				nga_dataset[full_name][cultivar]['warning'] = True
				nga_dataset[full_name][cultivar]['warning_desc'] = 'Invalid entry - not in online sources'
				
	sys.stdout.write('\rChecking NGA botanical entries... done.    \r\n')
	sys.stdout.flush()
	
	if dca_db is not None:
		# Check for missing accepted names
		sql = "SELECT genericName || ' ' || specificEpithet || ' ' || CASE WHEN taxonRank='FORM' THEN 'f.' WHEN taxonRank='VARIETY' THEN 'var.' WHEN taxonRank='SUBSPECIES' THEN 'subsp.' ELSE '' END || ' ' || infraspecificEpithet as epithet, locality, taxonRemarks from Taxon t LEFT JOIN Distribution d ON t.taxonID=d.taxonID WHERE taxonomicStatus='ACCEPTED' AND specificEpithet!='' GROUP BY epithet"
		cur.execute(sql)
		rows = cur.fetchall()

		# Close the database connection
		conn.close()
	
		# Iterate through all the accepted names
		print
		x = 0.0
		t = len(rows)
		nga_dataset_additions = []
		
		if nga_db is None:
			nga_db = nga.NGA.NGA()
		
		for row in rows:
			x += 1.0
			sys.stdout.write('\rChecking COL records... %00.1f%%' % (100.0*x/t))
			sys.stdout.flush()
			
			entry = row[0].strip() # Full botanical name
			entry_final = entry # Version of name to be added to list
			distribution = row[1] # Natural distribution
			description = row[2] # Descriptive notes
			check_nga = False
			
			# Check hybrid status
			(col_hyb, col_hyb_q, nat_hyb) = checkHybStatus(description, distribution)
			
			# If it's not a hybrid or a questionable one, check against the pending lists
			if not col_hyb or (col_hyb and col_hyb_q):
				if entry not in nga_dataset and entry not in updated_names:
					check_nga = True
					
			# Else check if it's a hybrid
			elif col_hyb and nat_hyb:
				hybrid_name = entry.replace(genus, '%s x' % genus)
				entry_final = hybrid_name # Change the new name to include the hybrid symbol
				
				# Check both with and without the hybrid symbol
				if hybrid_name not in nga_dataset and entry not in nga_dataset:
					check_nga = True
					
			else:
				# Only non-natural hybrids should reach this case
				pass
			
			# If this entry is missing from the NGA, check to see if it already exists as a synonym
			if check_nga:
				synonyms = COLengine.search(entry, True)
				synonym_entries = {}
				
				if synonyms is not None:
					for synonym in synonyms:
						dataset = nga_db.search(synonym)
						if dataset is not None and synonym in dataset:
							synonym_entries[synonym] = dataset[synonym]
							
							# Double-check to see if this is an accepted name; sometimes names are re-used
							synonym_check = COLengine.search(synonym)
							if synonym_check == synonym:
								for cultivar in synonym_entries[synonym]:
									synonym_entries[synonym][cultivar]['accepted'] = True
						
				# No match means we can add it
				# A match means that we need to check the nga_dataset and modify the object
				# If it's not in the same genus, we need to flag that
				valid_synonyms = False
				syn_count = len(synonym_entries)
				syn_duplicate = syn_count > 1
				if syn_count > 0:
					#print(synonym_entries)
					
					# Need to ensure that none of the synonyms are also accepted names
					# Just in case a name has multiple uses
					# For each synonym in the NGA database, ensure it is in the dataset
					for syn_entry in synonym_entries:
						if syn_entry not in nga_dataset:
							nga_dataset[syn_entry] = {}
						
						cultivars_nga = nga_dataset[syn_entry]
						cultivars_syn = synonym_entries[syn_entry]
						
						# Ensure that each cultivar is in the NGA dataset first
						for cultivar in cultivars_syn:
							if cultivar not in cultivars_nga:
								nga_dataset[syn_entry][cultivar] = synonym_entries[syn_entry][cultivar]
								
						# Update the botanical name status
						for cultivar in cultivars_nga:
							if 'accepted' not in cultivars_nga[cultivar]:
								cultivars_nga[cultivar]['changed'] = True
								cultivars_nga[cultivar]['duplicate'] = syn_duplicate
								cultivars_nga[cultivar]['new_bot_name'] = entry
								valid_synonyms = True
								#print("Synonym",entry,syn_entry)
				
				# If there were no valid synonyms (i.e. names in the database that weren't already 
				# as accepted names), add this name to the list of species to add to the database
				if not valid_synonyms:
					#print("No valid synonym", entry)
					nga_dataset_additions.append(entry_final)
		
		sys.stdout.write('\rChecking COL records... done. \r\n')
		sys.stdout.flush()
	
		nga_dataset['_additions'] = nga_dataset_additions
	
	return nga_dataset
	
	
def checkRegisteredOrchids(genera, nga_dataset, nga_db, parentage_check=False):
	"""Check the RHS register to ensure that hybrid entries are correct."""
	
	# Set up the connection to the RHS
	RHSengine = nga.RHS.Register()
	RHSengine.dbConnect(os.path.join(PATH,'RHS.db'))
	
	# Ensure we have a connection to the NGA database
	if nga_db is None:
		nga_db = nga.NGA.NGA()
	
	# Regular expression object for matching clonal names in entries
	cn = re.compile("\s'.*'$")
	
	# Iterate through all the genera (all the single-word keys)
	for genus in genera:
		hybrids = nga_dataset[genus]
		hybrid_names = list(hybrids.keys())
		hybrid_names.sort()
		
		if '' in hybrid_names:
			hybrid_names.remove('')
		hybrid_count = len(hybrid_names)
		h = 0
		
		sys.stdout.write("\rChecking hybrids in genus %s..." % genus)
		sys.stdout.flush()
		
		# Check each hybrid in this genus
		for hybrid in hybrid_names:
			h +=1 
			sys.stdout.write("\rChecking hybrids in genus %s... %d/%d" % (genus, h, hybrid_count))
			sys.stdout.flush()
			quotes = hybrid.count("'") # Get the number of quotes in the name
			hybrids[hybrid]['has_quotes'] = False
			
			if quotes < 2: # Simplest case - just a grex
				grex = hybrid
			else:
				# Might be a mis-entered grex or a grex with clonal name
				m = cn.search(hybrid)
				if m is not None:
					grex = hybrid[:m.start()]
				else:
					# This is probably a grex wrapped in quotes or a clonal name without a grex
					cleaned = hybrid.strip("'")
					if len(cleaned) == (len(hybrid)-2):
						grex = cleaned
						hybrids[hybrid]['has_quotes'] = True
						hybrids[hybrid]['cleaned_name'] = cleaned
					else:
						grex = hybrid
						
			# Remove whitespace
			grex = grex.strip()
			
			# Search the RHS for the entry
			r = RHSengine.search(genus, grex)
			
			if r is None:
				print("Error retrieving RHS results for",genus,grex)
			
			else:
				# Update the object in the dataset
				hybrids[hybrid]['registered'] = r['matched']
				hybrids[hybrid]['parentage'] = None
				
				# Populate parentage field
				if r['matched']:
					hybrids[hybrid]['remove_quotes'] = hybrids[hybrid]['has_quotes']
					hybrids[hybrid]['parentage'] = nga_db.formatParentage(genus, r)
					
					if parentage_check:
						# Check if the NGA page has the parentage field populated
						hybrids[hybrid]['parentage_exists'] = nga_db.checkParentageField(hybrids[hybrid])
		
		sys.stdout.write("\rChecking hybrids in genus %s... done.          \r\n" % genus)
		sys.stdout.flush()
		
	# Close the database before return the results
	RHSengine.dbClose()
	
	return nga_dataset
	
	
def compareDatasets(genus, dca_db, nga_dataset, nga_db=None, orchid_extensions=False, parentage_check=False):
	"""Compare the current DCA dataset to the current NGA dataset."""
	
	# Ensure the genus name is properly formatted
	genus = genus.strip().title()
	
	# Hybrids will be stored under the genus name, whilst species will have their own entries
	entries = list(set(list(nga_dataset.keys())))
	entries.sort()
	counts = {}
	
	# Identify which entries are single words (i.e. genera) and which are not (i.e. species)
	for x in entries:
		wc = len(x.split())
		
		if wc not in counts:
			counts[wc] = []
		
		counts[wc].append(x)
	
	# Remove the genera-level entries from the list so that they are not processed in the botanical comparison function
	if 1 in counts:
		genera = counts[1]
		genera.sort()
		
		if len(genera) > 1:
			print("Identified Genera:", ', '.join(genera))
		
		for x in genera:
			entries.remove(x)
	else:
		print("Missing genus-level entry for",genus)
		genera = []
	
	# Compare datasets
	nga_dataset = checkBotanicalEntries(genus, dca_db, nga_dataset, entries, nga_db, orchid_extensions)
	
	if orchid_extensions and len(genera) > 0:
		nga_dataset = checkRegisteredOrchids(genera, nga_dataset, nga_db, parentage_check)
	
	return (nga_dataset, genera)
	
	
def processDatasetChanges(genera, nga_dataset, nga_db=None, common_name=None, propose=False):
	"""Display the pending changes to the NGA database and implement them if allowed."""
	
	# Hybrids will be stored under the genera, whilst species will have their own entries
	entries = list(set(list(nga_dataset.keys())))
	entries.sort()
	
	# Set aside hybrids
	hybrids = {}
	for genus in genera:
		hybrids[genus] = nga_dataset[genus]
		entries.remove(genus)
	
	# Set aside new accepted names
	if '_additions' in entries:
		additions = nga_dataset['_additions']
		entries.remove('_additions')
	else:
		additions = []
	
	# Check if there are NGA entries
	if len(entries) > 0:
		
		# Work through existing entries first
		print("Processing botanical entries...", os.linesep,
			"W = Warning (no action taken)", os.linesep, 
			"MP = Missing parentage information (hybrids only)", os.linesep, 
			"NH = Not listed as natural hybrid in the COL", os.linesep, 
			"PH = Listed as a possible hybrid in the COL", os.linesep,
			"CN = Has the genus as the common name", os.linesep,
			"MC = Missing a common name", os.linesep)
		
		for botanical_name in entries:
			botanical_entry = nga_dataset[botanical_name]
			
			# Iterate through the selected clones of this botanical entry
			for selection_name in botanical_entry:
				selection_entry = botanical_entry[selection_name]
				full_name = selection_entry['full_name']
				update_selected_name = False
				update_selected_data = False
				
				# Check parentage field for natural hybrids
				if 'nat_hyb' in selection_entry and not selection_entry['parentage_exists']:
					if 'parentage' in selection_entry and selection_entry['parentage'] is not None:
						print('MP  ',botanical_name,'(%s)' % selection_entry['parentage']['formula'])
						update_selected_data = True
					else:
						print('MP  ',botanical_name)
				
				# Flag an update to the database entry if the common name needs changing
				if not selection_entry['warning']:
					if selection_entry['common_name']:
						print('CN  ',botanical_name)
						update_selected_name = True
					elif selection_entry['common_name'] is None and common_name is not None:
						print('MC  ',botanical_name)
						update_selected_name = True
				
				# Check if the botanical name field needs updating
				if 'changed' in selection_entry and selection_entry['changed']:
					if selection_entry['warning']:
						print('W   ', botanical_name, '->', selection_entry['new_bot_name'], '(%s)' % selection_entry['warning_desc'])
					else:
						msg = ''
						if 'duplicate' in selection_entry and selection_entry['duplicate']:
							msg = "(New name already exists in NGA database)"
						else:
							update_selected_name = True
						print('    ', botanical_name, '->', selection_entry['new_bot_name'], msg)
				
				# Warn only if it's not a natural hybrid or might be a natural hybrid
				elif 'not_nat_hybrid' in selection_entry and selection_entry['not_nat_hybrid']:
					print('NH  ', full_name)
				elif 'possible_hybrid' in selection_entry and selection_entry['possible_hybrid']:
					print('PH  ', full_name)
				
				# Otherwise print any warnings
				elif selection_entry['warning']:
					print('W   ', botanical_name, '(%s)' % selection_entry['warning_desc'])
				
				# Propose changes
				if propose:
					if update_selected_name:
						nga_db.proposeNameChange(selection_entry, common_name)
					if update_selected_data:
						nga_db.proposeDataUpdate(selection_entry)
		
		
	# Add any missing accepted names
	if len(additions) > 0:
		print("\nAccepted names missing from database:")
		
		for new_name in additions:
			print("    ",new_name)
			if propose:
				nga_db.proposeNewPlant(new_name, common_name)
	
	# Check hybrids
	genera_count = len(genera)
	if genera_count > 0:
		print("\nProcessing hybrid entries...", os.linesep,
		"W = Warning (no action taken)", os.linesep, 
		"MP = Missing parentage information (hybrids only)", os.linesep, 
		"NR = Not a registered hybrid", os.linesep,
		"Q = Registered grex name incorrectly wrapped in quotes", os.linesep)
	
	for genus in genera:
		hybrid_names = list(hybrids[genus].keys())
		
		# Exclude the genus entry
		if '' in hybrid_names:
			hybrid_names.remove('')
			
		if len(hybrid_names) > 0:

			# Only print out genus name if there's more than one
			if genera_count > 1:
				print('-',genus)
			
			# Iterate through all the hybrids in this genus
			for hybrid in hybrid_names:
				hybrid_entry = hybrids[genus][hybrid]
				update_hybrid_name = False
				update_hybrid_data = False
				
				if hybrid_entry['common_name']:
					update_hybrid_name = True
					
				if 'remove_quotes' in hybrid_entry and hybrid_entry['remove_quotes']:
					update_hybrid_name = True
					print('Q   ', hybrid)
				
				# Hybrid entries that aren't registered
				if 'registered' in hybrid_entry and not hybrid_entry['registered']:
					print('NR  ', hybrid)
				
				# Hybrid entries with missing parentage information
				elif 'parentage_exists' in hybrid_entry and not hybrid_entry['parentage_exists'] and hybrid_entry['parentage'] is not None and not hybrid_entry['parentage']['violates_rules']:
					update_hybrid_data = True
					print('MP  ', hybrid, '--', hybrid_entry['parentage']['formula'])
				
				# Update the hybrid entry as required
				if propose:
					if update_hybrid_name:
						nga_db.proposeNameChange(hybrid_entry, common_name)
					if update_hybrid_data:
						nga_db.proposeDataUpdate(hybrid_entry, genus)
	
	
def main(namespace_args):
	"""Take action based on the command-line options."""
	
	# Ensure the cache directory exists
	try:
		os.makedirs(args.cache)
	except FileExistsError as e:
		pass
	
	# Fetch latest genus data from the Darwin Core Archive
	darwin_core = nga.COL.DCA()
	darwin_core.setCache(args.cache)
	dca_cache = darwin_core.fetchGenus(args.genus)
	
	#if dca_cache is None:
	#	print("Error: unable to continue without current genus dataset.")
	#	sys.exit(1)
	
	# Fetch current NGA database list
	nga_db = nga.NGA.NGA()
	nga_dataset = nga_db.fetchGenus(args.genus)
	
	if nga_dataset is None:
		print("Error: unable to continue without NGA dataset.")
		sys.exit(1)
	
	# Compare the datasets
	(nga_dataset, genera) = compareDatasets(args.genus, dca_cache, nga_dataset, nga_db, args.orchids, args.parentage)
	
	# For Orchids, set the common name
	if args.orchids:
		common_name = 'Orchid'
	else:
		common_name = args.name
	
	# Display the recommended changes and implement them
	processDatasetChanges(genera, nga_dataset, nga_db, common_name, args.propose)


if __name__ == '__main__':
	args = initMenu()
	main(args)
