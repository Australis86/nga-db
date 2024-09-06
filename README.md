# National Gardening Association Plant Database Management Scripts

This repository is a collection of scripts for working with the NGA Database.

## Required Accounts

Use of these scripts requires accounts to enable access to datasets and for modification of the NGA Plant Database.

- [NGA](https://garden.org/) account required for proposing changes (admin account required for automatically accepting proposals)
- [GBIF](https://www.gbif.org/) account required for use of the Catalogue of Life API

On the first execution of `genusCheck.py`, it will check for two files - an NGA session cookie (`~/.nga`) and a GBIF authentication file (`~/.gbif`). If either are not found, it will prompt the user to log in with their account details. Custom paths for these files can be specified by setting passing a path to `NGA()` and `GBIF()` upon instantiation respectively.

## Dependencies

- Python 3.6+ with the following modules
  - Beautiful Soup 4 (bs4)
  - lxml (for BS4 parser)
  - Levenshtein (python-levenshtein)
  - requests
  - titlecase

Some of the additional sample scripts require pandas, numpy and openpyxl.

## Acknowledgements

These scripts rely on a number of online resources. The work by those who maintain the following projects is greatly appreciated:

- [Catalogue of Life](https://www.catalogueoflife.org/) and the [COL API Backend](https://github.com/CatalogueOfLife/backend/)
- [KEW World Checklist of Selected Plants](https://wcsp.science.kew.org/)
- [RHS Orchid Register](https://apps.rhs.org.uk/horticulturaldatabase/orchidregister/orchidregister.asp)

## Copyright and Licence

Unless otherwise stated, these scripts are Copyright © Joshua White and licensed under the GNU Lesser GPL v3.