# National Gardening Association Plant Database Management Scripts

This repository is a collection of scripts for working with the NGA Database. Use of the scripts with the NGA site requires an admin account with the NGA.

## Accounts

Use of these scripts requires accounts to enable access to datasets and for modification of the NGA Plant Database.

- [garden.org][https://garden.org/] account required for proposing changes (admin account required for automatically accepting proposals)
- [GBIF](https://www.gbif.org/) account required for use of the Catalogue of Life API

>> A future version of the code will allow the user to initialise a set of login files from the CLI.

## Dependencies

- Python 3 with the following modules
  - Beautiful Soup 4 (bs4)
  - Levenshtein (python-levenshtein)
  - requests

## Acknowledgements

These scripts rely on a number of online resources. The work by those who maintain the following projects is greatly appreciated:

- [Catalogue of Life](https://www.catalogueoflife.org/) and the [COL API Backend](https://github.com/CatalogueOfLife/backend/)
- [KEW World Checklist of Selected Plants](https://wcsp.science.kew.org/)
- [RHS Orchid Register](https://apps.rhs.org.uk/horticulturaldatabase/orchidregister/orchidregister.asp)

## Copyright and Licence

Unless otherwise stated, these scripts are Copyright © Joshua White and licensed under the GNU Lesser GPL v3.