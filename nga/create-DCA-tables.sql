CREATE TABLE Taxon(
taxonID text PRIMARY KEY,
parentNameUsageID text,
acceptedNameUsageID text,
originalNameUsageID text,
scientificNameID text,
datasetID integer,
taxonomicStatus text,
taxonRank text,
scientificName text,
scientificNameAuthorship text,
notho text,
genericName text,
infragenericEpithet text,
specificEpithet text,
infraspecificEpithet text,
cultivarEpithet text,
nameAccordingTo text,
namePublishedIn text,
nomenclaturalCode text,
nomenclaturalStatus text,
kingdom text,
phylum text,
"class" text,
"order" text,
superfamily text,
family text,
subfamily text,
tribe text,
taxonRemarks text,
"references" text);

CREATE TABLE Distribution(
taxonID text,
occurrenceStatus text,
locationID text,
locality text,
countryCode text,
source text,
FOREIGN KEY (taxonID) REFERENCES Taxon(taxonID));

CREATE TABLE SpeciesProfile(
taxonID text,
isExtinct boolean,
isMarine boolean,
isFreshwater boolean,
isTerrestrial boolean,
FOREIGN KEY (taxonID) REFERENCES Taxon(taxonID));

CREATE TABLE VernacularName(
taxonID	text,
language text,
vernacularName text,
FOREIGN KEY (taxonID) REFERENCES Taxon(taxonID));
