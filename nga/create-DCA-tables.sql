CREATE TABLE Taxon(
taxonID text PRIMARY KEY,
parentNameUsageID text,
acceptedNameUsageID text,
originalNameUsageID text,
datasetID integer,
taxonomicStatus text,
taxonRank text,
scientificName text,
genericName text,
specificEpithet text,
infraspecificEpithet text,
nameAccordingTo text,
namePublishedIn text,
nomenclaturalCode text,
nomenclaturalStatus text,
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
