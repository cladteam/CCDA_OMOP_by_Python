# CCDA_OMOP_by_Python

This is a project to convert CCDA documents to OMOP CDM format in Python.
The bulk of the work is in the prototype_2 directory. Further instruction is in a README.md file there.


## Sub Projects/Directories
This repositor has siblings that might be of interest.
- CCDA-tools is a collection of different file snoopers, scripts that find and show a XML elements with a certain tag, like id, code or section.
 https://github.com/chrisroederucdenver/CCDA-tools
- CCDA-data
sample data is in it's own repo. The directory has been flattened compared to earlier work.
https://github.com/chrisroederucdenver/CCDA-data
- CCDA_Private is a private repository for keeping and sharing the vocabularies used here. After dealing with licenses this data, the OMOP concepts can be dowloaded from athena.ohdsi.org.
https://github.com/chrisroederucdenver/CCDA_OMOP_Private


Code here, prototype_2/value_transformations.py  loads the concept map from a file map_to_standard.csv from the local top leve.
It is not included in this repo however. It is in CCDA-tools.  Running locally, for the moment, requires the file to be copied over (or linked).
(now owned by cladteam)
