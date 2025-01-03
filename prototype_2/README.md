# Prototype 2: Data Driven CCDA Parsing
This sub project goes more deeply into parsing the CCDA, driving towards a data-driven approach and adding some software engineering concerns like better documentation and testing.

The code here is meant to work, but also show and explain the approach. The meat of the code is in data_driven_parsing.py. For learning how it works in byte-size pieces, trails of the evoution are left. 
 - parse.py is not data-driven and is a pretty straight-forward place to start reading.
 - simple_data_driven_parse.py switches to a data-driven approach.
 - data_driven_parse.py is a bit more complex and crowded having the addtion of PK/FK handling and derived/translated fields.

## Description
### Steps in code evolution: simple to complex
- Basic Python: parse.py
- Simple data-driven: simple_data_driven_parse.py
- Data-driven with PK/FK and derived fields: data_driven_parse.py
  - metadata.py
  - value_transformations.py
- (TBD) Data-driven with vocabulary access and domain_id routing.

#### Create datasets from the generated Python structures coming out of the data-driven parse implementations
  - layer_datasets.py

#### allow for CSV metadata (WIP)
 - metadata_from_file.py
 - metadata.csv


## Getting Started (how to run)
From the directory above prototype_2:
- #setup a venv (or not)
- pip install lxml pandas
- mkdir output
- mkdir logs
- python -m prototype_2.data_driven_parse
- python -m prototype_2.layer_datasets
- bin/compare_correct.sh


### run-time configuration
- logging is set module wide in prototype_2/__init__.py
- files are in resources, specified for now in either of the entry points listed above FIX


## Mapping
The mapping file is, again for now, metadata.py.

## Apologies
This code was written by a CCDA neophyte with limited time and resources in hopes of 
making a timely case for further work. The code makes extensive use of simple dictionaries
in place of objects, has less unit tests and documentation than the author would prefer.
Hopefully it has succeeded in furthering the mission. Apologies to future maintainers. 
Please have patience.
