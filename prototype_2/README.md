# Prototype 2: Data Driven CCDA Parsing
This sub project goes more deeply into parsing the CCDA, driving towards a data-driven approach and adding some software engineering concerns like better documentation and testing.

The code here is meant to work, but also show and explain the approach. The meat of the code is in data_driven_parsing.py. For learning how it works in byte-size pieces, trails of the evoution are left. 
 - parse.py is not data-driven and is a pretty straight-forward place to start reading.
 - simple_data_driven_parse.py switches to a data-driven approach.
 - data_driven_parse.py is a bit more complex and crowded having the addtion of PK/FK handling and derived/translated fields.

## Steps in development: simple to complex
- Basic Python: parse.py
- Simple data-driven: simple_data_driven_parse.py
- Data-driven with PK/FK and derived fields: data_driven_parse.py
  - metadata.py
  - value_transformations.py
- (TBD) Data-driven with vocabulary access and domain_id routing.

### Create datasets from the generated Python structures coming out of the data-driven parse implementations
  - layer_datasets.py

### allow for CSV metadata (WIP)
 - metadata_from_file.py
 - metadata.csv
