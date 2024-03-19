# CCDA_OMOP_by_Python

Uses xpath in Python on CCDA example files.

https://www.w3.org/TR/xpath-31/ 


## ToDo
- Code
  - OMOP classes
  - CCDA Document classes with section methods
  - Factory method for the document class based on document type
  - a driver for consuming volumes of documents so we can get an inkling of performance time and cost
  - exceptions for null concept lookups
  - check/trhow cardinality of subsections like addresses and observations
  - namespaces in xpath 
  - value types in observation for value_as_string etc., 'PQ', etc.
  - map from HL7 codeSystem OIDs to OMOP vocabulary_id
  - name HL7 codeSystems correctly, not vocabulary_id 
  - use a real concept table
- Analysis
  - assess amount of <structuredBody> <entry> (structured)  vs <text> (non structured) plain text content
  - codeSystem vocabularies and OMOP mappings
  - how the "root" tells what kind of an ID you have, ID mapping and linking
- Deployment
  - Run in Foundry, maintain ability to run locally
  - integrate with Founddry OMOP domain tables, maintain local
  - integrate with OMOP vocabulary instead of the small local hack here, maintain local postrgres OMOP concept table too
- Design
  - maintain focus on readability, not just for code maintainability, but to keep the mapping easy to see


## Design
  A major goal of this effort is to keep the structure mapping from CCDA structures to OMOP tables easily visible, while keeping the infrastructure simple. 

  Another goal should be to fail visibly while in a development phase, rather than making assumptions and masking over irregularities for the illusion of a clean run.
