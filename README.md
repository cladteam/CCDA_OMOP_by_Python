# CCDA_OMOP_by_Python

Uses xpath in Python on CCDA example files.

https://www.w3.org/TR/xpath-31/ 


## ToDo
- Code
  - output a dictionary to a (OMOP) table
  - OMOP classes
  - CCDA Document classes with section methods
  - Factory method for the document class based on document type
  - a driver for consuming volumes of documents so we can get an inkling of performance time and cost
  - manage template ids across parsing like sections of different  documents
    - document templates are checked at top (main for now)
    - observation type template ids are searched for, and checked that way.
  - consider structure of classes in event of parsing the same OMOP entity out of multiple CCDA sections.
#    obs_concept_code  = data['code']['coding'][0]['code']
  - build a parsing report mechanism that describes outright exceptions, or unexpected deviations like different template IDs or multiple values for things? ...does it need it's own ontology?
    - also build a separate or integrated  analsyis report with quantities described under Analysis below
- Code Nits/Bugs
  - exceptions for null concept lookups
  - check/trhow cardinality of subsections like addresses and observations
  - namespaces in xpath 
  - value types in observation for value_as_string etc., 'PQ' (physical quantity), 'ST' (character string), etc.
    - https://terminology.hl7.org/CodeSystem-v3-DataType.html 
  - map from HL7 codeSystem OIDs to OMOP vocabulary_id
  - name HL7 codeSystems correctly, not vocabulary_id 
  - use a real concept table
  - observation IDs are not unique to the individual concepts and values!
- Analysis
  - assess amount of <structuredBody> <entry> (structured)  vs <text> (non structured) plain text content
  - codeSystem vocabularies and OMOP mappings
  - how the "root" tells what kind of an ID you have, ID mapping and linking
  - metrics per person, per person/day, per person/day/concept
- Deployment
  - Run in Foundry, maintain ability to run locally
  - integrate with Founddry OMOP domain tables, maintain local
  - integrate with OMOP vocabulary instead of the small local hack here, maintain local postrgres OMOP concept table too
- S/W Eng.
   - type annotations
   - test harness
   - unit tests: date conversion, template Id snarfing, anyting in util.py
   - per doc. tests: keep expected output from specific documents handy and have a harness comopare a build's output to what we have, edit and justify deviation as development progresses.
- Design
  - maintain focus on readability, not just for code maintainability, but to keep the mapping easy to see


## Design
  A major goal of this effort is to keep the structure mapping from CCDA structures to OMOP tables easily visible, while keeping the infrastructure simple. 

  Another goal should be to fail visibly while in a development phase, rather than making assumptions and masking over irregularities for the illusion of a clean run.
