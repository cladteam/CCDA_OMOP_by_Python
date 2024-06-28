# CCDA_OMOP_by_Python

## Introduction
This is a python project to use an XPath XML parser in Python to extract data from HL7 CCDA documents.
[Project Documentation on Google](https://docs.google.com/document/d/1pUxCQSuucQGJcXhrEg3miWuVTkcGLBdN2v88vYdVgrM/edit)

## Sub Projects/Directories
- tools is a collection of different file snoopers, scripts that find and show a XML elements with a certain tag, like id, code or section.
- util is a collection of utilities used elsewhere
- table_objects is the start of an investigation into using objects instead of dictionaries for the resulting OMOP rows. Very nascent.
- prototype is the inital prototype
  - run with bash> python -m prototype.main

## Getting Started running the Code
This needs more detail, but works on macos. I've not been successful yet on Windows.
- Install Python3
- Install Java
- Consider an IDE. For simple things, clearly I use a shell a lot, but also PyCharm.
  - https://www.jetbrains.com/pycharm/download/?section=mac
- Create an environment for the is project
- Clone the project repo. git@github.com:chrisroederucdenver/CCDA_OMOP_by_Python.git
- Add packages to it using Pip
  - long list in requirements.txt, but it's basically pyspark. This is what depends on Java.
- Try and run basic_example.py

## Additional Tools
- code_snooper.py looks for XML elements tagged with "code" and tells the paths and  values of their code and codeSystem attributes when it can.
- id_snooper.ph looks for XML elements tagged with "id" and tells the paths and values of their root and extension attributes.
To run them from the project's main directory enter the following at a command prompt: 

> python -m tools.code_snooper

or 

> python -m tools.id_snooper

The default to a particular file in the resources directory, and can be run with a filename argument as well.

> python -m tools.id_snooper -f resources/CCDA_CCD_b1_Ambulatory_v2.xml

## Directory Layout
- htmlcov is output from an html tool. This may not be part of the repo, but gets created.
- output is where created output files live. For now they are serialized dictionaries, but may evolve to CSV.
- resources is where the sample XML files live
- ref_data, short for reference data, is a place to keep the concept table and the spark data
  - a bit messy because I was trying to keep the concept table's vocabularies private in another workspace. TODO.
- tests is a driectory for vetted output we compare to when testing. If new code doesn't produce the same output it either added something good, and the comparision files need to be updated, or something is broken or both.
- table_objects is where I've started to experiment with table objects in Python for the OMOP tables. 
  - just using dictionaries is an option.
- util is for a package containing commonly used code developed for this project. Will likely evolve. Suggestsion welcome.
- venv is the name of the local python environment
- Try and run main.py that is much more elaborate
- Have a look at the Github actions that drive tests.
   
## High level Design

## Tests and Workflows
Workflows are a way of automating tests in github so that when you commit new work, github automatically runs tests. The workflow files are in .github/workflows. For now there is one file, just_run.yml that has three jobs in it: run-main for just running the code, and Lint-Flake8 and Lint-Pylint for linters. If you haven't seen them, linters are programs that look at your code and complain whenever you haven't conformed perfectly to a particular style of coding. I think they go overboard, but I prefer overboard to underboard...

## Tests  
We may write a number of different kinds of tests. 
### Unit tests
Unit tests are small, focussed tests to checkout tricky or clever bits of code we write. Besides being small, they may use small, focus bits of data too. I wrote unit tests to figure out the windowing functions in SQL last year. It doesn't have to be code that goes into the system that gets a unit test. If you're not sure how something works, you should horse around with it in a smaller more controlled environemnt to get a feel for it. Sharing your code may help others.

Infrastructure and organization isn't complete. TBD. Eventually the workflows will run this code too.

### System Tests
May have other names, but this is where we run our project aaginst the test data we have and compare it to established expected output. This is what the workflows run. Today, the code for these tests is part of main.py. Look for COMPARE on line 109 or so. We should build input and expected output pairs whenever we get started on a new type of CCDA section. Note the variables input_filename_list and expected_text_file_list higher up in the main.py file.

### Production Tests
May also have other names. I'm thinking here of analyzing the results of runs on data we haven't seeen to weather things are running correctly.

### Data Quality Dashboard
This will be a part of testing as well.

## Development Workflow

### Source Control (needs more detail)
- Fork (not clone) the main repository to your own user on github
- Clone your branch onto your computer
- Create a branch
- make a change
- test the change
- commit the change  to your repository
- create a PR
- get online with me as we go over the changes and merge them into main.

### Test

### Doc
What did you learn that someone new to this part of the project will have to know?

### Other tasks that are part of working a sub-task
- add to the Google sheet
- make notes about what was tricky, to share with folks who may have to work with it later
- add and update tests: unit and System.



## Uses xpath in Python on CCDA example files.

https://www.w3.org/TR/xpath-31/ 

- Design Doc.s / Sheets
  - [CCDA-to-OMOP Coarse](https://docs.google.com/spreadsheets/d/1HDF-ZPOCnyAZtKYCtkhMTOhlxTfGGrBbu0Z6TTwgaRg/edit#gid=0). Maps out CCDA sectiont to OMOP domains
  - [CCDA-to-OMOP Fine](https://docs.google.com/spreadsheets/d/1Rxgkcmlhmn_78z14H_I2mlq6IBKaN4TNOOicVrdE81I/edit?pli=1#gid=1097536195). One tab per section, mapping at the field or attribute level.
Both have copius links into the HL7 documentation.

# ToDo
- High Level Mapping    
  - Consider the templates and which are used in the different kinds of documents, re-used? don't want copy-pasta code
  - How will we know patients from different sources are the same one? maybe the IDs match, maybe it's a Databant-style linking thing
  -  visits
  - show OMOP schema in more detail, esp nullable fields that we don't populate (see OMOP classes below)
  - similarly, show the remaining fields in CCDA that we don't mine
- Code
  - OMOP classes
    - show the schema including NULLable, and type
    - check PK FK relationships
    - observation IDs are not unique to the individual concepts and values!
    - output a dictionary to a (OMOP) table
  - CCDA Document classes with section methods
    - a/o 2024-03-19 there is a test for CCD in main.py
  - Factory method for the document class based on document type
  - a driver for consuming volumes of documents so we can get an inkling of performance time and cost
  - manage template ids across parsing like sections of different  documents
    - document templates are checked at top (main for now)
    - observation type template ids are searched for, and checked that way.
  - consider structure of classes in event of parsing the same OMOP entity out of multiple CCDA sections.
  - build a parsing report mechanism that describes outright exceptions, or unexpected deviations like different template IDs or multiple values for things? ...does it need it's own ontology?
    - also build a separate or integrated  analsyis report with quantities described under Analysis below
- Code Nits/Bugs
  - what do root and extension mean in HL7 CCDA?
  - exceptions for null concept lookups
  - check/throw cardinality of subsections like addresses and observations
  - namespaces in xpath 
  - value types in observation for value_as_string etc., 'PQ' (physical quantity), 'ST' (character string), etc.
    - started with PQ and ST 2024-03-19
    - https://terminology.hl7.org/CodeSystem-v3-DataType.html 
  - map from HL7 codeSystem OIDs to OMOP vocabulary_id
  - name HL7 codeSystems correctly, not vocabulary_id 
  - use a real concept table, like in a database
  - ISSUE: observation IDs are not unique to the individual concepts and values!
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
    - we have, 2024-03-19, a very basic workflow that compares expected output to what the latest code outputs. 
        - just_run.yml and tests/expeted_output
   - unit tests: date conversion, template Id snarfing, anyting in util.py
   - per doc. tests: keep expected output from specific documents handy and have a harness comopare a build's output to what we have, edit and justify deviation as development progresses.
- Design
  - maintain focus on readability, not just for code maintainability, but to keep the mapping easy to see


## Design
  A major goal of this effort is to keep the structure mapping from CCDA structures to OMOP tables easily visible, while keeping the infrastructure simple. 

  Another goal should be to fail visibly while in a development phase, rather than making assumptions and masking over irregularities for the illusion of a clean run.
