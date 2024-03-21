# XPath oriented python style guide


- use _ for private functions
- provide public functions for fetching PK/FK from documents, Ex. person_id
- provide functions for managing IDs that dont' exist in the documents. Ex. location is not a CCDA entity and so doesn't have an ID, but is in OMOP
- start such functions' parsing from the tree
- question always pulling the first child
- mark open questions with TODO
- stay on the lookout for more namespace issues on attributes like in observation for the value type. The attr dictionary doesn't take a namespace mappig the same way the find() and findall()  functions do (it's not a function for starters). If that's the only instance, I'm fine using the ns dictionary and gluing it together as done there. If more instances pop up, we might create a function in util for it.

## ETL Structure
- create
- parse


