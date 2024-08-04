# CCDA tools
This is a collection of analysis tools to be used to compare an OMOP
mapping to the output here to help see that the mapping is complete.

- section_code_snooper.py
    Driven by a list of sections and their template IDs,
       this code looks for codes found in such a section and lists them.

- section_snooper.py
    Looks for specfic sections driven by metadata,
        and shows any ID, CODE and VALUE elements within them.

- header_code_snooper.py
    Finds and outputs code elements found under certain header elements

- header_snooper.py
    Driven by three levels of metadata for top-level header elements,
    middle elements, and attributes, shows what is foudn in the header. Mostly
    involving time, assinged person, assigned entity and encompassing encounter.

