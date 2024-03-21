

""" a namespace dict for xml.etree.ElementTree
    https://docs.python.org/3/library/xml.etree.elementtree.html
    Without the default entry below, paths required inclusion of the
    {urn:hl7-org:v3} namespace spec.
"""

ns = {
    '': 'urn:hl7-org:v3',  # default namespace
    'hl7': 'urn:hl7-org:v3',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'sdtc': 'urn:hl7-org:sdtc'
    }
