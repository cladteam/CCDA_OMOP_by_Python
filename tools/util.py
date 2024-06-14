
"""
   tools/util.py
    Miscellaneous common functions when working with ElementTree and the CCDA documents here.
"""

import xml.etree.ElementTree as ET  # https://docs.python.org/3/library/xml.etree.elementtree.html
import re  # https://docs.python.org/3.9/library/re.html

# credit: https://stackoverflow.com/questions/68215347/
#       capture-all-xml-element-paths-using-xml-etree-elementtree


def pathGen(fn):
    path = []
    it = ET.iterparse(fn, events=('start', 'end'))
    for evt, el in it:
        if evt == 'start':
            # trim off namespace stuff in curly braces
            trimmed_tag = re.sub(r"{.*}", '', el.tag)

            # don't include the "/ClinicalDocument" part at the very start
            if trimmed_tag == 'ClinicalDocument':
                path.append(".")
            else:
                path.append(trimmed_tag)

            yield '/'.join(path)
        else:
            path.pop()
