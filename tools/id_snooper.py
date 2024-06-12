

"""
    IDSnooper - looks for ID elements, fetches their root and extension
                  attributes, mapping OIDs to vocabularies and 
                  concept codes to names, lists the paths to the elements
                  with their attributes.
"""

import argparse
import xml.etree.ElementTree as ET # https://docs.python.org/3/library/xml.etree.elementtree.html
import re # https://docs.python.org/3.9/library/re.html
from util.xml_ns import ns
from util.vocab_map_file import  oid_map
from util import spark_util
from util.vocab_spark import VocabSpark

INPUT_FILENAME = 'resources/CCDA_CCD_b1_InPatient_v2.xml'
spark_util_object = spark_util.SparkUtil()
spark = spark_util_object.get_spark()

parser = argparse.ArgumentParser(
    prog='CCDA - OMOP Code Snooper',
    description="finds all code elements and shows what concepts the represent",
    epilog='epilog?')
parser.add_argument('-f', '--filename', default=INPUT_FILENAME,
                    help="filename to parse")
args = parser.parse_args()

tree = ET.parse(INPUT_FILENAME)

# credit: https://stackoverflow.com/questions/68215347/capture-all-xml-element-paths-using-xml-etree-elementtree
def pathGen(fn):
    path = []
    it = ET.iterparse(fn, events=('start', 'end'))
    for evt, el in it:
        if evt == 'start':
            # trim off namespace stuff in curly braces
            trimmed_tag = re.sub(r"{.*}",'', el.tag)

            # don't include the "/ClinicalDocument" part at the very start
            if trimmed_tag == 'ClinicalDocument':
                path.append(".")
            else:
                path.append(trimmed_tag)

            yield '/'.join(path)
        else:
            path.pop()

for path in pathGen(INPUT_FILENAME):
    # just get the paths that end with a code element (tag)
    if re.fullmatch(r".*/id", path):
        for id_element in tree.findall(path, ns):

            root = "(none)"
            try:
                root = id_element.attrib['root']
            except:
                pass #print(f"{path}  -- no attributes, or not both --")

            extension = "(none)"
            try:
                extension = id_element.attrib['extension']
            except:
                pass # print(f"{path}  -- no attributes, or not both --")
            print(f"{path}  root:{root} extension:{extension}")

