#!/usr/bin/env python3

# hacking with code from here: https://docs.python.org/3/library/xml.etree.elementtree.html

import xml.etree.ElementTree as ET
from pathlib import Path

tree = ET.parse("resources/CCDA_CCD_b1_InPatient_v2.xml")

xpaths_text = Path("resources/simple.txt").read_text()
for xpath_line in xpaths_text.splitlines():
    if len(xpath_line) > 0:
        print(f"{xpath_line}")
        child_list = tree.findall(xpath_line)
        for child in child_list:
            print(child.text, end=" ")
            print(child.tag, end=" ")
            print(child.attrib)
        print("-----------------------------")

