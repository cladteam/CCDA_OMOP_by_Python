#!/usr/bin/env python3

# hacking with code from here: https://docs.python.org/3/library/xml.etree.elementtree.html

import xml.etree.ElementTree as ET
from pathlib import Path

def get_xpath_step(xpath, root, level):
    parts = xpath.split("/")
    if (len(parts) > 0):
        print(len(parts), parts[1], xpath)
    if len(parts) > 2:
        new_path=""
        for i in range(2, len(parts) ):
            new_path = new_path + "/" + parts[i]
        get_xpath_step(new_path, child, level+1)
        

def get_xpath(xpath, root):
    for path_part in xpath.split("/"):
        if len(path_part) > 0:
            if path_part[0] == '@':
                print(f"attribute {path_part}")
            else:
                print(f"element {path_part}")

tree = ET.parse("resources/CCDA_CCD_b1_InPatient_v2.xml")
root = tree.getroot()
ET.dump(root)
print("==============================")

xpaths_text = Path("resources/simple.txt").read_text()
for xpath_line in xpaths_text.splitlines():
    if len(xpath_line) > 0:
        print(f"{xpath_line}")
        child = tree.findall(xpath_line)
        ##ET.dump(child)
        for thing in child:
        #    print(thing)
             ET.dump(thing)
        print("-----------------------------")

