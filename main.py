#!/usr/bin/env python3

# main.py
"""
 - Runs CCDA to OMOP conversion on a number of listed files for dev/text,
   retrieved from the resources directory.
 - Compares output to expected output files, in the tests directory.
   Comparision does not use Linux diff, because creating a temporary
   file in Spark environments can be a hassle. TBD. using difflib for now.
 - creating new files in Spark environments might make creating expected
   files difficult. TBD
"""

import xml.etree.ElementTree as ET
import pathlib
import difflib
import argparse
import sys

import location
import person
import observation
import util

input_filename_list = [
    'CCDA_CCD_b1_InPatient_v2.xml',
    'CCDA_CCD_b1_Ambulatory_v2.xml',
    'Inpatient_Encounter_Discharged_to_Rehab_Location(C-CDA2.1).xml',
    '170.314b2_AmbulatoryToC.xml',
    'ToC_CCDA_CCD_CompGuideSample_FullXML.xml']

expected_text_file_list = [
    'CCDA_CCD_b1_InPatient_v2.txt',
    'CCDA_CCD_b1_Ambulatory_v2.txt']

parser = argparse.ArgumentParser(
    prog='CCDA_OMOP_Converter Test Driver',
    description="Converts CCDA documents to OMOP tables",
    epilog='epilog?')
parser.add_argument('-n', '--num_tests', default=len(input_filename_list),
                    help="do the first n tests")
args = parser.parse_args()


FILE_NUM = 0
NUM_ERROR_FILES = 0
todo_list = input_filename_list
if (len(input_filename_list) >= int(args.num_tests) and int(args.num_tests) > 0):
    todo_list = input_filename_list[:(int(args.num_tests))]


actual_text_list = []


def output_fn(out_thing, my_outfile):
    """ function for capturing output to a file as well as to a string """
    actual_text_list.append(str(out_thing))
    my_outfile.write(str(out_thing) + '\n')


for input_filename in todo_list:
    print(f"==== {input_filename} ====")
    tree = ET.parse('resources/' + input_filename)
    actual_text_list = []

    output_filename = input_filename[0:(len(input_filename) - 4)] + '.txt'
    with open('output/' + output_filename, 'w', encoding='utf-8') as outfile:

        if not util.check_ccd_document_type(tree):
            print(f"ERROR:wrong doc type in {input_filename}")
        else:
            # Convert
            output_fn(location.convert(tree), outfile)
            output_fn(person.convert(tree), outfile)
            for obs in observation.convert(tree):
                output_fn(obs, outfile)

            # Compare
            expected_text = pathlib.Path('tests/' +
                                         expected_text_file_list[FILE_NUM]).\
                read_text(encoding='utf-8')
            expected_string_list = expected_text.split("\n")
            diff_gen = difflib.context_diff(actual_text_list,
                                            expected_string_list[:-1],
                                            fromfile="expected",
                                            tofile="actual")

            # Report
            COUNT_ERRORS = 0  # lints as a constant?!
            for difference in diff_gen:
                print("DIFF: ", difference)
                COUNT_ERRORS += 1
            if COUNT_ERRORS > 0:
                print(f"ERROR:Differences found for {input_filename}:")
                NUM_ERROR_FILES += 1
            else:
                print(f"INFO:No differences found for {input_filename}:")

    FILE_NUM += 1
    if NUM_ERROR_FILES > 0:
        sys.exit(1)
