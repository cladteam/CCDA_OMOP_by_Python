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
from util import util
from util import spark_util


# INIT
spark_util_object = spark_util.SparkUtil()
spark = spark_util_object.get_spark()


input_filename_list = [
    'CCDA_CCD_b1_InPatient_v2.xml',
    'CCDA_CCD_b1_Ambulatory_v2.xml',
    'ToC_CCDA_CCD_CompGuideSample_FullXML.xml',
    '170.314b2_AmbulatoryToC.xml'
]

input_section_filename_list = [  # TODO this one fails because of
    # checks for things like doctype and an address or patient
    'Inpatient_Encounter_Discharged_to_Rehab_Location(C-CDA2.1).xml'
]

expected_text_file_list = [
    'CCDA_CCD_b1_InPatient_v2.txt',
    'CCDA_CCD_b1_Ambulatory_v2.txt',
    'ToC_CCDA_CCD_CompGuideSample_FullXML.txt'
]

parser = argparse.ArgumentParser(
    prog='CCDA_OMOP_Converter Test Driver',
    description="Converts CCDA documents to OMOP tables",
    epilog='epilog?')
parser.add_argument('-n', '--num_tests', default=len(input_filename_list),
                    help="do the first n tests")
parser.add_argument('-s', '--save', action='store_true',
                    help="save output, requires a directory called 'output'")
args = parser.parse_args()

NUM_ERROR_FILES = 0


def report_diffs(diff_generator):
    """ prints differences from a diff generator, counts them and summarizes """
    count_errors = 0  # lints as a constant?!
    for difference in diff_generator:
        print("DIFF: ", difference)
        count_errors += 1
    if count_errors > 0:
        print(f"ERROR:Differences found for {input_filename}:")
        return True
    print(f"INFO:No differences found for {input_filename}:")
    return False


FILE_NUM = 0

todo_list = input_filename_list
if (len(input_filename_list) >= int(args.num_tests) and int(args.num_tests) > 0):
    todo_list = input_filename_list[:(int(args.num_tests))]

for input_filename in todo_list:
    print(f"==== {input_filename} ====")

    try:
        tree = ET.parse('resources/' + input_filename)
        actual_text_list = []

        if not util.check_ccd_document_type(tree):
            print(f"WARN:wrong doc type in {input_filename}")

        # Convert
        actual_text_list.append(str(location.convert(tree)))
        actual_text_list.append(str(person.convert(tree, spark)))

        for obs in observation.convert(tree):
            actual_text_list.append(str(obs))

        # Save
        if args.save:
            output_filename = input_filename[0:(len(input_filename) - 4)] + '.txt'
            with open('output/' + output_filename, 'w', encoding='utf-8') as outfile:
                for line in actual_text_list:
                    outfile.write(line + "\n")

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
        if report_diffs(diff_gen):
            NUM_ERROR_FILES += 1

        FILE_NUM += 1
    except ET.ParseError as x:
        NUM_ERROR_FILES += 1
        print(f"ERROR: Could not parse {input_filename}:\n   {x}")
    except IndexError as x:
        print(f"ERROR: something wrong with {input_filename}:\n   {x}")
        raise


if NUM_ERROR_FILES > 0:
    sys.exit(1)
