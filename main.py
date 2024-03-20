#!/usr/bin/env python3

# main.py <capture_output>
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
parser.add_argument('-s', '--save', action="store_true",
                    help="store the output in the outputs directory")
parser.add_argument('-n', '--num_tests', default=len(input_filename_list),
                    help="do the first n tests")
args = parser.parse_args()


output_fn = print
OUTFILE = None


FILE_NUM = 0
todo_list = input_filename_list
if (len(input_filename_list) >= int(args.num_tests) and int(args.num_tests) > 0):
    todo_list = input_filename_list[:(int(args.num_tests))]
for input_filename in todo_list:
    tree = ET.parse('resources/' + input_filename)

    if args.save:
        output_filename = input_filename[0:(len(input_filename) - 4)] + '.txt'
        OUTFILE = open('output/' + output_filename, 'w', encoding='utf-8')

        def capture_output(out_thing):
            """ closure for capturing output to a file as well as to a string """
            actual_text = ""
            actual_text += str(out_thing)
            OUTFILE.write(str(out_thing) + '\n')
            return actual_text

        output_fn = capture_output
    else:
        def capture_output(out_thing):
            """ closure for capturing output to just a string """
            actual_text = ""
            actual_text += str(out_thing)
            return actual_text
        output_fn = capture_output

    loc = location.convert(tree)

    if util.check_ccd_document_type(tree):
        # Convert
        target = {
             'location': loc,
             'person': person.convert(tree),
             'observation': observation.convert(tree)}
        output_fn(target['location'])
        output_fn(target['person'])
        for obs in target['observation']:
            output_fn(obs)

        # Compare
        ACTUAL_STRING_LIST = output_fn('')
        expected_text = pathlib.Path('tests/' +
                                     expected_text_file_list[FILE_NUM]).\
            read_text()
        expected_string_list = [expected_text]
        diff_gen = difflib.context_diff(ACTUAL_STRING_LIST,
                                        expected_string_list,
                                        fromfile="expected",
                                        tofile="actual")
        try:
            for difference in diff_gen:
                print(difference)
            print(f"ERROR:Differences found for {input_filename}:")
            if args.save:
                OUTFILE.close()
            sys.exit(1)
        except StopIteration:
            print(f"INFO:No Differences for {input_filename}:")
    else:
        print(f"ERROR:wrong doc type boss {input_filename}")

    FILE_NUM += 1

if args.save:
    OUTFILE.close()

