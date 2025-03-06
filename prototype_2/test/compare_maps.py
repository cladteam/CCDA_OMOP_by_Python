
import unittest
import pandas as pd
import numpy
from .. import concept_df
from .. import codemap_xwalk
from .. import ccda_value_set_mapping_table_dataset

codemap_exceptions = [
    # different mapping, but OK
    '409586006', 
    #'4668005', 
    '99212', '99213'
]


#    NO ERROR MESSAGE HERE?
#2.16.840.1.113883.6.101, 261QP2300X --> 38004247 Visit
#    NOT FOUND codemap code mapped:"0" domain:"Observation" 
    
#2.16.840.1.113883.6.238, 2076-8 --> 8557 Race
#    NOT FOUND codemap code mapped:"0" domain:"Observation" 

#2.16.840.1.113883.6.238, 2186-5 --> 38003564 Ethnicity
#    NOT FOUND codemap code mapped:"0" domain:"Observation" 

#2.16.840.1.113883.6.96, 95971004 --> 3299517 Observation
#    NOT FOUND codemap code mapped:"0" domain:"Observation" 
    

failures = [
    #2.16.840.1.113883.6.101, NUCC  -- all to 0
    '163W00000X', '207QA0505X', '207R00000X', '207RC0000X',
    '208D00000X', '261QP2300X', 

    #2.16.840.1.113883.6.238, PHIN VADS -- all to 0
    '2076-8', '2106-3', '2186-5',

    #2.16.840.1.113883.6.96, SNOMED 
    '95971004' # maps to 0
]
        
class Compare_maps(unittest.TestCase):
        
    def test_them(self):
        source_count=0
        valueset_correct_count=0
        valueset_found_count=0
        codemap_found_count=0
        codemap_correct_count=0
        for index, source_row in concept_df.iterrows():
            oid = source_row['oid']
            concept_code = source_row['concept_code']
            expected_concept_id = str(source_row['concept_id'])
            expected_domain_id = source_row['domain_id']
            source_count += 1

            
            # Value Set
            df = ccda_value_set_mapping_table_dataset[ 
                 (ccda_value_set_mapping_table_dataset['codeSystem'] == oid) &
                 (ccda_value_set_mapping_table_dataset['src_cd']  == concept_code) ]
            if df['target_concept_id'].size > 0:
                valueset_found_count += 1
                mapped_concept_id = df['target_concept_id'].iloc[0]
                mapped_domain_id = df['target_domain_id'].iloc[0]
                print(f"FOUND valueset mapped concept:{mapped_concept_id} ")
                if (mapped_concept_id != expected_concept_id):
                    print(f"Error on {oid} {concept_code}")
                self.assertEqual(mapped_concept_id, expected_concept_id)
                self.assertEqual(mapped_domain_id, expected_domain_id)
                if mapped_concept_id == expected_concept_id:
                    valueset_correct_count += 1
                    if concept_code in failures:
                        print(f"failure was successfully mapped by thevalue set table {concept_code}")
                else:
                    print((f"*** concept found in value set table, but not mapped "
                           f"correctly code:{concept_code} to id: {mapped_concept_id}"))
            else:
                # Code Map
                df = codemap_xwalk[ (codemap_xwalk['src_vocab_code_system'] == oid) &
                            (codemap_xwalk['src_code']  == concept_code) ]
                if df['target_concept_id'].size > 0:
                    codemap_found_count += 1
                    mapped_concept_id = df['target_concept_id'].iloc[0]
                    mapped_domain_id = df['target_domain_id'].iloc[0]

                    if mapped_concept_id == numpy.int32(expected_concept_id):
                        codemap_correct_count += 1
                        if concept_code in failures:
                            print(f"failure was successfully mapped by thevalue set table {concept_code}")
                    else: 
                        print(f"OID:{oid}, code:{concept_code} --> concept_id:{expected_concept_id} domain_id{expected_domain_id}")
                        print(( f"    NOT FOUND codemap code mapped:\"{mapped_concept_id}\" "
                                f"domain:\"{mapped_domain_id}\" "))
                        if concept_code in failures:
                            print(f"*** failure was found but not mapped the same by the CODEM table {concept_code}")
                    
#                    self.assertEqual(mapped_concept_id, numpy.int32(expected_concept_id))
#                    self.assertEqual(mapped_domain_id, expected_domain_id)
                    

        print(f"source:{source_count},  valueset:{valueset_correct_count}/{valueset_found_count}, codemap:{codemap_correct_count}/{codemap_found_count}")
