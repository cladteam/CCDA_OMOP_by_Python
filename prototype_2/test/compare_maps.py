
import unittest
import pandas as pd
import numpy
from .. import concept_df
from .. import codemap_xwalk
from .. import ccda_value_set_mapping_table_dataset

        
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
            #print(f" {index} {source_row}")
            ### print(f" {oid}, {concept_code} --> {expected_concept_id} {expected_domain_id}")

            
            #print(f" oid:{source_row['vocab_oid']}  concept_code:{source_row['source_concept_code']}")
            #print( (f"source_concept_id:{source_row['source_concept_id']} "
            #        f"target_concept_id:{source_row['target_concept_id']} "
            #        f"target_domain_id:{source_row['target_domain_id']} ") )
            #vocabulary_oid = source_row['vocab_oid']
            #concept_code = source_row['source_concept_code']

            
            # Value Set
            df = ccda_value_set_mapping_table_dataset[ 
                 (ccda_value_set_mapping_table_dataset['codeSystem'] == oid) &
                 (ccda_value_set_mapping_table_dataset['src_cd']  == concept_code) ]
            if df['target_concept_id'].size > 0:
                valueset_found_count += 1
                mapped_concept_id = df['target_concept_id'].iloc[0]
                mapped_domain_id = df['target_domain_id'].iloc[0]
                print(f"FOUND valueset mapped concept:{mapped_concept_id} ")
                self.assertEqual(mapped_concept_id, expected_concept_id)
                self.assertEqual(mapped_domain_id, expected_domain_id)
                if mapped_concept_id == expected_concept_id:
                    valueset_correct_count += 1
            
            # Code Map
            df = codemap_xwalk[ (codemap_xwalk['vocab_oid'] == oid) &
                            (codemap_xwalk['src_code']  == concept_code) ]
            if df['target_concept_id'].size > 0:
                codemap_found_count += 1
                mapped_concept_id = df['target_concept_id'].iloc[0]
                mapped_domain_id = df['target_domain_id'].iloc[0]
                # AssertionError: np.int32(37158809) != np.int32(4260179)
                if mapped_concept_id != 0:
                    if mapped_concept_id == numpy.int32(expected_concept_id):
                        codemap_correct_count += 1
                        # print(f"FOUND codemap mapped concept:\"{mapped_concept_id}\"  \"{expected_concept_id}\" {type(mapped_concept_id)}  {type(expected_concept_id)}  ")
                        self.assertEqual(mapped_concept_id, numpy.int32(expected_concept_id))
                        self.assertEqual(mapped_domain_id, expected_domain_id)
                    else:
                        print("")
                        print(f"{oid}, {concept_code} --> {expected_concept_id} {expected_domain_id}")
                        print(f"    NOT FOUND codemap mapped:\"{mapped_concept_id}\"  expected:\"{expected_concept_id}\" {type(mapped_concept_id)}  {type(expected_concept_id)}  ")

                    

        print(f"source:{source_count},  valueset:{valueset_correct_count}/{valueset_found_count}, codemap:{codemap_correct_count}/{codemap_found_count}")
