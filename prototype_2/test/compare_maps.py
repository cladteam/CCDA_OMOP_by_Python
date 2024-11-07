
import unittest
import pandas as pd
from .. import concept_df
from .. import codemap_xwalk
from .. import ccda_value_set_mapping_table_dataset

        
class Compare_maps(unittest.TestCase):
        
    def test_them(self):
        for index, source_row in concept_df.iterrows():
            print(f" {index} {source_row}")
            print(f" {source_row['concept_id']} {source_row['domain_id']}")
            oid = source_row['oid']
            concept_code = source_row['concept_code']
            
            #print(f" oid:{source_row['vocab_oid']}  concept_code:{source_row['source_concept_code']}")
            #print( (f"source_concept_id:{source_row['source_concept_id']} "
            #        f"target_concept_id:{source_row['target_concept_id']} "
            #        f"target_domain_id:{source_row['target_domain_id']} ") )
            #vocabulary_oid = source_row['vocab_oid']
            #concept_code = source_row['source_concept_code']
            
            df = ccda_value_set_mapping_table_dataset[ 
                 (ccda_value_set_mapping_table_dataset['codeSystem'] == oid) &
                 (ccda_value_set_mapping_table_dataset['src_cd']  == concept_code) ]
            # data_source, resource, data_element_path, data_element_node, 
            # codeSystem, src_cd, src_cd_description, 
            # target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code, target_tbl_column_name, notes]
            # df[column_name].iloc[0]
            print(f"xxxx mapped concept:{df['target_concept_id'].iloc[0]} ")
            # print(f"      vocab:{df['target_vocabulary_id']}  ")
            # print(f"      domain:{df['target_domain_id'].iloc[0]}")

            ## df = codemap_xwalk[ (codemap_xwalk['vocab_oid'] == vocabulary_oid) &
            ##                (codemap_xwalk['src_code']  == concept_code) ]
            ### print(df)

            print("====")
