#!/usr/bin/env bash
set -eou pipefail

echo test_value_transformations
 python3 -m unittest prototype_2.test.test_value_transformations
echo "-----------------"

echo test_value_transformations_valueset
 python3 -m unittest prototype_2.test.test_value_transformations_valueset
echo "-----------------"

echo test_value_transformations_codemap
 python3 -m unittest prototype_2.test.test_value_transformations_codemap
echo "-----------------"

echo test_field_types
 python3 -m unittest prototype_2.test.test_field_types
echo "-----------------"

# obsolete?
#echo compare_maps
#python3 -m unittest prototype_2.test.compare_maps
#echo "-----------------"

echo test_value_transformations_concat
python3 -m  unittest prototype_2.test.test_value_transformations_concat
echo "-----------------"

echo test_concept_lookups_codemap
python3 -m  unittest prototype_2.test.test_concept_lookups_codemap
echo "------------------"

echo test_timezone_comparisons
python3 -m  unittest prototype_2.test.test_timezone_comparisons
echo "------------------"

echo test_type_handling
python3 -m  unittest prototype_2.test.test_type_handling
echo "------------------"

echo test_visit_FK_reconciliation
python3 -m  unittest prototype_2.test.test_visit_FK_reconciliation
echo "------------------"


# load into DuckDB for constraint errors
python3 -m omop.setup_omop
