#!/usr/bin/env bash
set -eou pipefail

#python3 -m  unittest prototype_2.test.compare_maps # tests incomplete, deprecated map
python3 -m  unittest prototype_2.test.test_field_types
python3 -m  unittest prototype_2.test.test_value_transformations
python3 -m  unittest prototype_2.test.test_value_transformations_codemap
python3 -m  unittest prototype_2.test.test_value_transformations_concat
python3 -m  unittest prototype_2.test.test_value_transformations_valueset
python3 -m  unittest prototype_2.test.test_concept_lookups_codemap
