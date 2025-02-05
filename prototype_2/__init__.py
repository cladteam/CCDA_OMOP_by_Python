
import pandas as pd
import logging
import sys
import os

MIN_PYTHON = (3, 10)
if sys.version_info < MIN_PYTHON:
    sys.exit(f"Python version {MIN_PYTHON}  or later is required.")

try:
    from foundry.transforms import Dataset
except Exception:
    print("no foundry transforms imported")
try:
    os.mkdir("logs")
except:
    pass
try:
    os.mkdir("output")
except:
    pass

logging.basicConfig(
    stream=sys.stdout,
    format='%(levelname)s: %(message)s',
    # level=logging.ERROR
    level=logging.WARNING
    # level=logging.INFO
    # level=logging.DEBUG
)

concept_df = pd.read_csv("map_to_standard.csv")

codemap_xwalk = None
ccda_value_set_mapping_table_dataset = None
visit_concept_xwalk_mapping_dataset = None
try:
    codemap_xwalk = Dataset.get("codemap_xwalk").read_table(format="pandas")
    ccda_value_set_mapping_table_dataset = Dataset.get("ccda_value_set_mapping_table_dataset").read_table(format="pandas")
    visit_concept_xwalk_mapping_dataset = Dataset.get("visit_concept_xwalk_mapping_dataset").read_table(format="pandas")
except Exception:
    print("no mapping tables from foundry for codemap_xwalk and ccda_valueset")

