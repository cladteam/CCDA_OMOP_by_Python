
import pandas as pd
import logging
import sys
from foundry.transforms import Dataset


print("***** MAIN INIT ******")

logging.basicConfig(
    stream=sys.stdout,
    format='%(levelname)s: %(message)s',
    # level=logging.ERROR
    level=logging.WARNING
    # level=logging.INFO
    # level=logging.DEBUG
)

concept_df = pd.read_csv("map_to_standard.csv")

codemap_xwalk = Dataset.get("codemap_xwalk").read_table(format="pandas")
ccda_value_set_mapping_table_dataset = Dataset.get("ccda_value_set_mapping_table_dataset").read_table(format="pandas")


