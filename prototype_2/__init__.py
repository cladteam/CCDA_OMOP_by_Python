

import logging
import sys


print("***** MAIN INIT ******")

logging.basicConfig(
    stream=sys.stdout, 
    format='%(levelname)s: %(message)s',
    # level=logging.ERROR
    level=logging.WARNING
    #level=logging.INFO
    # level=logging.DEBUG
)

