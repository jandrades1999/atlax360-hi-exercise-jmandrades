import json
import os
import pyodbc
import sys
import logging

import pandas as pd
from typing import List

from libs.DBExtractor import DBExtractor

BASEDIR = os.getcwd()
CSVDIR = BASEDIR + '/csv'
GZDIR = BASEDIR + '/gzip'
    

def main(args: List[str]=[]):
    exitcode = 0
    
    try:        
        extractor = DBExtractor(os.getcwd() + "/src/config/exercise-atlax360.json")
        extractor.extract()
    except Exception as e:
        logging.error(e)
        exitcode = 1
    finally:
        if exitcode == 0:
            logging.info(f"\n\n Data  extracted correctly! \n   - CSV file path: {CSVDIR}\n   - GZ file path: {GZDIR}")
        sys.exit(exitcode)

main()
