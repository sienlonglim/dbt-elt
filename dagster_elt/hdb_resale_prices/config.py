import os

from ..dagster_utils.constants import AMAZON_S3_BUCKET_PROJECT_FOLDER
from .functions import build_list_of_year_month_strings

S3_SUBFOLDER = "hdb-resale-records"
S3_PREFIX = os.path.join(AMAZON_S3_BUCKET_PROJECT_FOLDER, S3_SUBFOLDER).replace(os.path.sep, '/')

# Input the historical starting period to look up
HISTORICAL_START_YEAR_MONTH = "2018-01"
YEAR_MONTHS_TO_EXTRACT = build_list_of_year_month_strings(HISTORICAL_START_YEAR_MONTH)
