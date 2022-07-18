import bs4
import os
import requests
from config import (base_url, home_url, crime_data_dir, month_dict)
from helper_funcs import (get_excel_links, download_excel_files, rename_files)

###################
# DATA COLLECTION #
###################

excel_links = get_excel_links(home_url, base_url)
download_excel_files(excel_links, base_url, month_dict, crime_data_dir)
rename_files(crime_data_dir)









