


import config
import glob
import os
import pandas as pd
import xlrd
from config import (base_url, home_url, 
					crime_data_dir, month_dict)
from helper_funcs import (drop_extraneous_cols, drop_extraneous_rows, 
						  cleanup_whitespaces, assemble_numeric_dates,
						  reformat_date_columns, append_monthly_sum)

#################
# DATA CLEANING #
#################

# create empty dataframe storing cleaned data
col_names = ["Year", "Month", "Offense", "#"]
df_full = pd.DataFrame(columns=col_names)

# loop over all parsable Excel files
path = crime_data_dir + "*.xlsx"
for month_file in sorted(glob.glob(path)):

	file_name = month_file.split("/")[-1].split(".")[0].split("-")

	year = int(file_name[0])
	month = int(file_name[1])

	print([month, year])

	# import spreadsheet as Pandas dataframe
	wb = xlrd.open_workbook(month_file, logfile=open(os.devnull, 'w'))	
	df = pd.read_excel(wb)
	
	drop_extraneous_cols(df)

	drop_extraneous_rows(df)

	cleanup_whitespaces(df)

	# convert TimeStep dates to numeric lists [yyyy, mm]
	dates = assemble_numeric_dates(df)
	reformat_date_columns(dates, df)

	# sum all incidents of the same type for each month
	df_full = append_monthly_sum(year, month, df, df_full)

# export the monthly summed and cleaned data to pickle
df_full.to_pickle("monthly_violent_crime_stats.pkl")


