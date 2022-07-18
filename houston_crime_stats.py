import bs4
import glob
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import requests
import seaborn as sns
import xlrd
from scipy import stats


def get_all_excel_links(h_url, b_url):

	res = requests.get(h_url)
	soup = bs4.BeautifulSoup(res.text, 'lxml')

	links = list()

	for link in soup.find_all('a', href=True):
		if link == '#':
			pass
		if str(link['href']).startswith('xls'):
			links.append(b_url + link['href'])

	return links


def download_all_excel_files(links, b_url, m_dict, data_dir):

	"""
	function takes an array of links, a base URL, a dictionary for months,
	and a download directory path and downloads the files from those links

	due to the fact that there are two types of Excel files, the two types
	need to be parsed differently so will be saved in separate directories
	"""

	for link in links:

		r = requests.get(link, stream = True)

		# extract the filename from the link
		file_name = link.replace(b_url + 'xls/', '')

		print(f"Downloading {file_name}...")

		# clean up and standardize filenames into format mm-yyyy.xls
		if file_name.endswith('xlsx'):

			file_name = file_name.replace('.NIBRS_Public_Data_Group_A&B','')

			with open(data_dir + '/messy/' + file_name,'wb') as f: 
			    f.write(r.content) 
		else:
			file_name = m_dict[file_name[0:3]]+ '-' + '20' + file_name[3:]
			file_name = file_name.replace('xls', 'xlsx')

			with open(data_dir + file_name,'wb') as f: 
			    f.write(r.content) 


def rename_files(data_dir):
	
	files_in_data_dir = os.listdir(data_dir)
	excel_file_names = [file for file in files_in_data_dir if file.endswith(".xlsx")]

	for name in excel_file_names:

		new_name = name.split('-')
		new_name[1] = new_name[1].replace('.xlsx','')
		new_name[0], new_name[1] = new_name[1], new_name[0]
		new_name = "-".join(new_name) + ".xlsx"
		os.rename(os.path.join(data_dir, name),os.path.join(data_dir, new_name))

def drop_extraneous_cols(df):

	"""
	function taking Pandas dataframe created from an Excel file of crime incidents and dropping extraneous columns
	"""

	cols = list(df.columns)
	num_offenses_col_names = ['# Of Offenses', '# Of', '# Offenses', '# offenses', 'Offenses']

	num_offenses_col_name = [name for name in num_offenses_col_names if name in cols][0]

	cols_to_keep = ['Date', 'Offense Type', num_offenses_col_name]
	cols_to_drop = [col for col in cols if col not in cols_to_keep]
	df.drop(columns=cols_to_drop, inplace=True)
	df.rename(columns={"Offense Type": "Offense", num_offenses_col_name: "#"}, inplace=True)

	df.loc[(df["Offense"] == 'Aggravated Assault'), "Offense"] = 'Assault'


def drop_extraneous_rows(df):

	"""
	function taking Pandas dataframe created from an Excel file of crime incidents and dropping null/non-violent rows
	"""

	nonviolent_offenses = ['Auto Theft', 'Theft', 'Burglary']
	for offense in nonviolent_offenses:
		index_names = df[df['Offense'] == offense].index
		df.drop(index_names, inplace=True)

	date_bools = pd.isna(df["Date"])
	df.drop(date_bools[date_bools==True].index, axis=0, inplace=True)

	offenses = set(df["Offense"])
	non_str_offenses = [offense for offense in offenses if type(offense) != str]
	for non_str_offense in non_str_offenses:
		index_names = df[df['Offense'] == non_str_offense].index
		df.drop(index_names, inplace=True)


def cleanup_whitespaces(df):

	offenses = set(df["Offense"])

	for offense in offenses:
		new_offense_name = offense.strip()
		df.loc[(df["Offense"] == offense), "Offense"] = new_offense_name

	
	df.loc[(df["Offense"] == 'Aggravated Assault'), "Offense"] = 'Assault'


def assemble_numeric_dates(df):

	dates = list()
	for date in list(df["Date"]):
		if str(date)[4] == '-':
			dates.append([int(item) for item in str(date).split(' ')[0][0:7].split('-')])
		elif str(date)[2] == '/':
			dates.append([int(str(date).split('/')[2]), int(str(date).split('/')[0])])
	return dates


def reformat_date_columns(dates, df):

	# drop rows with missing date values 'NaT'
	date_bools = pd.isna(df["Date"])
	df.drop(date_bools[date_bools==True].index, axis=0, inplace=True)

	# replace original date entries in df with numeric lists
	df["Year"], df["Month"] = [date[0] for date in dates], [date[1] for date in dates]
	df.drop(columns=["Date"], inplace=True)

	# throw out entries with dates preceding June 2009
	drop_indices = df[(df["Year"] < 2009) | 
					 ((df["Year"] == 2009) & (df["Month"] < 6))].index
	df.drop(drop_indices, axis=0, inplace=True)


def append_monthly_sum(year, month, df_in, df_out):

	# specify list of violent crime offenses
	violent_offenses = ['Rape', 'Murder', 'Assault',  'Robbery']

	for offense in violent_offenses:

		offense_counts = df_in[(df_in["Year"] == year) & 
					           (df_in["Month"] == month) & 
					           (df_in["Offense"] == offense)
					          ]["#"]
					        
		df_out = df_out.append({"Year": year, 
						"Month": month, 
						"Offense": offense, 
						"#": int(sum(offense_counts))}, 
						ignore_index=True
						)

	return df_out


def print_monthly_plots(df):

	violent_offenses = ['Rape', 'Murder', 'Assault',  'Robbery']

	for offense in violent_offenses:

		plot_name = "monthly_" + offense.lower() + "_counts.png"

		data = df[df["Offense"] == offense]
		data = data.reset_index(drop=True)
		data['index'] = list(data.index)

		x = list(data.index)
		y = list(df[df["Offense"] == offense]["#"])

		plt.xlabel("Months after June 2009")
		plt.ylabel("Reported Incidents of " + offense)
		plt.plot(x,y, color='red', linewidth = 0.4, marker='o', markerfacecolor='red', markersize=4)
		plt.savefig(plot_name)
		plt.close()


base_url = "https://www.houstontx.gov/police/cs/"
home_url =  base_url + "crime-stats-archives.htm"

crime_data_dir = "/Users/mason/Documents/DataScience/HoustonCrime/crime_excel_files/"

month_dict = {'jan': '01', 'feb': '02',
              'mar': '03', 'apr': '04', 
              'may': '05', 'jun': '06', 
              'jul': '07', 'aug': '08', 
              'sep': '09', 'oct': '10', 
              'nov': '11', 'dec': '12'}

#################
# DATA CLEANING #
#################

# create empty dataframe storing all data
col_names = ["Year", "Month", "Offense", "#"]
df_full = pd.DataFrame(columns=col_names)

# loop over all non-messy Excel files
# path = crime_data_dir + "*.xlsx"
# for month_file in sorted(glob.glob(path)):

# 	file_name = month_file.split("/")[-1].split(".")[0].split("-")
#
# 	year = int(file_name[0])
# 	month = int(file_name[1])

# 	print([month, year])

# 	wb = xlrd.open_workbook(month_file, logfile=open(os.devnull, 'w'))	
# 	df = pd.read_excel(wb)
	
# 	# drop extraneous columns and rename others
# 	drop_extraneous_cols(df)

# 	# drop extraneous rows (non-violent offenses)
# 	drop_extraneous_rows(df)

# 	# clean up whitespaces in offense name fields
# 	cleanup_whitespaces(df)

# 	# convert TimeStep date objects to numeric lists [yyyy, mm]
# 	dates = assemble_numeric_dates(df)
# 	reformat_date_columns(dates, df)

# 	# sum up all incidents of the same type for each month
# 	df_full = append_monthly_sum(year, month, df, df_full)


# df_full.to_pickle("monthly_violent_crime_stats.pkl")


df = pd.read_pickle("monthly_violent_crime_stats.pkl")
print_monthly_plots(df)






########################################
#			   graveyard               #
########################################
# violent_offenses = ['Rape', 'Murder', 'Assault',  'Robbery']
# ['# Of Offenses', '# Of', '# Offenses', '# offenses', 'Offenses']
# sns.set(style="darkgrid")
# ax = sns.lineplot(x='index', y='#', data=data)
# fig = ax.get_figure()
# fig.savefig('rape_data_plot.png')

########################################
#			used-up code               #
########################################






