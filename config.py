# IMPORTS

import bs4
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import requests


# DEFINITIONS

base_url = "https://www.houstontx.gov/police/cs/"
home_url =  base_url + "crime-stats-archives.htm"
crime_data_dir = "/Users/mason/Documents/DataScience/HoustonCrime/crime_excel_files/"
month_dict = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 
              'may': '05', 'jun': '06', 'jul': '07', 'aug': '08', 
              'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}

# FUNCTIONS

def get_excel_links(h_url, b_url):

	res = requests.get(h_url)
	soup = bs4.BeautifulSoup(res.text, 'lxml')

	links = list()

	for link in soup.find_all('a', href=True):
		if link == '#':
			pass
		if str(link['href']).startswith('xls'):
			links.append(b_url + link['href'])

	return links


def download_excel_files(links, b_url, m_dict, data_dir):

	"""
	takes array of links, a base URL, a dictionary of month names and a 
	download directory path and downloads the files from those links

	two types of Excel files: parsable and messy; the two types need to be 
	downloaded into separate directories
	"""

	for link in links:

		r = requests.get(link, stream=True)

		# extract the filename from the link
		file_name = link.replace(b_url + 'xls/', '')

		print(f"Downloading {file_name}...")

		# standardize filenames into format mm-yyyy.xls
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
	takes Pandas dataframe created from Excel file,
	drops non-violent offenses and location information
	and renames the aggravated assault incidents
	"""

	cols = list(df.columns)

	col_names = ['# Of Offenses', '# Of', '# Offenses', 
				 '# offenses', 'Offenses']

	col_name = [name for name in col_names if name in cols][0]

	cols_to_keep = ['Date', 'Offense Type', col_name]
	cols_to_drop = [col for col in cols if col not in cols_to_keep]

	df.drop(columns=cols_to_drop, inplace=True)
	df.rename(columns={"Offense Type": "Offense", col_name: "#"}, 
		      inplace=True)

	df.loc[(df["Offense"] == 'Aggravated Assault'), "Offense"] = 'Assault'


def drop_extraneous_rows(df):

	"""
	function taking Pandas dataframe created from Excel file
	and dropping null/non-violent rows
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

		temp = list()

		if str(date)[4] == '-':
			temp = [int(item) for item in str(date).split(' ')[0][0:7].split('-')]
			dates.append(temp)

		elif str(date)[2] == '/':
			temp = [int(str(date).split('/')[2]), int(str(date).split('/')[0])]
			dates.append(temp)

	return dates


def reformat_date_columns(dates, df):

	# drop rows with missing date values 'NaT'
	date_bools = pd.isna(df["Date"])
	df.drop(date_bools[date_bools==True].index, axis=0, inplace=True)

	# replace original date entries in df with numeric lists
	df["Year"] = [date[0] for date in dates]
	df["Month"] = [date[1] for date in dates]
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


def moving_average(n_w, w_size, data):

	out_arr = list()

	w_idxs = [[w_size*i, w_size*(i+1)] for i in range(n_w)]

	for idx in range(0, n_w):

		data_array = data[w_idxs[idx][0]:w_idxs[idx][1]]
		data_mean = np.mean(data_array)
		out_arr.append(data_mean)

	return out_arr



def monthly_plot_with_moving_average(df_in, offense):

	months, win_size = 105, 5
	n_win = int(months/window_size)

	plotname = f"moving_average_trend_{offense}.png"

	df_offense = df_in[df_in["Offense"]==offense]
	trunc_month_data = list(df_offense["#"])[0:months]
	num_data, indices = trunc_month_data, list(df_offense.index)
	month_data = [item//(win_size-1) for item in indices][0:months]

	average_num_data = moving_average(n_win, win_size, num_data)
	average_month_data = moving_average(n_win, win_size, month_data)

	# plot the averaged data
	x, y = average_month_data, average_num_data
	plt.xlabel("Months after June 2009")
	plt.ylabel(f"Reported Incidents of {offense}")
	plt.plot(x,y, color='blue', linewidth = 1.5, marker='', 
		     markerfacecolor='blue', markersize=4)

	# add the monthly data to plot

	x, y = list(range(months)), num_data
	plt.plot(x,y, color='red', linewidth = 0.4, marker='o', 
		     markerfacecolor='red', markersize=4)


	plt.savefig(plotname)
	plt.close()



# DATA COLLECTION


