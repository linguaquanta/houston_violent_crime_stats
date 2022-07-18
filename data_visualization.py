
import config
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from config import (base_url, home_url, 
			crime_data_dir, month_dict)
from helper_funcs import (moving_average, 
		monthly_plot_with_moving_average)



######################
# DATA VISUALIZATION #
######################

df = pd.read_pickle("monthly_violent_crime_stats.pkl")
violent_offenses = ['Rape', 'Murder', 'Assault',  'Robbery']
for offense in violent_offenses:
	monthly_plot_with_moving_average(df, offense)








