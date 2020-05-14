 # import dependencies
 # C:/ProgramData/Anaconda3/Scripts/activate
# conda activate base
import os, io
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import seaborn as sns
import plotly.figure_factory as ff
from sklearn.ensemble import RandomForestRegressor 

# plt.rcParams['figure.figsize'] = 10,6
import warnings 
warnings.filterwarnings("ignore")





maf_data = pd.read_csv("HtM_MAF_Data_Aircraft1.csv", header=0, 
                         dtype = {'Transaction Code': str,
                                 'Aircraft': 'category',
                                 'Transaction Code': 'category',
                                 'Malfunction Code': 'category',
                                 'Action Taken Code': 'category'}) 

msp_data = pd.read_csv("mu_clean.csv")

msp_data['Fault Date'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" ", n = 2, expand = True)[1])

msp_data['Fault Time'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" ", n = 2, expand = True)[2], format = '%H:%M:%S:%f', errors= 'ignore')
msp_data['Fault Time'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" ", n = 2, expand = True)[2], format = '%H:%M:%S', errors= 'ignore')

msp_data['action_year'] = pd.DatetimeIndex(msp_data['Fault Date']).year

maf_data['received_date'] = pd.to_datetime(maf_data['received_date'])

maf_data['completion_date'] = pd.to_datetime(maf_data['completion_date'])

maf_data["action_month"] = pd.DatetimeIndex(maf_data['completion_date']).month

maf_data["action_year"] = pd.DatetimeIndex(maf_data['completion_date']).year



print(maf_data)

maf_data.reset_index()
msp_data.reset_index()

print(list(maf_data.columns.values))
print(list(msp_data.columns.values))

#maf_data = maf_data.reindex(columns=['job_code', 'aircraft', 'transaction_code', 'malfunction_code', 'action_taken_code', 'description_of_problem', 'correction_of_problem'])

combined_data = maf_data.merge(msp_data[['Aircraft', 'action_year', 'MSP', 'Flight_Mode', 'Fault Date']], how = 'left', on = ['action_year', 'aircraft'])

#msp_data.loc[:, maf_data.columns] = maf_data

print(combined_data)

# x= data.iloc[: ,7:8].values

# print(x)

# y= data.iloc[: , 11].values

# regressor = RandomForestRegressor(n_estimators = 100, random_state = 0) 

# regressor.fit(x, y)   