import warnings; warnings.simplefilter('ignore')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime 
import dask.dataframe as dd


maf_data = pd.read_csv("HtM_MAF Data_Final.csv", index_col=0,
                       dtype = {'Transaction Code': str,
                                 'Aircraft': 'category',
                                 'Transaction Code': 'category',
                                 'Malfunction Code': 'category',
                                 'Action Taken Code': 'category'})

msp_data = pd.read_csv('HTM_MSP_Final.csv',
                     header = 0,
                     names = ['Aircraft', 'Squadron', 'Lot', 'MSP', 'ZULU_Time', 'Flight_Mode'], 
                     dtype = {'Aircraft': 'category',
                             'Squadron': 'category',
                             'Lot': 'category',
                             'MSP': 'category',
                             'Flight_Mode': 'category'})

maf_data.dropna(how = 'all', inplace=True)

maf_data['Corrosion'] = maf_data['Corrosion'] == 'Yes'

maf_data['Bare Metal'] = maf_data['Bare Metal'] == 'Yes'

maf_data['Corrosion Prevention Treatment'] = maf_data['Corrosion Prevention Treatment'] == 'Yes'

maf_data['Routine Maintenance'] = maf_data['Routine Maintenance'] == 'Yes'

maf_data['Unscheduled Maintenance'] = maf_data['Unscheduled Maintenance'] == 'Yes'

maf_data['Mission-Related Maintenance'] = maf_data['Mission-Related Maintenance'] == 'Yes'

maf_data['Failure'] = maf_data['Failure'] == 'Yes'


maf_data['Received Date'] = pd.to_datetime(maf_data['Received Date'])
maf_data['Completion Date'] = pd.to_datetime(maf_data['Completion Date'])
maf_data.dtypes

msp_data['Fault Date'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" ", n = 2, expand = True)[1])    

msp_data['Fault Time'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" "
    , n = 2, expand = True)[2], format = '%H:%M:%S:%f', errors='ignore')

msp_data['Fault Time'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" "
    , n = 2, expand = True)[2], format = '%H:%M:%S', errors='ignore')


# ------------------------------------------

#print(maf_data.count())
#input("Enter to continue.")

#print(msp_data.count())
#input("Enter to continue.")

# maf_data['Aircraft'].count()
#print(maf_data.Aircraft.count())
#input("Enter to continue.")

#print((maf_data.Aircraft == 1).sum())
#input("Enter to continue.")

#print(maf_data.Aircraft.value_counts)
#input("Enter to continue.")
#print(maf_data['Aircraft'].value_counts)
#input("Enter to continue.")

sns.set(style="darkgrid")
plt.figure(figsize=(16, 10))
ax = sns.countplot(y="Aircraft",linewidth = 0.5 , data = maf_data)

#-----------------------------------------------------------------------------
# Corrosion Begins

corrosion_actions = maf_data[maf_data['Corrosion'] | maf_data['Bare Metal'] | maf_data['Corrosion Prevention Treatment']]

corrosion_actions["action_month"] = pd.DatetimeIndex(corrosion_actions['Completion Date']).month

corrosion_actions["action_year"] = pd.DatetimeIndex(corrosion_actions['Completion Date']).year

msp_data['action_year'] = pd.DatetimeIndex(msp_data['Fault Date']).year

#print(msp_data.head())
#input("Enter to continue.")

#print(msp_data.head())
#input("Enter to continue.")

#print(corrosion_actions.head())
#input("Enter to continue.")

# newb_data = pd.DataFrame(columns=['Aircraft', 'action_year', 'Fault Date'])

# newb_data['Aircraft'] = corrosion_actions['Aircraft']
# newb_data['action_year'] = corrosion_actions['action_year']
# newb_data.loc['Fault Date'] = msp_data.loc['Fault Date']

combined_data = np.array_split(pd.merge(corrosion_actions, msp_data.filter(['Aircraft', 'action_year']), how='left', on = ['Aircraft', 'action_year']), 4)

print(combined_data.head())

print("I'm done")
