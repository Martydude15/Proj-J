import warnings; warnings.simplefilter('ignore')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime 
import dask.dataframe as dd


maf_data = pd.read_csv("HtM_MAF Data_Final.csv", index_col=None,
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

# Gets unique values in problem description
#  ['Perform system or component checks ' 'Perform a periodic inspection'
#  'Perform a phase inspection' ... 'Code: VBQGZJ was set.'
#  'Code: QGVBUX was set.' 'Code: VBMQZJ was set.']
print(len(maf_data['Description of Problem'].unique().tolist()) , 'Unique Values in Desc. of Problem')
print(len(maf_data['Job Code'].unique().tolist()) , 'Unique Values in Job Codes')
print(len(maf_data['Correction of Problem'].unique().tolist()) , 'Unique Values in Correction of Problem')
print(len(msp_data['MSP'].unique().tolist()) , 'Unique Values in MSP')
print(len(msp_data['Flight_Mode'].unique().tolist()), 'Unique Flight_Control Values')
print(len(msp_data['Lot'].unique().tolist()))

maf_data.info()
msp_data.info()