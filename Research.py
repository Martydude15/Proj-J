import pandas as pd
import numpy as np
import datetime

#Gathers MAF Data
def maf_search():
    maf_datadf = pd.read_csv("HtM_MAF Data_Final.csv" ,
                       dtype = {
                           'Transaction Code': str,
                            'Action Taken Code ' : 'category',
                            'Description of a problem' : 'category',
                            'Correction of Problem': 'category',
                            'Aircraft': 'category',
                            'Transaction Code': 'category',
                            'Malfunction Code': 'category',
                           'Failure' : 'category'
                       })
    maf_datadf.dropna(how = 'all', inplace=True)
    maf_datadf['Received Date'] = pd.to_datetime(maf_datadf['Received Date'])
    maf_datadf['Completion Date'] = pd.to_datetime(maf_datadf['Completion Date'])
    pd.options.display.max_columns = None
    return maf_datadf

# Gathers MSP Data
def msp_search():

    msp_data = pd.read_csv('HTM_MSP_Final.csv',
                      header = 0,
                     names = ['Aircraft', 'Squadron', 'Lot', 'MSP', 'ZULU_Time', 'Flight_Mode'], 
                     dtype = {'Aircraft': 'category',
                             'Squadron': 'category',
                             'Lot': 'category',
                             'MSP': 'category',
                             'Flight_Mode': 'category'})
    
    POSSIBLE_DATE_FORMATS = ['%H:%M:%S:%f', '%H:%M:%S']
    
    msp_data['Fault Date'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" ", n = 2, expand = True)[1])
    
    msp_data['Fault Time'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" "
    , n = 2, expand = True)[2], format = '%H:%M:%S:%f', errors='ignore')#.dt.time  

    msp_data.head()

    return msp_data



maf_data = maf_search()
msp_data = msp_search()



