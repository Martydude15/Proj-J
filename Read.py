import pandas as pd
import numpy as np

# Makes csv for mu and msp
def read_funct():

    maf_actions = pd.read_csv("HtM_MAF Data_Final.csv",
                              dtype={'Transaction Code': str,
                                     'Aircraft': 'category',
                                     'Transaction Code': 'category',
                                     'Malfunction Code': 'category',
                                     'Action Taken Code': 'category'})

    maf_actions.dropna(how='all', inplace=True)

    maf_actions['Corrosion'] = maf_actions['Corrosion'] == 'Yes'

    maf_actions['Bare Metal'] = maf_actions['Bare Metal'] == 'Yes'

    maf_actions['Corrosion Prevention Treatment'] = maf_actions['Corrosion Prevention Treatment'] == 'Yes'

    maf_actions['Routine Maintenance'] = maf_actions['Routine Maintenance'] == 'Yes'

    maf_actions['Unscheduled Maintenance'] = maf_actions['Unscheduled Maintenance'] == 'Yes'

    maf_actions['Mission-Related Maintenance'] = maf_actions['Mission-Related Maintenance'] == 'Yes'

    maf_actions['Failure'] = maf_actions['Failure'] == 'Yes'

    maf_actions['Received Date'] = pd.to_datetime(maf_actions['Received Date'])

    maf_actions['Completion Date'] = pd.to_datetime(
        maf_actions['Completion Date'])

    print(maf_actions.dtypes)

    print(maf_actions.head())

    maf_actions.to_csv("maf_clean.csv")

    mu_data = pd.read_csv('HTM_MSP_Final.csv',
                          header=0,
                          names=['Aircraft', 'Squadron', 'Lot',
                                 'MSP', 'ZULU_Time', 'Flight_Mode'],
                          dtype={'Aircraft': 'category',
                                 'Squadron': 'category',
                                 'Lot': 'category',
                                 'MSP': 'category',
                                 'Flight_Mode': 'category'})

    mu_data['Fault Date'] = pd.to_datetime(
        mu_data['ZULU_Time'].str.split(" ", n=2, expand=True)[1])
    
    mu_data['Fault Time'] = pd.to_datetime(mu_data['ZULU_Time'].str.split(
        " ", n=2, expand=True)[2] , format='%H:%M:%S:%f'  ,errors='coerce').dt.time
    
#    mu_data['Fault Time'] = pd.to_datetime(mu_data['ZULU_Time'].str.split(
#        " ", n=2, expand=True)[2]).strptime('%H:%M:%S:%f').dt.time

    print(mu_data.dtypes)

    print(mu_data.head())

    mu_data.to_csv("mu_clean.csv")


read_funct()
