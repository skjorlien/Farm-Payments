import os
import numpy as np
import pandas as pd
import pickle

'''
Takes 'raw/all-data.pkl', which is raw excels converted to csv, aggregated, and pickled. 
Outputs 'clean/all-data.pkl'
'''


RAWDATA = os.path.join(os.getcwd(), 'data', 'raw', 'all-data.pkl') 
CLEANDATA = os.path.join(os.getcwd(), 'data', 'clean', 'all-data.pkl') 

## Define a dictionary of column names for renaming/keeping
COLNAMES = {
    'FIP': 'FIP',
    'year': 'year',
    'State Abbreviation': 'state',
    'County FSA Name': 'county',
    'Formatted Payee Name': 'customer',
    'Delivery Address Line': 'address',
    'Address Information Line': 'addressInfo',
    'City Name': 'city',
    'Zip Code': 'zip',
    'Disbursement Amount': 'payment',
    'Payment Date': 'paymentDate',
    'Accounting Program Code': 'programCode',
    'Accounting Program Description': 'programName',    
    'Accounting Program Year': 'programYear',
    'Delivery Point Bar Code': 'deliveryBarCode',
    'Delivery Address': 'deliveryAddress'
}

### Define Cleaner Functions
def generate_FIP(df):
    df['State FSA Code'] = df['State FSA Code'].astype('str').str.zfill(2)
    df['County FSA Code'] = df['County FSA Code'].astype('str').str.zfill(3)
    df['FIP'] = df['State FSA Code'] + df['County FSA Code']    
    return df

def extract_year(df):
    tmp = pd.to_datetime(df['Payment Date'], format='mixed')
    df['year'] = tmp.dt.year
    return df

def extract_zip(df):
    split = df['Zip Code'].str.split('-', n=2, expand=True)
    df['Zip Code'] = split[0]
    return df

def rename_and_select(df):
    df = df.rename(columns=COLNAMES)
    df = df[COLNAMES.values()]
    return df

def round_programYear(df):
    df['programYear'] = df['programYear'].fillna(0)
    df['programYear'] = df['programYear'].astype(int)
    return df

cleaners = [generate_FIP, extract_year, extract_zip, rename_and_select]
formatters = [round_programYear]


if __name__ == '__main__':
    # Read In Data
    file = open(RAWDATA, 'rb')
    df = pd.DataFrame(pickle.load(file))
    
    # Run Cleaners 
    for cleaner in cleaners: 
        df = cleaner(df)

    for formatter in formatters: 
        df = formatter(df)

    # Check the Output
    print(df.head())

    # Save it. 
    with open(CLEANDATA, 'wb') as pickle_file:
        pickle.dump(df, pickle_file)
