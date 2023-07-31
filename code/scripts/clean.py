import os
import numpy as np
import pandas as pd
import pickle
import dask.dataframe as dd
from itertools import chain
'''
Takes 'raw/all-data.pkl', which is raw excels converted to csv, aggregated, and pickled. 
Outputs 'clean/all-data.pkl'
'''

RAWDATA = os.path.join(os.getcwd(), 'data', 'raw', 'all-data.pkl') 
CLEANDATA = os.path.join(os.getcwd(), 'data', 'clean', 'all-data.pkl') 

## Define a dictionary of column names for renaming/keeping
COLNAMES = {
    'FIP': 'FIP',
    'State FSA Code': 'statecode',
    'County FSA Code': 'countycode',
    'year': 'year',
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
}

DTYPES = {
    "State FSA Code":'int64', 
    "State FSA Name":'object',
    "County FSA Code":'int64',
    "County FSA Name":'object',
    "Formatted Payee Name":'object',
    "Address Information Line":'object',
    "Delivery Address Line":'object',
    "City Name":'object',
    "State Abbreviation":'object',
    "Zip Code" :'object',
    "Delivery Point Bar Code" :'object',
    "Disbursement Amount" :'float64',
    "Accounting Program Code" :'object',
    "Accounting Program Description" :'object',
    "Accounting Program Year" :'float64'
}

### Define Cleaner Functions
def generate_FIP(df):
    df['State FSA Code'] = df['State FSA Code'].astype('str').str.zfill(2)
    df['County FSA Code'] = df['County FSA Code'].astype('str').str.zfill(3)
    df['FIP'] = df['State FSA Code'] + df['County FSA Code']    
    return df

def extract_year(df):
    # tmp = pd.to_datetime(df['Payment Date'], format='mixed')
    df['Payment Date'] = dd.to_datetime(df['Payment Date'], format='mixed')
    df['year'] = df['Payment Date'].dt.year
    return df

def extract_zip(df):
    df['Zip Code'] = df['Zip Code'].map(lambda x: str(x).split('-', 2)[0])
    # split = df['Zip Code'].str.split('-', n=2, expand=True).compute()
    # print(split)
    # df['Zip Code'] = split[0]
    return df

def rename_and_select(df, column_dict=COLNAMES):
    df = df.rename(columns=column_dict)
    df = df[list(column_dict.values())]
    return df

def round_programYear(df):
    df['programYear'] = df['programYear'].fillna(0)
    df['programYear'] = df['programYear'].astype(int)
    return df

cleaners = [generate_FIP, extract_year, extract_zip, rename_and_select]
formatters = [round_programYear]

def visualize_header_discrepancies():
    datapath = "./data/raw/Public"
    csvfiles = [x for x in os.listdir(f"{datapath}") if (os.path.splitext(x)[1] == '.csv') and (os.path.splitext(x)[0] != 'header_analysis')]
    headers = []
    # First loop, get unique header names for all files.
    for csv in csvfiles: 
        df = pd.read_csv(f"{datapath}/{csv}", nrows=1)
        headers.append(list(df.columns))
    headers = list(set(chain.from_iterable(headers)))
    # Second loop, create viz of which files have which headers. 
    print(headers)
    out = {'colnames': headers}
    for csv in csvfiles: 
        df = pd.read_csv(f"{datapath}/{csv}", nrows=1)
        pos = [list(df.columns).index(h) if h in df.columns else None for h in headers]
        out[csv] = pos
    # print(out)
    outdf = pd.DataFrame.from_dict(out, orient='index')
    outdf.to_csv(f'{datapath}/header_analysis.csv')    

    # TODO: just give a simple output that says whether or not there are any remaining discrepancies.
    return


def merge_columns(fromcols, tocol):
    datapath = "./data/raw/Public"
    csvfiles = [x for x in os.listdir(f"{datapath}") if (os.path.splitext(x)[1] == '.csv') and (os.path.splitext(x)[0] != 'header_analysis')]

    for csv in csvfiles: 
        df = pd.read_csv(f"{datapath}/{csv}")
        for fromcol in fromcols:
            if fromcol in df.columns:
                df.rename(columns={fromcol: tocol}, inplace=True)
        df.to_csv(f"{datapath}/{csv}")
    return


def coerce_dates(colname = 'Payment Date'):
    datapath = "./data/raw/Public"
    csvfiles = [x for x in os.listdir(f"{datapath}") if (os.path.splitext(x)[1] == '.csv') and (os.path.splitext(x)[0] != 'header_analysis')]
    for csv in csvfiles: 
        df = pd.read_csv(f"{datapath}/{csv}")
        df[colname] = pd.to_datetime(df[colname], format='mixed')
        df.to_csv(f"{datapath}/{csv}")


def standardize_csv_headers():
    ''' 
    I had to write this to address some small discrepancies in column names so that dask can 
    save parquet without metadata conflicts. May have to re-address if bringing in new data or just handle case-by case.
    '''

    datapath = f"{os.getcwd()}/data/raw"
    column_names = [
                    'Unnamed: 0', 
                    'State FSA Code',
                    'State FSA Name',
                    'County FSA Code',
                    'County FSA Name',
                    'Formatted Payee Name',
                    'Address Information Line',
                    'Delivery Address Line',
                    'City Name',
                    'State Abbreviation',
                    'Zip Code',
                    'Delivery Point Bar Code',
                    'Disbursement Amount',
                    'Payment Date',
                    'Accounting Program Code',
                    'Accounting Program Description',
                    'Accounting Program Year']

    csvfiles = [x for x in os.listdir(f"{datapath}") if os.path.splitext(x)[1] == '.csv']
    for file in csvfiles:
        df = pd.read_csv(f"{datapath}/{file}")
        df = df.iloc[:, :17]
        df.columns = column_names
        df.to_csv(f"{datapath}/{file}")


def keep_headers():
    headers = ['Disbursement Amount',
            'State Abbreviation', 
            'Delivery Address Line', 
            'Delivery Point Bar Code',
            'State FSA Code',
            'Formatted Payee Name',
            'Zip Code',
            'Accounting Program Description',
            'State FSA Name',
            'Address Information Line',
            'County FSA Name',
            'Accounting Program Year',
            'Accounting Program Code',
            'Payment Date',
            'City Name',
            'County FSA Code']

    datapath = "./data/raw/Public"
    csvfiles = [x for x in os.listdir(f"{datapath}") if (os.path.splitext(x)[1] == '.csv') and (os.path.splitext(x)[0] != 'header_analysis')]
    for csv in csvfiles: 
        df = pd.read_csv(f"{datapath}/{csv}")
        df = df[df.columns.intersection(headers)]
        df.to_csv(f"{datapath}/{csv}")


if __name__ == '__main__':
    datapath = f"{os.getcwd()}/data"  
    # standardize_csv_headers()
    df = dd.read_csv(f"{datapath}/raw/Public/*.csv", dtype=DTYPES)
    
    # Run Cleaners 
    for cleaner in cleaners: 
        df = cleaner(df)

    for formatter in formatters: 
        df = formatter(df)

    # Add State Abbreviations
    states = pd.read_csv('./data/raw/state_codes.csv')
    states['statecode'] = states['statecode'].astype('str').str.zfill(2)
    df = df.merge(states, how='left', on=['statecode'])


    savedir = f"{datapath}/clean/parquet-Public"
    if not os.path.exists(savedir):
        os.mkdir(savedir)

    df.to_parquet(savedir)

    ###### EXTRA STUFF TO GET CSV HEADERS IN ALIGNMENT ###########
    # coerce_dates()

    # merge_columns(['Customer Name', 'Formatted Payee Name'], 'Formatted Payee Name')
    # merge_columns(['Payment Request Amount', 'Disbursement Amount'], 'Disbursement Amount')
    # merge_columns(['Delivery Address', 'Delivery Address Line'], 'Delivery Address Line')
    # keep_headers()
    # visualize_header_discrepancies()
