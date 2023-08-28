import os
import pickle
import pandas as pd
import numpy as np
import settings
import dask.dataframe as dd


def aggregate(df, groupby = [], payment=True, customer = False):
    '''aggregate Takes a dataframe and a list of groupings ('level') and aggregates data by 
    summing over payments, and counting unique payees

    Args:
        df (pandas DataFrame): Should just be the cleaned, unaggregated pickled data
        groupby: a list of columns to pass to pandas method `groupby`

    Returns:
        dask DataFrame: Aggregated by groupby cols
    '''
    nunique = dd.Aggregation(
        name="nunique",
        chunk=lambda s: s.apply(lambda x: list(set(x))),
        agg=lambda s0: s0.obj.groupby(level=list(range(s0.obj.index.nlevels))).sum(),
        finalize=lambda s1: s1.apply(lambda final: len(set(final))),
    )
    aggFn = {} 
    if payment: 
        aggFn['payment'] = 'sum'
    if customer:
        aggFn['customer'] = nunique

    if len(aggFn) == 0:
        raise ValueError('Must aggregate by either payment, customer, or both.')
    
    for col in groupby: 
        if col not in df.columns:
            print(f'Column {col} not found')
            groupby = []
            return df

    if len(groupby) >= 1: 
        df = df.groupby(groupby).agg(aggFn).reset_index()
    else:
        print(f'Returning Transaction-Level Data')

    return df


# load data and aggregate (if 'groupby' is passed as a kwarg)
def load_data(groupby = [], source='Public', **kwargs):
    '''load_data is for playing around with different data loading techniques without messing up other parts of my code.

    Returns:
        dask (or pandas) DataFrame: Transaction level data. Although it aggregates by 'level' if passed as kwarg
    '''

    # file = open(settings.CLEANDATA, 'rb')
    path = settings.PARQUET_FOIA if source=='FOIA' else settings.PARQUET_Public

    df = dd.read_parquet(f"{path}")

    df = aggregate(df, groupby=groupby, **kwargs)
    return df

if __name__ == "__main__":
    import time 

    df = load_data(groupby=['FIP', 'stateabbr', 'year'], customer=True)
    df = df.compute()
    print(df[df['stateabbr'] == 'DC'])
    # df.to_csv(f"{os.getcwd()}/data/clean/year_state_data.csv")

    # states = pd.read_csv('./data/raw/state_codes.csv')
    # states['statecode'] = states['statecode'].astype('str').str.zfill(2)
    # df = df.merge(states, how='left', on=['statecode'])
    # print(df.head())

