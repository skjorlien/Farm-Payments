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
    '''
    Returns:
        dask DataFrame: Transaction level data. Although it aggregates by 'level' if passed as kwarg
    '''
    path = settings.PARQUET_FOIA if source=='FOIA' else settings.PARQUET_Public

    df = dd.read_parquet(f"{path}")

    df = aggregate(df, groupby=groupby, **kwargs)
    return df


if __name__ == "__main__":
    import time 
    import matplotlib.pyplot as plt

    df = load_data()
    print(df.columns)
    programs = []
    
    for prog in list(df['programName'].unique()): 
        if prog is None:
            continue
        if prog.find('LIVESTOCK') >= 0:
            programs.append(prog)
    print(programs)
    lips = ['05 - 07 LIVESTOCK INDEMNITY PROGRAM', 'LIVESTOCK INDEMNITY PROGRAM', 'LIVESTOCK INDEMINITY PAYMENTS PROGRAM']

    df = df.loc[df['programName'].isin(lips), ['programName', 'year', 'payment']]
    df = df.groupby(['year']).agg({'payment': 'sum'}).compute().reset_index()


    # # take the two LIPs 
    fig, ax = plt.subplots()
    ax.bar(df['year'], df['payment']/1e6)
    # df.plot.bar(x='year', y = ['LIVESTOCK INDEMINITY PAYMENTS PROGRAM', 'LIVESTOCK INDEMNITY PROGRAM'], ax=ax)
    ax.set_ylabel('Millions $')
    ax.set_xlabel('Year')
    ax.set_title('Livestock Indemnity Program')
    plt.show()


