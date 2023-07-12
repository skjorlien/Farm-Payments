import os
import pickle
import pandas as pd
import numpy as np
import settings

# Cleaning TODO: 
# - are there any errors in FIP codes?
# - there is a decision to be made about programYear vs year
# - right now, I grab year from payment date, not accounting program year. Is that right?
# Why are payments in 2022 $0 across the board?


def aggregate(df, groupby = []):
    '''aggregate Takes a dataframe and a list of groupings ('level') and aggregates data by 
    summing over payments, and counting unique payees

    Args:
        df (pandas DataFrame): Should just be the cleaned, unaggregated pickled data
        groupby: a list of columns to pass to pandas method `groupby`

    Returns:
        pandas DataFrame: Aggregated by groupby cols
    '''
    aggFn = {
                'payment': 'sum',
                'customer': pd.Series.nunique,
            }

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
def load_data(groupby = []):
    '''load_data opens the pickled, clean transaction-level data

    Returns:
        pandas DataFrame: Transaction level data. Although it aggregates by 'level' if passed as kwarg
    '''
    file = open(settings.CLEANDATA, 'rb')
    df = pd.DataFrame(pickle.load(file))
    df = aggregate(df, groupby=groupby)
    return df


if __name__ == "__main__":

    df = load_data()

    print(df[df['county'] == 'Baldwin']['state'].iloc[0])