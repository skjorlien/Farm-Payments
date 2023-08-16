from datasets import load_data, aggregate
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
import os
from matplotlib.ticker import FuncFormatter
import settings 
from utils import utils 
import glob
import dask.dataframe as dd

'''
All visualizations should take transaction-level dask df with arguments 
for filtering. 

All visualizations should return fig, ax, fname (file name)
All tables should return latex, fname
'''


## Settings/Helpers
milFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e6) + ' M')
thouFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e3) + ' T')

## Annual Payment Totals by Program
def payment_trend(df: dd.DataFrame, program = 'Annual Totals'):
    aggdf = aggregate(df, groupby=['programCode', 'programName', 'year'])
    AnnualTot = aggregate(df, groupby=['year'])
    AnnualTot['programName'] = 'Annual Totals'
    aggdf = dd.concat([aggdf, AnnualTot]).compute()
    minyear = np.min(aggdf['year'])
    maxyear = np.max(aggdf['year'])
    filt_df = aggdf[aggdf['programName'] == program]
    y = filt_df['payment']
    x = filt_df['year']

    fig, ax = plt.subplots()
    ax.bar(x, y)
    ax.set_xlabel('Year')
    ax.set_ylabel('Payments')
    ax.set_xlim(minyear, maxyear)
    ax.set_ylim(0, np.max(y))
    ax.set_title(f'Annual Payment by Program - {program}')

    fname = f'plot_annualTrend_{program}.png'
    return fig, ax, fname


def top_programs_by_year_table(df: dd.DataFrame, FIP=None, headnum = 3): 
    '''top_programs_by_year_table 

    Args:
        aggdf (pd.DataFrame): Should be dataframe already aggregated by ['year', 'programName'] with only 'payment' summary col
        FIP (int, optional): if provided, filter the aggregate
        headnum (int, optional): Number of 'top n' programs to display for each year. Defaults to 3.

    Returns:
        str: a Formatted latex table. Saving to be done on the outside.
    '''
    if FIP: 
        df = df[df['FIP'] == FIP]
        county = df['county'].compute().iloc[0]
        state = df['state'].compute().iloc[0]
        fname =  f"{county}-{state}_prog-table.tex"
    else: 
        fname = "all-counties_prog-table.tex"

    aggdf = aggregate(df, groupby=['year', 'programName'])

    summarytable = aggdf.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
    summarytable = summarytable.compute()
    summarytable.drop(columns='year', inplace=True)
    summarytable.index = summarytable.index.droplevel(level=1)
    summarytable.set_index('programName', append=True, inplace=True)
    return summarytable.to_latex(
            float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
        ), fname


def top_states_by_year_table(df: dd.DataFrame, headnum = 5):
    '''top_states_by_year_table 

    Args:
        aggdf (pd.DataFrame): Should be dataframe already aggregated by ['year', 'statecode'] with only 'payment' summary col
        headnum (int, optional): Number of 'top n' programs to display for each year. Defaults to 3.

    Returns:
        str: a Formatted latex table. Saving to be done on the outside.
    '''
    aggdf = aggregate(df, groupby=['year', 'stateabbr'])
    summarytable = aggdf.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
    summarytable = summarytable.compute()
    summarytable.drop(columns='year', inplace=True)
    summarytable.index = summarytable.index.droplevel(level=1)
    summarytable.set_index('stateabbr', append=True, inplace=True)
        
    fname = "top_states_table.tex"
    return summarytable.to_latex(
        float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
    ), fname


def top_counties_by_year_table(df: dd.DataFrame, headnum = 5):
    aggdf = aggregate(df, groupby = ['year', 'FIP'])
    cnty_state_crswlk = df.drop_duplicates(subset=['FIP'])[['FIP', 'county', 'state']]
    cnty_state_crswlk = cnty_state_crswlk.compute()
    cnty_state_crswlk['cntyState'] = cnty_state_crswlk.apply(lambda x: f"{x['county']}, {x['state']}", axis=1)
    cnty_state_crswlk.drop(columns=['county','state'], inplace=True)
    
    summarytable = aggdf.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
    summarytable = summarytable.compute()
    summarytable = summarytable.merge(cnty_state_crswlk, how='left')
    summarytable.set_index('year', inplace=True)
    summarytable.drop(columns=['FIP'], inplace=True)
    summarytable.rename(columns = {'cntyState': 'County'}, inplace=True)
    summarytable.set_index('County', append=True, inplace=True)

    fname = "top_counties_table.tex"

    return summarytable.to_latex(
        float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
    ), fname


def program_boxplot(df: dd.DataFrame, prog = None, log=True):
    if prog is None: 
        data = df
        prog = "All Programs"
    else: 
        data = df[df['programName'] == prog]

    years = range(2006, 2022)
    if log:
        dta = [data[data['year'] == year]['logpayment'] for year in years]
    else: 
        dta = [data[data['year'] == year]['payment'] for year in years]

    # Generate a list of dataseries, one for each year
    fig, ax = plt.subplots(figsize=(12,8)) 
    ax.boxplot(dta, vert=True, labels = years)
    ax.yaxis.grid(True)
    ax.set(title = prog,
        xlabel = 'Year',
        ylabel = 'Payment ($)')
    fig.tight_layout()

    fname = "".join(i for i in prog if i not in "\/:*?<>|") + "_boxplot.png"
    return fig, ax, fname


def county_boxplot(df: dd.DataFrame, FIP = None, log=True): 
    if FIP is None: 
        data = df 
        state = None
        county = "All Counties"
    else:
        data = df[df['FIP'] == FIP]
        state = data['state'].compute().iloc[0]
        county = data['county'].compute().iloc[0]

    years = range(2006, 2022)
    
    if log:
        dta = [data[data['year'] == year]['logpayment'] for year in years]
    else: 
        dta = [data[data['year'] == year]['payment'] for year in years]

    fig, ax = plt.subplots(figsize= (12,8))
    ax.boxplot(dta, vert=True, labels=years) 
    ax.yaxis.grid(True) 
    title = f"{county}, {state}" if state is not None else f"{county}"
    ax.set(title = title,
           xlabel='Year', 
           ylabel='Payment ($)')
    fig.tight_layout() 
    fname =  f"{county}-{state}_boxplot.png"
    return fig, ax, fname


''''
COUNTY-LEVEL Chloropleth
'''
def us_county_map(df: dd.DataFrame, legend_fmt = None, variable=None, year=2020, **kwargs):
    df = aggregate(df, groupby=['FIP', 'year'], customer=True)
    df = df[df['year'] == year]
    df['concentration'] = df['payment'] / df['customer']
    if variable in ['payment', 'concentration', 'customer']:
        pass 
    else: 
        raise ValueError('must provide a variable like payment, concentration, or customer') 

    cols = list(df.columns)
    vmin = kwargs.get('vmin', None)
    vmax = kwargs.get('vmax', None)

    # make sure df has two columns: FIP and one more for chloroplething
    if len(cols) != 2: 
        raise ValueError('Expected two column data frame with columns [FIP, mapdim]')

    if 'FIP' not in cols:
        raise ValueError('FIP must be a column')
    
    # extract the map dimension
    fipi = cols.index('FIP')
    dimi = int(not bool(fipi))
    mapdim = cols[dimi]

    # Load Maps
    county_map = gpd.read_file('data/basemaps/us_county.zip')
    county_map = county_map[['STATEFP', 'COUNTYFP', 'NAME', 'NAMELSAD', 'geometry']]
    county_map = county_map.cx[-126:-66, 22:50]
    county_map['FIP'] = county_map['STATEFP'] + county_map['COUNTYFP']

    state_map = gpd.read_file('data/basemaps/us_state.zip')
    state_map = state_map.cx[-126:-66, 22:50]
    
    # merge a data series 
    county_map = county_map.merge(df, on='FIP', how='left')

    legend = {'format': legend_fmt}

    # Plot it
    fig, ax = plt.subplots()
    county_map.plot(ax=ax, 
                    column = mapdim,
                    legend = True, 
                    vmin = vmin, 
                    vmax = vmax,
                    legend_kwds=legend,
                    missing_kwds={
                    'color': 'lightgrey',
                    'label': 'No payments'
                    }
                    )
    state_map.boundary.plot(ax=ax, color = 'black', linewidth=1)
    ax.set_axis_off()
    return fig, ax,


def mapper_wrapper(const_colorbar = False, years = range(2006, 2022), dims = ['payment', 'customer', 'concentration']):
    formatter = {
        'payment': milFormatter, 
        'customer': thouFormatter,
        'concentration': thouFormatter
    }
    df = load_data(groupby=['FIP', 'year'])
    df['concentration'] = df['payment'] / df['customer']
    for dim in dims:
        maxscale = np.max(df[dim])
        minscale = np.min(df[dim])
        
        for year in years:
            # filter the data to this dimension and year
            tdf = df[['FIP', dim]][df['year'] == year]

            # prep settings
            conststr = '_const' if const_colorbar else ''
            fname = f"map_cnty_{dim}_{year}{conststr}.png"
            constdict = {'vmin': minscale, 'vmax': maxscale} if const_colorbar else {}
            lfmt = formatter[dim]

            # get map
            fig, ax = us_county_map(tdf, legend_fmt = lfmt, **constdict)
            conststr = ' -- Constant Colorbar' if const_colorbar else ''
            ax.set_title(f"{dim} -- {year}{conststr}")
            plt.savefig(os.path.join(settings.OUTDIR, fname))
            plt.close()


def gif_all():
    '''gif_all _summary_
    '''
    scopes = ['concentration', 'payment', 'customer']
    conststrs = ['_const', '']
    for scope in scopes:
        for conststr in conststrs:
            imgs = glob.glob(f"{settings.OUTDIR}/map_cnty_{scope}_20??{conststr}.png")
            imgs = sorted(imgs)
            fname = f"map_GIF_{scope}{conststr}.gif"
            utils.gifify(imgs, fname)


## Data Difference Visualizations
def annualized_summary_stats(source):
    df = load_data(source = source)
    df = df[['year', 'payment']]
    df = df.groupby(['year']).agg(['mean', 'max', 'sum', 'std', 'count'])
    df = df.compute()
    df.sort_index(inplace=True)
    fname = f'{source}_summary_table.tex'    
    return df.to_latex(
        float_format=lambda x: "\$ {:,.2f}".format(x),
        caption = f"Summary Stats from {source} data",
        position = 'H'
    ), fname


def program_year_mean_pmt():
    foia_data = load_data(source="FOIA", groupby=['programCode', 'year'])
    public_data = load_data(source="Public", groupby=['programCode', 'year'])
    data = public_data.merge(foia_data, how='outer', on=['programCode', 'year'])
    data = data.compute()
    data.fillna(0, inplace=True)
    data['diff'] = data['payment_x'] - data['payment_y']
    data.drop(columns=['payment_x', 'payment_y'], inplace=True)
    data['programCode'] = data['programCode'].str.replace('&', 'and')
    data = data.pivot(columns='year', index='programCode', values='diff')
    data.fillna(0, inplace=True)
    fname = "program_year_mean_diff.tex"
    return data.to_latex(
        float_format=lambda x: "\$ {:,.0f}".format(x/1000000),
        longtable = True,
        label = "progYearDiffTable",
        caption = "Public minus FOIA in Millions of Dollars by Program Code"
    ), fname


def state_year_mean_pmt():
    foia_data = load_data(source="FOIA", groupby=['stateabbr', 'year'])
    public_data = load_data(source="Public", groupby=['stateabbr', 'year'])
    data = public_data.merge(foia_data, how='outer', on=['stateabbr', 'year'])
    data = data.compute()
    data.fillna(0, inplace=True)
    data['diff'] = data['payment_x'] - data['payment_y']
    data.drop(columns=['payment_x', 'payment_y'], inplace=True)
    data = data.pivot(columns='year', index='stateabbr', values='diff')
    data.fillna(0, inplace=True)
    fname = "state_year_mean_diff.tex"
    return data.to_latex(
        float_format=lambda x: "{:,.0f}".format(x/1000000),
        longtable = True,
        label = "stateYearDiffTable",
        caption = "Public minus FOIA in Millions of Dollars by State"
    ), fname


def payment_distribution_by_year(df, year=2020, source='Public'):
    df = df[df['year'] == year]
    df = df.compute()
    df['month'] = df['paymentDate'].dt.month
    aggdf = df.groupby('month').count()
    fig, ax = plt.subplots()
    ax.bar(aggdf.index, aggdf['FIP'])
    ax.set_title(f"{source} - {year}")
    fname = f"{source}_data_dist_{year}"
    return fig, ax, fname


def reference_prog_code_name():
    df = load_data()
    df = df.groupby(['programCode', 'programName']).count()
    df = df.compute()
    df.drop(columns = df.columns, inplace=True)
    print(df.head())
    fname = "program_reference.tex"
    return df.to_latex(
        longtable=True, 
        label='progCodeLookup',
        caption='Program Code / Name Lookup Table'
    ), fname

if __name__ == '__main__':
    sources = ['FOIA', 'Public']
    # for source in sources: 
    #     tex, fname = annualized_summary_stats(source)
    #     with open(os.path.join(settings.OUTDIR, 'tables', fname), 'w') as f:
    #         f.write(tex)

    # tex, fname = program_year_mean_pmt()
    # with open(os.path.join(settings.OUTDIR, 'tables', fname), 'w') as f:
    #     f.write(tex)

    # tex, fname = state_year_mean_pmt()
    # with open(os.path.join(settings.OUTDIR, 'tables', fname), 'w') as f:
    #     f.write(tex)


    # plt.switch_backend('agg')
    # years = range(2004, 2023)
    # for source in sources:
    #     df = load_data(source = source)
    #     for year in years:
    #         print(f"Producing {source} {year}")
    #         fig, ax, fname = payment_distribution_by_year(df, year, source)
    #         plt.savefig(os.path.join(settings.OUTDIR, 'figures', 'annual_source_distribution', fname))
    #         plt.close()


    tex, fname = reference_prog_code_name()
    with open(os.path.join(settings.OUTDIR, 'tables', fname), 'w') as f:
        f.write(tex)

