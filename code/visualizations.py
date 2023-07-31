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

## Settings/Helpers
milFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e6) + ' M')
thouFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e3) + ' T')

## Annual Payment Totals by Program
def payment_trend(program = 'Annual Totals'):
    df
    aggdf = aggregate(df, groupby=['programCode', 'programName', 'year'])
    AnnualTot = aggregate(df, groupby=['year'])
    AnnualTot['programName'] = 'Annual Totals'
    aggdf = pd.concat([aggdf, AnnualTot])
    
    minyear = np.min(aggdf['year'])
    maxyear = np.max(aggdf['year'])

    filt_df = aggdf[aggdf['programName'] == program]
    y = filt_df['payment']
    x = filt_df['year']

    fig, ax = plt.subplots()
    ax.plot(x, y)
    ax.set_xlabel('Year')
    ax.set_ylabel('Payments')
    ax.set_xlim(minyear, maxyear)
    ax.set_ylim(0, np.max(y))
    ax.set_title(f'Annual Payment by Program - {program}')

    fname = f'plot_annualTrend_{program}.png'
    return fig, ax, fname

## NOTE: DONE
def top_programs_by_year_table(aggdf: pd.DataFrame, headnum = 3): 
    '''top_programs_by_year_table 

    Args:
        aggdf (pd.DataFrame): Should be dataframe already aggregated by ['year', 'programName'] with only 'payment' summary col
        headnum (int, optional): Number of 'top n' programs to display for each year. Defaults to 3.

    Returns:
        str: a Formatted latex table. Saving to be done on the outside.
    '''
    summarytable = aggdf.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
    summarytable.drop(columns='year', inplace=True)
    summarytable.index = summarytable.index.droplevel(level=1)
    summarytable.set_index('programName', append=True, inplace=True)

    return summarytable.to_latex(
            float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
        )   


def top_states_by_year_table(aggdf: pd.DataFrame, headnum = 5):
   '''top_states_by_year_table 

    Args:
        aggdf (pd.DataFrame): Should be dataframe already aggregated by ['year', 'statecode'] with only 'payment' summary col
        headnum (int, optional): Number of 'top n' programs to display for each year. Defaults to 3.

    Returns:
        str: a Formatted latex table. Saving to be done on the outside.
    '''
   summarytable = aggdf.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
   summarytable.drop(columns='year', inplace=True)
   summarytable.index = summarytable.index.droplevel(level=1)
   summarytable.set_index('stateabbr', append=True, inplace=True)
    
   return summarytable.to_latex(
       float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
   )   


def top_counties_by_year_table(df: pd.DataFrame, headnum = 5):
    aggdf = df.groupby(['year', 'FIP']).agg({'payment': 'sum'}).reset_index()
    cnty_state_crswlk = df.drop_duplicates(subset=['FIP'])[['FIP', 'county', 'state']]
    cnty_state_crswlk['cntyState'] = cnty_state_crswlk.apply(lambda x: f"{x['county']}, {x['state']}", axis=1)
    cnty_state_crswlk.drop(columns=['county','state'], inplace=True)
    
    summarytable = aggdf.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
    summarytable = summarytable.merge(cnty_state_crswlk, how='left')
    summarytable.set_index('year', inplace=True)
    summarytable.drop(columns=['FIP'], inplace=True)
    summarytable.rename(columns = {'cntyState': 'County'}, inplace=True)
    summarytable.set_index('County', append=True, inplace=True)

    return summarytable.to_latex(
        float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
    )   



def program_boxplot(df, prog = None):
    if prog is None: 
        data = df
        prog = "All Programs"
    else: 
        data = df[df['programName'] == prog]

    years = range(2006, 2022)
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


def county_boxplot(df, FIP = None): 
    if FIP is None: 
        data = df 
        state = None
        county = "All Counties"
    else:
        data = df[df['FIP'] == FIP]
        state = data['state'].iloc[0]
        county = data['county'].iloc[0]

    years = range(2006, 2022)
    
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
def us_county_map(df, legend_fmt = None, **kwargs):
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
    return fig, ax


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


def summary_stats(df: dd.DataFrame):
    df = df[['year', 'payment']]
    df = df.groupby(['year']).agg(['mean', 'max', 'sum', 'std', 'count'])
    return df.compute()


def compare_data_sources():
    df1 = load_data(source='FOIA')
    df2 = load_data(source='Public')
    df1['payment'] = np.log(df1['payment'])
    df2['payment'] = np.log(df2['payment'])
    years = range(2004, 2023)
    
    dta1 = [df1[df1['year'] == year]['payment'] for year in years] 
    dta2 = [df2[df2['year'] == year]['payment'] for year in years] 

    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize= (12,8))
    ax1.boxplot(dta1, vert=True, labels=years) 
    ax2.boxplot(dta2, vert=True, labels=years)
    ax1.yaxis.grid(True) 
    ax2.yaxis.grid(True) 
    title = "FOIA vs Public Data"
    # fig.set(title = title,
    #        xlabel='Year', 
    #        ylabel='Payment ($)')
    fig.tight_layout() 
    # fname =  f"{county}-{state}_boxplot.png"
    return fig, (ax1, ax2)

if __name__ == '__main__':
    # sources = ['FOIA', 'Public']
    # for source in sources: 
    #     df = load_data(source=source)
    #     df = summary_stats(df)
    #     df.sort_index(inplace=True)
    #     tex = df.to_latex()
    #     fname = f'{source}_summary_table.tex'
    #     with open(os.path.join(settings.OUTDIR, 'tables', fname), 'w') as f:
    #         f.write(tex)

    fig, ax = compare_data_sources()
    plt.show()