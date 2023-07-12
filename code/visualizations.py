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

## Settings/Helpers
milFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e6) + ' M')
thouFormatter = FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1e3) + ' T')

## Annual Payment Totals by Program
def payment_trend(df, program = 'Annual Totals'):
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


def top_programs_by_year_by_county(df, year = None, FIP = None, headnum = 10): 
    if FIP is None:
        aggdf = aggregate(df, groupby=['programCode', 'programName', 'year'])
    else:
        aggdf = aggregate(df, groupby=['FIP', 'programCode', 'programName', 'year'])
        aggdf = aggdf[aggdf['FIP'] == 'FIP']
    
    if year is not None: 
        aggdf = aggdf[aggdf['year'] == year]

    aggdf.sort_values(by = 'payment', inplace=True, ascending=False)
    print(aggdf.head(headnum)) 


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


if __name__ == '__main__':
    df = load_data()
    fig, ax, fname = county_boxplot(df)
    plt.show()