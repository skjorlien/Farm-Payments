import pandas as pd 
import geopandas as gpd
import matplotlib.pyplot as plt
import panel as pn 
import numpy as np
import holoviews as hv 
# import geoviews as gv
import hvplot.pandas
from datasets import load_data, aggregate
import visualizations as viz



## Settings
hv.opts.defaults(hv.opts.Curve(axiswise=True, framewise=True, shared_axes=False),                                  
                     hv.opts.Scatter(axiswise=True, framewise=True, shared_axes=False),                                
                     hv.opts.Image(axiswise=True, framewise=True, shared_axes=False),                                  
                     hv.opts.Histogram(axiswise=True, framewise=True, shared_axes=False))


### Load Data
all_df = load_data()

df = aggregate(all_df, level=['programCode', 'programName', 'year'])
AnnualTot = aggregate(all_df, level=['year'])
AnnualTot['programName'] = 'Annual Totals'
aggdf = pd.concat([df, AnnualTot])

gdf = gpd.read_file('data/basemaps/us_county.zip')
gdf = gdf[['STATEFP', 'COUNTYFP', 'NAME', 'NAMELSAD', 'geometry']]
gdf = gdf.cx[-126:-66, 22:50]
gdf['FIP'] = gdf['STATEFP'] + gdf['COUNTYFP']

tdf = aggregate(all_df, level = ['FIP', 'year'])
tdf = tdf[tdf['year'] == 2020]
tempgdf = gdf 
tempgdf = tempgdf.merge(tdf, on='FIP', how='left')


### Build Widgets
minyear = int(np.min(df['year']))
maxyear = int(np.max(df['year']))

## This didn't work because it generates a new widget, which in turn is not bound to the outcome
# of another function.
def dynamic_county_dropdown(state): 
    df = all_df[all_df['state'] == state]
    counties = list(pd.unique(df['county']))
    return pn.widgets.Select(name = 'County', options=counties)


state_dropdown = pn.widgets.Select(name = 'State', options=list(pd.unique(all_df['state'])), value='AL')
county_dropdown = pn.bind(dynamic_county_dropdown, state_dropdown)
year_slider = pn.widgets.IntSlider(value = 2021, start=minyear, end=maxyear, name = "Year")
program_select = pn.widgets.Select(name='Program', options=list(aggdf['programName'].unique()), value='Annual Totals')
prog_code_enter = pn.widgets.TextInput(name='Type a Program Code')

### Define Visualizations
def prog_year_pmt():
    # get a dataframe with Program Code - Payments - # of Active Years
    tdf = df.groupby(['year', 'programCode']).agg(
        {'payment': 'sum'}
        ).reset_index()
    tdf = tdf.groupby(['programCode']).agg(
        {'payment': 'sum',
        'year': 'count'}
    ).reset_index()
    # fig, ax = plt.subplots()
    # ax.scatter(tdf['year'], tdf['payment'])
    # ax.set_xlabel('# of Years With Payment')
    # ax.set_ylabel('Total Payments ($)')
    # ax.set_title('Program Productivity')
    fig = tdf.hvplot.scatter(x='year', y='payment', hover_cols=['programCode']).opts(xlim=(0,12), ylim=(0, np.max(tdf['payment'])))
    return fig


def payment_trend(program = 'Annual Totals'):
    fdf = aggdf[aggdf['programName'] == program]
    y = fdf['payment']
    # fig, ax = plt.subplots() 
    # ax.plot(fdf['year'], y)
    # ax.set_xlabel('Year')
    # ax.set_ylabel('Payments')
    # ax.set_xlim(minyear, maxyear)
    # ax.set_ylim(0, np.max(y))
    # ax.set_title(f'Annual Payment by Program - {program}')
    fig = fdf.hvplot.line(x='year', y='payment').opts(xlim=(2006, 2021), ylim=(0, np.max(fdf['payment'])))
    return pn.panel(fig, width=500)    

def df_filter_progcode(code=None):
    piv_table = aggdf.pivot(columns=['year'], index=['programCode', 'programName'], values='payment').reset_index()
    print(piv_table.head())

    piv_table['programCode'] = piv_table['programCode'].fillna(-1) 
    piv_table['programCode'] = piv_table['programCode'].astype('int').astype('str')
    print(f'code was {code}')
    if code is None or code == '':
        return piv_table
    return piv_table[piv_table['programCode'] == code]

def top_progs_in_county_table(county = 'Baldwin'):
    fip = all_df[all_df['county'] == county.value]['FIP']
    df = all_df[all_df['FIP'] == fip]
    df = aggregate(df, level = ['FIP', 'programCode', 'programName', 'year'])
    years = pd.unique(df['year'])
    tables = []
    for year in years: 
        tables.append(df[df['year'] == year].sort_values('payment', ascending=False)
                      .drop(columns=['FIP', 'year']).head(10))

    tabs = pn.Tabs()    
    for year, table in zip(years, tables): 
        tabs.append((str(year), table))
    return tabs

def county_map(year = 2020): 
    tdf = aggregate(all_df, level = ['FIP', 'year'])
    tdf = tdf[tdf['year'] == year]
    tempgdf = gdf 
    tempgdf = tempgdf.merge(tdf, on='FIP', how='left')
    return tempgdf.hvplot(geo=True, tools=['tap'], selection_line_color='red')


selection_map = county_map()
# fig_progyear = prog_year_pmt()
# fig_annual_summary = pn.bind(payment_trend, program_select)
# data_program_activity = pn.bind(df_filter_progcode, prog_code_enter)
# map_dict = {year: county_map(year) for year in range(2006,2022)}
# county_year_map = hv.HoloMap(map_dict, kdims="year")
def print_cnty_info(index):
    # print(index)
    if not index: 
        print('HELLO')
        return hv.Table(tempgdf)
    county = tempgdf[index[0]]
    print('goodbye!!')
    return hv.Table(county)

cnty_map = tempgdf.hvplot(geo=True, tools=['tap'], selection_line_color='red')

index_stream = hv.streams.Selection1D(source = cnty_map)


# selection_output = hv.DynamicMap(print_cnty_info, streams=[index_stream])
# progs_in_county = pn.bind(top_progs_in_county_table, county_dropdown)

### Build the Dash
header = pn.Row(pn.panel('# Data Exploration'))
# body = pn.Row(program_select, fig_annual_summary, fig_progyear) 
# dash = pn.Column(header, body, prog_code_enter, data_program_activity)
mapper = pn.Row(cnty_map)
# body = pn.Row(state_dropdown, county_dropdown, progs_in_county)
dash = pn.Column(header, mapper)
print(dash)
dash.show()


####################################################
### clickable map that outputs county info
####################################################
# def print_cnty_info(index):
#     if not index: 
#         return 
#     county = tempgdf[index[0]]
#     return county

# cnty_map = tempgdf.hvplot(geo=True, tools=['tap'], selection_line_color='red')
# index_stream = hv.streams.Selection1D(source = cnty_map)
# selection_output = hv.DynamicMap(print_cnty_info, streams=[index_stream])

# dash = pn.Column(cnty_map, selection_output)
# dash.show()