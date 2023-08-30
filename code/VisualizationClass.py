from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import settings
import dask.dataframe as dd
from datasets import load_data, aggregate
import numpy as np
import time
from utils import utils
import itertools
import geopandas as gpd
from mpl_toolkits.axes_grid1 import make_axes_locatable

''' Abstract Parent Class '''
class AbstractViz:
    def __init__(self, df: dd.DataFrame, path):
        self.df = self._process_data(df)
        self.path = os.path.join(settings.OUTDIR, path)
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def _clean_fname(self, fname):
        return "".join(i for i in fname if i not in "\/:*?<>|&")

    def _generate_argmap(self, **kwargs):
        '''return list of dictionaries to pass to vis function.'''
        return [kwargs]

    def _process_data(self, df):
        ''' gets the data in the smallest format usable by all configurations of the viz '''
        return df

    def test(self, **kwargs):
        if kwargs:
            argmap = [kwargs] 
        else:
            argmap = self._generate_argmap(**kwargs)
        fname = self.save(**argmap[0])
        print(f"args: {argmap[0]}, fname: {fname}, numargs: {len(argmap)}, path: {self.path}")

    def visualize(self, **kwargs):
        pass  

    def save(self, **kwargs):
        pass

    def generate_all(self, **kwargs):
        argmap = self._generate_argmap(**kwargs)
        print(argmap)
        start = time.time()
        try: 
            with ProcessPoolExecutor() as executor: 
                results = [executor.submit(self.save, **args) for args in argmap]
                for f in as_completed(results): 
                    print(f"{f.result()} -- Complete")
        except:
            print('Multiprocessing Failed.')
            for arg in argmap: 
                print(f"{self.save(**arg)} -- Complete")
        end = time.time()
        print(f'Completed {len(argmap)} in {np.round(end-start, 2) / 60} mins')


''' Handle Visualization Types ''' 
class Figure(AbstractViz):
    def __init__(self, df, path=None):
        path = os.path.join('figures', path) if path is not None else 'figures'
        super().__init__(df, path)

    def save(self, **kwargs):
        fig, ax, fname = self.visualize(**kwargs)
        plt.savefig(os.path.join(self.path, fname))
        plt.close()
        return fname


class Table(AbstractViz):
    def __init__(self, df, path=None):
        path = os.path.join('tables', path) if path is not None else 'tables'
        super().__init__(df, path)

    def save(self, **kwargs): 
        tex, fname = self.visualize(**kwargs)
        with open(os.path.join(self.path, fname), 'w') as f:
            f.write(tex)
        return fname


class Map(AbstractViz):
    def __init__(self, df, path=None):
        path = os.path.join('maps', path) if path is not None else 'maps'
        super().__init__(df, path)

    def save(self, **kwargs):
        fig, ax, fname = self.visualize(**kwargs)
        plt.savefig(os.path.join(self.path, fname), dpi=300)
        plt.close()
        return fname


''' Begin Declaring Visualizations'''
class PaymentTrend(Figure):
    def __init__(self, df):
        self.daterange = settings.DATERANGE
        super().__init__(df, 'payment_trends')

    def _process_data(self, df):
        aggdf = aggregate(df, groupby=['programCode', 'programName', 'year'])
        AnnualTot = aggregate(df, groupby=['year'])
        AnnualTot['programName'] = 'Annual Totals'
        return dd.concat([aggdf, AnnualTot])

    def _generate_argmap(self, **kwargs):
        programs = self.df['programName'].unique().compute()
        argmap = []
        for program in programs:
            args = kwargs
            args['program'] = program 
            argmap.append(args)
        argmap.append({})
        return argmap

    def visualize(self, **kwargs):
        try:
            program = kwargs['program']
        except: 
            print('Program argument missing, generating viz of all programs')
            program = 'Annual Totals'

        df = self.df[self.df['programName'] == program].compute()

        minyear = np.min(self.daterange)
        maxyear = np.max(self.daterange)

        fig, ax = plt.subplots()
        ax.bar(df['year'], df['payment'])
        ax.set_xlabel('Year')
        ax.set_ylabel('Payments')
        ax.set_xlim(minyear, maxyear)
        ax.set_ylim(0, np.max(df['payment']))
        ax.set_title(f'Annual Payment by Program - {program}')

        fname = self._clean_fname(f'plot_annualTrend_{program}.png')
        
        return fig, ax, fname


class ProgramBoxplot(Figure):
    def __init__(self, df):
        self.daterange = settings.DATERANGE
        super().__init__(df, 'program_boxplots')

    def _generate_argmap(self, **kwargs):
        programs = self.df['programName'].unique().compute()
        argmap = []
        for program in programs:
            args = kwargs 
            args['program'] = program 
            argmap.append(args)
        argmap.append({})
        return argmap

    def visualize(self, **kwargs):
        if 'log' in kwargs: 
            log = kwargs['log']
        else: 
            log = False

        # filter data
        try: 
            program = kwargs['program']
            data = self.df[self.df['programName'] == program]
        except: 
            print('Program argument missing, generating viz of all programs')
            program = 'All Programs'
            data = self.df


        # subset data
        if log: 
            data = data[['year', 'logpayment']].compute()
            data = data.rename(columns={'logpayment': 'payment'})
        else: 
            data = data[['year', 'payment']].compute()


        data = [data[data['year'] == year]['payment'] for year in self.daterange]

        fig, ax = plt.subplots(figsize=(12,8)) 
        ax.boxplot(data, vert=True, labels = self.daterange)
        ax.yaxis.grid(True)
        ylab = "Log Payment ($)" if log else "Payment ($)"
        ax.set(title = program,
            xlabel = 'Year',
            ylabel = ylab)
        fig.tight_layout()

        fname = self._clean_fname(f"{program}_boxplot.png")
        return fig, ax, fname


class CountyBoxplot(Figure):
    def __init__(self, df):
        self.daterange = settings.DATERANGE
        super().__init__(df, 'county_boxplots')

    def _generate_argmap(self, **kwargs):
        counties = self.df['FIP'].unique().compute()
        argmap = []
        for county in counties:
            args = kwargs 
            args['FIP'] = county 
            argmap.append(args)
        argmap.append({})
        return argmap

    def visualize(self, **kwargs):
        log = kwargs.get('log', True)

        # filter data
        try: 
            fip = kwargs['FIP']
            data = self.df[self.df['FIP'] == fip]
            state = data['state'].compute().iloc[0]
            county = data['county'].compute().iloc[0]
        except: 
            print('Program argument missing, generating viz of all programs')
            state = None
            county = "All Counties"
            data = self.df

        # subset data
        if log: 
            data = data[['year', 'logpayment']].compute()
            data = data.rename(columns={'logpayment': 'payment'})
        else: 
            data = data[['year', 'payment']].compute()

        data = [data[data['year'] == year]['payment'] for year in self.daterange]

        fig, ax = plt.subplots(figsize=(12,8)) 
        ax.boxplot(data, vert=True, labels = self.daterange)
        ax.yaxis.grid(True)
        title = f"{county}, {state}" if state is not None else f"{county}"
        ylab = "Log Payment ($)" if log else "Payment ($)"
        ax.set(title = title,
            xlabel = 'Year',
            ylabel = ylab)
        fig.tight_layout()

        fname = self._clean_fname(f"{county}-{state}_boxplot.png")
        return fig, ax, fname


class TopProgramTable(Table):
    def __init__(self, df, path=None):
        super().__init__(df, 'top_programs_by_county')

    def _generate_argmap(self, **kwargs):
        counties = self.df['FIP'].unique().compute()
        argmap = []
        for county in counties:
            args = kwargs 
            args['FIP'] = county 
            argmap.append(args)
        argmap.append({})
        return argmap

    def visualize(self, **kwargs):
        try: 
            fip = kwargs['FIP']
            data = self.df[self.df['FIP'] == fip]
            state = data['state'].compute().iloc[0]
            county = data['county'].compute().iloc[0]
            fname =  self._clean_fname(f"{county}-{state}_prog-table.tex")
        except Exception as error:
            print(f"error occurred: {error}") 
            print('FIP argument missing, generating viz of all counties')
            fname = "all-counties_prog-table.tex"
            data = self.df

        headnum = kwargs.get('headnum', 3)

        aggdf = aggregate(data, groupby=['year', 'programName']).compute()

        summarytable = aggdf.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
        # summarytable = summarytable.compute()
        summarytable.drop(columns='year', inplace=True)
        summarytable.index = summarytable.index.droplevel(level=1)
        summarytable.set_index('programName', append=True, inplace=True)
        return summarytable.to_latex(
            float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
        ), fname


class TopStateTable(Table):
    def __init__(self, df, path=None):
        super().__init__(df, path)

    def _process_data(self, df):
        aggdf = aggregate(df, groupby=['year', 'stateabbr'])
        return aggdf

    def visualize(self, **kwargs):
        headnum = kwargs.get('headnum', 5)
        
        summarytable = self.df.groupby(['year']).apply(lambda x: x.nlargest(headnum, 'payment'))
        summarytable = summarytable.compute()
        summarytable.drop(columns='year', inplace=True)
        summarytable.index = summarytable.index.droplevel(level=1)
        summarytable.set_index('stateabbr', append=True, inplace=True)
            
        fname = "top_states_table.tex"
        return summarytable.to_latex(
            float_format=lambda x: "\$ {:,.2f} M".format(x/1000000)
        ), fname


class TopCountyTable(Table):
    def __init__(self, df, path=None):
        super().__init__(df, path)

    def visualize(self, **kwargs):
        headnum = kwargs.get('headnum', 5)

        aggdf = aggregate(self.df, groupby = ['year', 'FIP'])
        cnty_state_crswlk = self.df.drop_duplicates(subset=['FIP'])[['FIP', 'county', 'state']]
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


class USCountyMap(Map):
    def __init__(self, df):
        self.daterange = settings.DATERANGE
        self.state_map, self.county_map = self._load_maps()
        super().__init__(df, 'county_choropleths')

    def _load_maps(self):
        county_map = gpd.read_file('data/basemaps/us_county.zip')
        county_map = county_map[['STATEFP', 'COUNTYFP', 'NAME', 'NAMELSAD', 'geometry']]
        county_map = county_map.cx[-126:-66, 22:50]
        county_map['FIP'] = county_map['STATEFP'] + county_map['COUNTYFP']

        state_map = gpd.read_file('data/basemaps/us_state.zip')
        state_map = state_map.cx[-126:-66, 22:50]
        return state_map, county_map

    def _process_data(self, df):
        df = aggregate(df, groupby=['FIP', 'year'], customer=True)
        df = df[df['FIP'] != '11001']
        df['concentration'] = df['payment'] / df['customer']
        return df

    def _generate_argmap(self, **kwargs):
        variables = ['payment', 'concentration', 'customer']
        args = itertools.product(self.daterange, variables)
        argmap = [{'year': arg[0], 'variable': arg[1]} for arg in args]
        return argmap

    def visualize(self, **kwargs):
        year = kwargs.get('year', 2020)
        variable = kwargs.get('variable', 'payment')
        config = kwargs.get('config', {})

        df = self.df[self.df['year'] == year]
        df = df[['FIP', variable]] 
        df = df.compute()

        titles = {
            'payment': "Farm Payments ($)", 
            'customer': "# of Payment Recipients",
            'concentration': "$ per Recipient"
        }

        config = {
            'legend': True,
            'vmin': np.min(df[variable]),
            'vmax': np.max(df[variable]),
            'legend_kwds': {
                'format': utils.get_float_formatter(df[variable].mean())
            },
            'missing_kwds': {
                'color': 'lightgrey',
                'label': 'No payments'
            }
        }

        if 'config' in kwargs:
            config = utils.update_settings(config, kwargs['config'])

        # merge a data series 
        county_map = self.county_map.merge(df, on='FIP', how='left')
        fname = self._clean_fname(f"map_cnty_{variable}_{year}.png")

        # Plot it
        fig, ax = plt.subplots()
        divider = make_axes_locatable(ax)
        cax = divider.append_axes('right', size='5%', pad=0.05)
        county_map.plot(ax=ax, 
                        cax=cax,
                        column=variable,
                        **config)
        self.state_map.boundary.plot(ax=ax, color = 'black', linewidth=1)
        ax.set_title(f"County-Level {titles[variable]} - {year}")
        ax.set_axis_off()

        return fig, ax, fname


class SummaryStats(Table):
    def __init__(self, df, path=None):
        super().__init__(df, path)

    def _process_data(self, df):
        df = df[['year', 'payment']]
        df = df.groupby(['year']).agg(['mean', 'max', 'sum', 'std', 'count'])
        return df

    def visualize(self, **kwargs):
        if 'source' in kwargs:
            source = kwargs['source']
        else:
            raise KeyError('you must provide a source name in kwargs.')

        df = self.df.compute()
        df.sort_index(inplace=True)
        fname = f'{source}_summary_table.tex'    
        return df.to_latex(
            float_format=lambda x: "\$ {:,.2f}".format(x),
            caption = f"Summary Stats from {source} data",
            position = 'H'
        ), fname


class PaymentDistribution(Figure):
    def __init__(self, source, path=None):
        df = load_data(source = source)
        self.daterange = settings.DATERANGE
        self.source = source
        super().__init__(df, 'payment_distributions')

    def _generate_argmap(self, **kwargs):
        argmap = []
        for y in self.daterange:
            args = {}
            args['source'] = self.source
            args['year'] = y 
            argmap.append(args)
        return argmap

    def _process_data(self, df):
        df['month'] = df['paymentDate'].dt.month
        return df

    def visualize(self, **kwargs):
        year = kwargs['year']
        source = kwargs['source']
        df = self.df[self.df['year'] == year].compute()
        # df['month'] = df['paymentDate'].dt.month 
        aggdf = df.groupby('month').count()
        fig, ax = plt.subplots()
        ax.bar(aggdf.index, aggdf['FIP'])
        ax.set_title(f"{source} - {year}")
        fname = f"{source}_data_dist_{year}"
        return fig, ax, fname


class ProgramYearPMTDiff(Table): 
    def __init__(self, path=None):
        foia_data = load_data(source="FOIA", groupby=['programCode', 'year'])
        public_data = load_data(source="Public", groupby=['programCode', 'year'])
        df = public_data.merge(foia_data, how='outer', on=['programCode', 'year'])
        super().__init__(df, path)

    def visualize(self, **kwargs):
        df = self.df.compute()
        df.fillna(0, inplace = True)
        df['diff'] = df['payment_x'] - df['payment_y']
        df.drop(columns=['payment_x', 'payment_y'], inplace=True)
        df['programCode'] = df['programCode'].str.replace('&', 'and')
        df = df.pivot(columns='year', index='programCode', values='diff')
        df.fillna(0, inplace=True)
        fname = "program_year_mean_diff.tex"
        return df.to_latex(
            float_format=lambda x: "\$ {:,.0f}".format(x/1000000),
            longtable = True,
            label = "progYearDiffTable",
            caption = "Public minus FOIA in Millions of Dollars by Program Code"
        ), fname
        

class StateYearPMTDiff(Table): 
    def __init__(self, path=None):
        foia_data = load_data(source="FOIA", groupby=['stateabbr', 'year'])
        public_data = load_data(source="Public", groupby=['stateabbr', 'year'])
        df = public_data.merge(foia_data, how='outer', on=['stateabbr', 'year'])
        super().__init__(df, path)

    def visualize(self, **kwargs):
        df = self.df.compute()
        df.fillna(0, inplace = True)
        df['diff'] = df['payment_x'] - df['payment_y']
        df.drop(columns=['payment_x', 'payment_y'], inplace=True)
        df = df.pivot(columns='year', index='stateabbr', values='diff')
        df.fillna(0, inplace=True)
        fname = "state_year_mean_diff.tex"
        return df.to_latex(
            float_format=lambda x: "{:,.0f}".format(x/1000000),
            longtable = True,
            label = "stateYearDiffTable",
            caption = "Public minus FOIA in Millions of Dollars by State"
        ), fname


class ProgramReference(Table):
    def __init__(self, path=None):
        df = load_data()
        super().__init__(df, path)

    def visualize(self, **kwargs):
        df = self.df[['programCode', 'programName', 'FIP']]
        df['programName'] = df['programName'].str.replace('&', 'and')
        df = df.groupby(['programCode', 'programName']).count()
        df = df.compute()
        df.drop(columns = df.columns, inplace=True)
        df.sort_index(level=[0, 1], inplace=True)
        fname = "program_reference.tex"
        return df.to_latex(
            longtable=True, 
            label='progCodeLookup',
            caption='Program Code / Name Lookup Table'
        ), fname


if __name__ == '__main__':
    obj = USCountyMap(load_data())
    args = {'year': 2019, 'variable': 'payment'}
    obj.test(**args)
