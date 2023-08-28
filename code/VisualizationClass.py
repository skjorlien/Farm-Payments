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

''' Abstract Parent Class '''
class AbstractViz:
    def __init__(self, df: dd.DataFrame, path):
        self.df = self._process_data(df)
        self.path = os.path.join(settings.OUTDIR, path)

    def _clean_fname(self, fname):
        return "".join(i for i in fname if i not in "\/:*?<>|&")

    def _generate_argmap(self, **kwargs):
        '''return list of dictionaries to pass to vis function.'''
        return [kwargs]

    def _process_data(self, df):
        ''' gets the data in the smallest format usable by all configurations of the viz '''
        return df

    def test(self, **kwargs):
        argmap = self._generate_argmap(**kwargs)
        fname = self.save(**argmap[0])
        print(f"args: {argmap[0]}, fname: {fname}, numargs: {len(argmap)}, path: {self.path}")

    def visualize(self, **kwargs):
        pass  

    def save(self, **kwargs):
        pass

    def generate_all(self, **kwargs):
        argmap = self._generate_argmap(**kwargs)
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
        plt.savefig(os.path.join(self.path, fname))
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


class SummaryStats(Table):
    def __init__(self, df, path=None):
        super().__init__(df, path)

    def _generate_argmap(self):
        argmap = {'headnum': 5}
        return argmap

    def visualize(self, **kwargs):
        if 'headnum' in kwargs:
            headnum = kwargs['headnum']
        else:
            headnum = 5

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
        county_map.plot(ax=ax, 
                        column=variable,
                        **config)
        self.state_map.boundary.plot(ax=ax, color = 'black', linewidth=1)
        ax.set_title(f"{variable} {year}")
        ax.set_axis_off()

        return fig, ax, fname


if __name__ == '__main__':
    # obj = TopStateTable(load_data())
    # obj.test()
    # 46.5
    obj = USCountyMap(load_data())
    obj.generate_all()
    # obj = TopStateTable(load_data())
    # obj.test()
    