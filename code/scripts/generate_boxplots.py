import sys
import os 
sys.path.append(os.path.join(os.getcwd(), 'code'))
# os.register_at_fork(after_in_child=lambda: _get_font.cache_clear())

import time
from visualizations import program_boxplot, county_boxplot
from datasets import load_data
import settings
import matplotlib.pyplot as plt
import matplotlib 
matplotlib.use('agg')

from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor

def program_boxplot_wrapper(df, prog=None):
    fig, ax, fname = program_boxplot(df, prog)
    fig.savefig(os.path.join(settings.OUTDIR, 'boxplots', 'programs', fname))
    plt.close(fig)
    return f"Completed {fname}"

def county_boxplot_wrapper(df, fip=None):
    fig, ax, fname = county_boxplot(df, fip)
    plt.savefig(os.path.join(settings.OUTDIR, 'boxplots', 'counties', fname))
    plt.close()
    return f"Completed {fname}"


if __name__ == "__main__":
    start = time.time()
    df = load_data()
    progs = df['programName'].unique()
    fips = df['FIP'].unique()

    print('Starting the processing')
    # LAME WAY
    # for prog in progs: 
    #     result = program_boxplot_wrapper(df, prog)
    #     print(result)

    # for fip in fips:
    #     result = county_boxplot_wrapper(df, fip)
    #     print(result)

    # print(program_boxplot_wrapper(df))
    # print(county_boxplot_wrapper(df))

    # cool Way 
    with ThreadPoolExecutor() as executor: 
        results = [executor.submit(program_boxplot_wrapper, df, prog) for prog in progs]
        for f in as_completed(results): 
            print(f.result())

    finish = time.time()
    print(f"Finished in {round((finish - start)/60, 2)} mins")