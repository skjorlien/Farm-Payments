import sys
import os 
sys.path.append(os.path.join(os.getcwd(), 'code'))
# os.register_at_fork(after_in_child=lambda: _get_font.cache_clear())

import time
from visualizations import top_programs_by_year_table
from datasets import load_data
import settings 
from tqdm import tqdm

if __name__ == '__main__':
    df = load_data()
    fips = df['FIP'].unique()

    tex = top_programs_by_year_table(df)
    with open(os.path.join(settings.OUTDIR, 'tables', 'all-counties_prog-table.tex'), 'w') as f:
        f.write(tex)


    for fip in tqdm(fips):
        tmpdf = df[df['FIP'] == fip]
        county = tmpdf['county'].iloc[0]
        state = tmpdf['state'].iloc[0]
        fname =  f"{county}-{state}_prog-table.tex"
        tex = top_programs_by_year_table(tmpdf)
        with open(os.path.join(settings.OUTDIR, 'tables', 'counties', fname), 'w') as f:
            f.write(tex)

    