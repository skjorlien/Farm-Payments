import sys
import os 
sys.path.append(os.path.join(os.getcwd(), 'code'))
# os.register_at_fork(after_in_child=lambda: _get_font.cache_clear())

import time
from visualizations import top_counties_by_year_table, top_states_by_year_table
from datasets import load_data
import settings 
from tqdm import tqdm


if __name__ == "__main__":
    df = load_data()

    tex = top_counties_by_year_table(df)
    with open(os.path.join(settings.OUTDIR, 'tables', 'top_counties_table.tex'), 'w') as f:
        f.write(tex)

    tex = top_states_by_year_table(df)
    with open(os.path.join(settings.OUTDIR, 'tables', 'top_states_table.tex'), 'w') as f:
        f.write(tex)
