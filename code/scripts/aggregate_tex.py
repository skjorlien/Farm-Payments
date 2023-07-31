import sys
import os 
sys.path.append(os.path.join(os.getcwd(), 'code'))

import settings 
from datasets import load_data
from tqdm import tqdm

if __name__ == '__main__':
    df = load_data()
    fips = df['FIP'].unique()
    i = 0
    output = []
    currstate = None
    for fip in tqdm(fips): 
        county = df[df['FIP'] == fip]['county'].iloc[0]        
        state = df[df['FIP'] == fip]['state'].iloc[0]
        if state != currstate:
            currstate = state
            output.append(f"\section*{{{state}}}")

        output.append(f"\subsection*{{{state} - {county} - boxplot}}")

        BOXPLOTPATH = "../output/boxplots/counties"
        boxplot_fname = f"{county}-{state}_boxplot.png"
        output.append(f"\\begin{{figure}}[h]")
        output.append(f"\centering")
        output.append(f"\includegraphics[width=7in]{{{BOXPLOTPATH}/{boxplot_fname}}}")
        output.append(f"\\end{{figure}}")
        output.append("\n")

        output.append(f"\subsection*{{{state} - {county} - top programs}}")
        TABLEPATH = "../output/tables/counties"
        table_fname = f"{county}-{state}_prog-table.tex"
        output.append(f"\input{{{TABLEPATH}/{table_fname}}}")
        output.append(f"\\newpage")


    with open(os.path.join(settings.OUTDIR, 'county_aggregated.tex'), 'w') as f:
        f.writelines([cmd + '\n' for cmd in output])