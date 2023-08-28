import os 

OUTDIR = os.path.join(os.getcwd(), 'output')
RAWDATA = os.path.join(os.getcwd(), 'data', 'raw', 'all-data.pkl') 
CLEANDATA = os.path.join(os.getcwd(), 'data', 'clean', 'all-data.pkl') 
DATERANGE = range(2006, 2023)

PARQUET_FOIA = os.path.join(os.getcwd(), 'data', 'clean', 'parquet-FOIA') 
PARQUET_Public = os.path.join(os.getcwd(), 'data', 'clean', 'parquet-Public') 
# When working from USB, Comment out above, and use below
# PARQUET_FOIA = os.path.join('E:', 'data', 'Ethan-GSR','clean', 'parquet-FOIA') 
# PARQUET_Public = os.path.join('E:', 'data', 'Ethan-GSR','clean', 'parquet-Public') 
