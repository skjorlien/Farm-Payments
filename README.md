# virtual environment
`conda activate ethan-gsr`
`conda install pandas matplotlib geopandas`

# To Do:
- I need to create a data cleaning script that fixes the data in the pickle and saves as that same pickle REFER to Admin_payments/combine_admin_payments_files.org for cleaning inspo
- Generate a pickle of FOIA data AND free online data. Create some comparison tables.

# Scripts
- `xlsx_to_csv.py`: each xslx file takes ~2 min to complete
- `aggregate_csv.py`: take all csvs, aggregate them, pickle em. 
- 

# Manual

## Data Aggregation
- I use a local package `kjtools` to convert each excel to csv and then aggregate the csvs into one pickle. This generates data/raw/all-data.pkl 

## Data Cleaning 
- scripts/clean.py takes raw/all-data.pkl, runs each of the cleaner functions on the data and outputs clean/all-data.pkl.

## Data Loading/Aggregating 
see `datasets` for function declarations 
- `load_data` opens the cleaned pickled file, with option to pass aggregation groups 
- `aggregate` takes a list of columns to groupby and returns aggregated payment sum and # of unique payees 