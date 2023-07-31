# virtual environment
`conda activate ethan-gsr`
`conda install pandas matplotlib geopandas`

# To Do:
- Compare online vs FOIA... 
- Streamline visualizations (incl. log boxplot) 
- Streamline scripts. (debug matplotlib multiprocessing)
- Share Github With Ethan
- Look into other papers using this data (Environmental Workign Group, Joe Glauber, Barry Goodwin) 

### Document: 
- Cleaning Process (and how to incorporate new data) including decisions about data storage and multiprocessing.
- Loading / Aggregation procedures 
- Scripts
- Lessons learned from "top 5" tables (Including an expose on heaviest hitting programs)
- Lessons learned from cartograms 
- Lessons learned from maps/gifs


 



- I need to create a data cleaning script that fixes the data in the pickle and saves as that same pickle REFER to Admin_payments/combine_admin_payments_files.org for cleaning inspo
- Generate a pickle of FOIA data AND free online data. Create some comparison tables.

# Overview of Scripts
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