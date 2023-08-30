# THIS CODEBASE IS UNDER DEVELOPMENT

# To Do:
- More Cartograms: # of producers, agricultural product by state. 

# File Structure: 
```
├── Rcode
│   ├──  *.R
├── code
│   ├──   scripts
│   ├──   utils
│   ├──   *.py
├── output
│   ├──   figures
│   ├──   maps
│   ├──   tables
├── writing
├── data (not included in Github Repo. See 'Data' section Below)
│   ├── basemaps
│   ├── clean
│   └── raw
│   │   ├── **/*.css
```

# Data
Data is available publicly on https://www.fsa.usda.gov/news-room/efoia/electronic-reading-room/frequently-requested-information/payment-files-information/index 

## Download Payments Data
`code/scripts/download_payments_data.py`: downloads individual excel files, stores locally in `data/raw/Public`. Note: some code is dedicated to analyzing the difference between publicly available data and data recieved via FOIA. For a report on why we choose to proceed with the publicly available data, see `writing/compare_data_sources/report.pdf`. 

Researcher must convert each excel file to CSV using method of choice and ensure uniform column names before using cleaning script.

## Clean Data
Run `code/scripts/clean.py` to clean the public raw data (in .csv format), and store a parquet output to `data/clean/parquet-Public`.

TODO: There are extraneous functions in `clean.py` that are used to coerce headers so that the csv files play nice with Dask and Parquet. Incorporate into the cleaning script. 

# Using the Data
see `code/datasets` for function declarations 
- `load_data` reads cleaned parquet, with option to pass aggregation groups. Defaults to loading data from `data/clean/parquet-Public` 
- `aggregate` takes a list of columns to groupby and returns aggregated payment sum and # of unique payees 

# Manual

## Data Aggregation
- download excel files from source 
- convert to csv
- conform headers (using helper functions in scripts/clean.py)

## Data Cleaning 
- utilizing dask, read all csvs as one dataframe, clean, format and save parquet (see scripts/clean.py)

