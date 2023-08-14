# THIS DOCUMENT IS UNDER DEVELOPMENT

# virtual environment
`conda activate ethan-gsr`
`conda install pandas matplotlib geopandas`



# To Do:
- Compare online vs FOIA -- generate side-by-side boxplot figure
- Streamline visualizations (incl. log boxplot) 
- Streamline scripts. (debug matplotlib multiprocessing)
- Look into other papers using this data (see: Environmental Workign Group, Joe Glauber, Barry Goodwin) 
- California County-Level Cartogram 
- More Cartograms: # of producers, agricultural product by state. 


### Investigate: 
- With the public data, DC is the top recipient 2010-2021. What program does that represent? Was that program included in the FOIA?
- Is 2022 complete? check chart of payment dates.

### Document: 
- Cleaning Process (and how to incorporate new data) including decisions about data storage and multiprocessing.
- Loading / Aggregation procedures 
- Scripts
- Lessons learned from "top 5" tables (Including an expose on heaviest hitting programs)
- Lessons learned from cartograms 
- Lessons learned from maps/gifs


# Overview of Scripts
- `xlsx_to_csv.py`: each xslx file takes ~2 min to complete
- `aggregate_csv.py`: take all csvs, aggregate them, pickle em. 
- 

# Manual

## Data Aggregation
- download excel files from source
- convert to csv
- conform headers (using helper functions in scripts/clean.py)

## Data Cleaning 
- utilizing dask, read all csvs as one dataframe, clean, format and save parquet (see scripts/clean.py)

## Data Loading/Aggregating 
see `datasets` for function declarations 
- `load_data` reads parquet, with option to pass aggregation groups, and option to source from FOIA or Public files 
- `aggregate` takes a list of columns to groupby and returns aggregated payment sum and # of unique payees 