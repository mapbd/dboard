
## setup -------------------------------
import numpy as np
import pandas as pd
import os
import janitor as jn
import datetime as dt

# mac
# os.chdir("/Users/conorkelly/Documents/COVID Dashboard")

# windows
os.chdir("C:/Users/ckelly/Documents/Covid-Personal - Copy")

######################################################
# FUNCTIONS
######################################################

# 7-day averages
def get_rolling_avg(df, new_var, using_var, group_var = 'state'):
  x = df.copy()
  x = (x
    .assign(new_var = lambda x: x.groupby(group_var).rolling(7,7)[using_var].mean().reset_index(drop=True))
    .rename(columns = {'new_var' : new_var}))
  return(x)
  
# make sure fips codes have zero in front if need be
def fix_fips(df, var):
  x = df.copy()
  v = var
  x = (x
    .assign(fips = lambda x: x[v].fillna(0)) # replace NA with 0
    .assign(fips = lambda x: x[v].astype(int)) # get rid of float
    .assign(fips = lambda x: x[v].astype(str)) # convert to string
    .assign(fips = lambda x: x[v].str.pad(width=5, side='left', fillchar='0')))
  return(x)

######################################################
# COUNTIES
######################################################

# NYT counties
nyt_covid = pd.read_csv("https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv")

# regions
regions = (pd.read_csv("https://raw.githubusercontent.com/cphalpert/census-regions/master/us%20census%20bureau%20regions%20and%20divisions.csv")
  .clean_names()
  .rename(columns = {'state' : 'stname'})
)
  
# MSAs
msa = (pd.read_csv("cbsatocountycrosswalk.csv", dtype = str)
  .clean_names()
  .filter(['fipscounty', 'msaname', 'cbsaname'])
  .rename(columns = {'fipscounty' : 'fips'})
  .assign(fips = lambda x: pd.to_numeric(x['fips']))
)

# pop density
density = (pd.read_csv("https://raw.githubusercontent.com/juliachristensen/ps239T-final-project/master/Data_Geo_Sophistication/County_Density_Census/DEC_10_SF1_GCTPH1.US05PR.csv",
                       encoding='ISO-8859-1', skiprows = 1)
  .clean_names()
  .assign(fips = lambda x: x['target_geo_id2'],
          pop_density = lambda x: x['population'] / x['area_in_square_miles_total_area'])
  .filter(['fips', 'pop_density'])
)

# elections
cty_election_raw = pd.read_csv("https://raw.githubusercontent.com/tonmcg/US_County_Level_Election_Results_08-16/master/2016_US_County_Level_Presidential_Results.csv")
cty_election = cty_election_raw
cty_election = (cty_election
  .filter(['combined_fips', 'votes_dem', 'votes_gop'])
  .rename(columns = {'combined_fips' : 'fips'})
)

        
# clean county population
county_pop = (pd.read_csv("co-est2019-alldata.csv", encoding='ISO-8859-1', dtype = str)
  .clean_names()
  .filter(['sumlev', 'region', 'division', 'state', 'county', 'stname', 'ctyname', 'popestimate2019'])
  .assign(fips = lambda x: pd.to_numeric(x['state'] + x['county']))
)

county_pop.tail()

## merge NYT data to other datasets

# population
covid_data = nyt_covid.merge(county_pop, how = 'left', on = 'fips')

# msa
covid_data = covid_data.merge(msa, how = 'left', on = 'fips')

# regions
covid_data = covid_data.merge(regions, how = 'left', on = 'stname')

# density
covid_data = covid_data.merge(density, how = 'left', on = 'fips')

# deal with unmerged obs
covid_data = (covid_data
  .assign(ctyname = lambda x: np.where(pd.isna(x['ctyname']), x['county_y'], x['ctyname']),
          stname = lambda x: np.where(pd.isna(x['stname']), x['state_y'], x['stname']),
          msaname = lambda x: np.where(x['ctyname'] == "New York City", "NEW YORK-NEWARK, NY-NJ-PA", x['msaname']))
)

# set population variable
covid_data['pop'] = pd.to_numeric(covid_data['popestimate2019'])

# cases per pop and var select
covid_data = (covid_data
  .assign(caseper1k = lambda x: x['cases'] / x['pop'] * 1000,
          deathper1k = lambda x: x['deaths'] / x['pop'] * 1000)
  .filter(['fips', 'stname', 'ctyname','pop', 'cases', 'deaths', 'caseper1k', 
           'deathper1k', 'date', 'msaname', 'cbsaname', 'pop_density'])
)

## make sure fips codes have zero in front if need be
covid_data = fix_fips(covid_data, 'fips')

## calculate new cases and new deaths
covid_data = (covid_data
  .assign(cases_prior = lambda x: x.groupby('fips').shift(1)['cases'],
          new_cases = lambda x: np.where(pd.isna(x['cases_prior']), x['cases'], x['cases'] - x['cases_prior']),
          
          deaths_prior = lambda x: x.groupby('fips').shift(1)['deaths'],
          new_deaths = lambda x: np.where(pd.isna(x['deaths_prior']), x['deaths'], x['deaths'] - x['deaths_prior']))
          
  # 7-day averages
   .pipe(get_rolling_avg, 'pos_7d_avg', 'new_cases', 'fips')
   .pipe(get_rolling_avg, 'death_7d_avg', 'new_deaths', 'fips')
)

## elections
cty_election_raw = pd.read_csv("https://raw.githubusercontent.com/tonmcg/US_County_Level_Election_Results_08-16/master/2016_US_County_Level_Presidential_Results.csv")

cty_election = (cty_election_raw
  .filter(['combined_fips', 'votes_dem', 'votes_gop'])
  .rename(columns = {'combined_fips' : 'fips'})
  .assign(result2016 = lambda x: np.where(x['votes_dem'] > x['votes_gop'], "Blue", "Red"))
  .pipe(fix_fips, 'fips')
)

## check uniqueness
len(covid_data.drop_duplicates(['fips', 'date'])) / len(covid_data)

######################################################
# STATES
######################################################
                          
## COVID Tracking Project ----------------------------

# load CTP data
ctp_raw = pd.read_json("https://api.covidtracking.com/v1/states/daily.json")
ctp = ctp_raw

# check uniqueness of state and date
len(ctp.drop_duplicates(['state','date'])) / len(ctp)

# clean CTP data
ctp = (ctp
  .filter(['date', 'state', 'fips', 'negative', 'hospitalizedIncrease',
           'hospitalizedCurrently', 'totalTestResultsIncrease', 'positiveIncrease', 
           'negativeIncrease', 'deathIncrease', 'totalTestResults', 'death', 
           'inIcuCumulative', 'inIcuCurrently', 'onVentilatorCurrently', 
           'onVentilatorCumulative', 'positiveCasesViral', 'totalTestEncountersViral'])
           
   # deal with dates
  .assign(date = pd.to_datetime(ctp['date'].astype(str), format = '%Y%m%d'))
       
   # percent positive
  .assign(pct_positive = lambda x: x['positiveIncrease'] / x['totalTestResultsIncrease'])
       
   # sort by state and date for 7-day moving averages
  .sort_values(by = ['state', 'date'], ignore_index=True)
  
  # 7-day averages
  .pipe(get_rolling_avg, new_var = 'pos_7d_avg', using_var = 'positiveIncrease')
  .pipe(get_rolling_avg, 'test_7d_avg', 'totalTestResultsIncrease')
  .pipe(get_rolling_avg, 'death_7d_avg', 'deathIncrease')
        
  # 7-day totals
  .assign(totaltests_7d = lambda x: x.groupby('state').rolling(7,7)['totalTestResultsIncrease'].sum()
            .reset_index(drop=True),

          totalpos_7d = lambda x: x.groupby('state').rolling(7,7)['positiveIncrease'].sum()
            .reset_index(drop=True),

   # 7-day percent positive   
          pct_pos_7d = lambda x: x['totalpos_7d'] / x['totaltests_7d'],

      # lagged values for deaths, cases, tests, and hospitalizations (one-week lag)
      
          lstwk_death_7d_avg = lambda x: x.groupby('state').shift(7)['death_7d_avg'],
            
          lstwk_pos_7d_avg = lambda x: x.groupby('state').shift(7)['pos_7d_avg'],
       
          lstwk_test_7d_avg = lambda x: x.groupby('state').shift(7)['test_7d_avg'],
                 
          lstwk_hosp = lambda x: x.groupby('state').shift(7)['hospitalizedCurrently'],
                 
          change_hosp = lambda x: x.groupby('state')['hospitalizedCurrently'].diff(),

      # change in deaths, cases, and tests by week
      
          deaths_incr_week = lambda x: (x['death_7d_avg'] - x['lstwk_death_7d_avg']) / x['lstwk_death_7d_avg'],
           
          pos_incr_week = lambda x: (x['pos_7d_avg'] - x['lstwk_pos_7d_avg']) / x['lstwk_pos_7d_avg'],
           
          tests_incr_week = lambda x: (x['test_7d_avg'] - x['lstwk_test_7d_avg']) / x['lstwk_test_7d_avg'])
)

## has your state peaked?
covid_data_states = ctp

# deaths
covid_data_states = (covid_data_states
  .assign(date_temp = lambda x: x['date'].astype(np.int64),
          max_deaths = lambda x: x.groupby('state')['death_7d_avg'].transform('max'),
          max_death_date = lambda x: np.where(x['death_7d_avg'] == x['max_deaths'], x['date_temp'], 0))
          
  .assign(max_death_date = lambda x: x.groupby('state')['max_death_date'].transform('max'))
  .assign(max_death_date = lambda x:pd.to_datetime(x['max_death_date'], unit = 'ns'))
  
  .assign(perc_decline_fr_death_peak = lambda x: (x['death_7d_avg'] - x['max_deaths']) / x['max_deaths'],
          days_since_peak_death = lambda x: (x['date'] - x['max_death_date']) / pd.Timedelta(days=1))
)
          
# cases
covid_data_states = (covid_data_states 
  .assign(max_pos = lambda x: x.groupby('state')['pos_7d_avg'].transform('max'),
          max_pos_date = lambda x: np.where(x['pos_7d_avg'] == x['max_pos'], x['date_temp'], 0))
          
  .assign(max_pos_date = lambda x: x.groupby('state')['max_pos_date'].transform('max'))
  .assign(max_pos_date = lambda x:pd.to_datetime(x['max_pos_date'], unit = 'ns'))
  
  .assign(perc_decline_fr_pos_peak = lambda x: (x['pos_7d_avg'] - x['max_pos']) / x['max_pos'],
          days_since_peak_pos = lambda x: (x['date'] - x['max_pos_date']) / pd.Timedelta(days=1))
)

              

