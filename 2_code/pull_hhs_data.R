


library(jsonlite)
library(tidyverse)
library(janitor)
library(anytime)
library(pracma)
library(lubridate)

start <- Sys.time()

###############################################################
## Pull testing data from HHS ---------------------------------
###############################################################

#https://healthdata.gov/dataset/covid-19-diagnostic-laboratory-testing-pcr-testing-time-series

# new HHS beta
url <- "https://beta.healthdata.gov/api/views/j8mb-icvb/rows.csv?accessType=DOWNLOAD"

tests_raw <- read_csv(url)

# clean
tests <- tests_raw %>%
  mutate(date = anydate(date)) %>%
  rename(type = overall_outcome,
         new_tests = new_results_reported,
         total_tests = total_results_reported) %>%
  select(state, type, date, new_tests, total_tests)

# reshape
tests_wide <- tests %>%
  pivot_wider(id_cols = c("state", "date"),
              names_from = type,
              values_from = c("new_tests", "total_tests")) %>%
  clean_names()

# 7d avg
test_df <- tests_wide %>%
  mutate(new_tests = new_tests_negative + new_tests_positive) %>%
  group_by(state) %>%
  arrange(state, date) %>%
  mutate(new_tests_7d_avg = movavg(new_tests, n = 7),
         new_pos_tests_7d_avg = movavg(new_tests_positive, n = 7),
         pct_pos = new_tests_positive / new_tests,
         last_report_date = max(date, na.rm = TRUE)) %>%   
  ungroup()%>%
  mutate(test_date = max(date, na.rm = TRUE))

###############################################################
## Pull case and death data from CDC ----------------------------
###############################################################

# API
cd <- read_csv("https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD")

# get one value for each state and date
nrow(cd %>% distinct(state, submission_date)) / nrow(cd) # good

case_death_df <- cd %>%
  select(tot_cases,
         tot_death,
         new_case,
         new_death,
         state,
         date = submission_date) %>%
  group_by(state) %>%
  mutate(date = mdy(date)) %>%
  arrange(state, date) %>%
  mutate(new_case_7d_avg = movavg(new_case, n = 7),
         new_death_7d_avg = movavg(new_death, n = 7)) %>%
  ungroup()


# filter dates that are incomplete
case_death_df <- case_death_df %>%
  group_by(date) %>%
  mutate(n_states = row_number(),
         n_states = max(n_states, na.rm = TRUE)) %>%
  ungroup() %>%
  filter(n_states >= 5) %>%
  mutate(case_death_date = max(date, na.rm = TRUE))


###############################################################
## Hospitalizations -----------------------------
###############################################################

## timeseries 

## new HHS beta
url <- "https://beta.healthdata.gov/api/views/g62h-syeh/rows.csv?accessType=DOWNLOAD"

# read data
hosp <- read_csv(url) %>%
  clean_names()

# clean
hosp_df <- hosp %>%
  select(state,
         date,
         currently_hospitalized = inpatient_beds_used_covid,
         previous_day_admission_adult_covid_confirmed,
         previous_day_admission_adult_covid_suspected,
         previous_day_admission_pediatric_covid_confirmed,
         previous_day_admission_pediatric_covid_suspected,
         staffed_icu_adult_patients_confirmed_and_suspected_covid,
         previous_day_admission_adult_covid_confirmed,
         previous_day_admission_adult_covid_suspected,
         previous_day_admission_pediatric_covid_suspected,
         previous_day_admission_pediatric_covid_confirmed) %>%
  mutate(date = ymd(date),
         new_admit = previous_day_admission_adult_covid_confirmed + previous_day_admission_adult_covid_suspected +
           previous_day_admission_pediatric_covid_suspected + previous_day_admission_pediatric_covid_confirmed) %>%   
  ungroup()

# max date
max_date <- max(hosp_df$date, na.rm = TRUE)

## daily

# new HHS beta (daily)
url <- "https://beta.healthdata.gov/api/views/6xf2-c3ie/rows.csv?accessType=DOWNLOAD"

hosp_daily_raw <- read_csv(url) %>%
  clean_names()

hosp_daily <- hosp_daily_raw %>%
  select(state, currently_hospitalized = inpatient_beds_used_covid, reporting_cutoff_start,
         previous_day_admission_adult_covid_confirmed,
         previous_day_admission_adult_covid_suspected,
         previous_day_admission_pediatric_covid_confirmed,
         previous_day_admission_pediatric_covid_suspected,
         staffed_icu_adult_patients_confirmed_and_suspected_covid,
         previous_day_admission_adult_covid_confirmed,
         previous_day_admission_adult_covid_suspected,
         previous_day_admission_pediatric_covid_suspected,
         previous_day_admission_pediatric_covid_confirmed) %>%
  mutate(date = ymd(reporting_cutoff_start) + 4)

daily_date <- max(hosp_daily$date, na.rm = TRUE)
        
write_csv(hosp_daily, paste0("hosp_daily_backup/hosp_", daily_date, ".csv"))

# read in daily data
files <- list.files("hosp_daily_backup")
dailies <- tibble()

for(f in files) {
  df <- read_csv(paste0("hosp_daily_backup/", f)) %>%
    mutate(date = ymd(date),
           currently_hospitalized = as.numeric(currently_hospitalized))
  dailies <- bind_rows(dailies, df) %>%
    filter(date > max_date) %>%
    mutate(provisional = "Yes")
}

## append
hosp_df <- bind_rows(hosp_df, dailies) %>%
  distinct(state, date, .keep_all = TRUE) %>%
  
  # daily change
  group_by(state) %>%
  arrange(state, date) %>%
  mutate(hosp_change = currently_hospitalized - lag(currently_hospitalized)) %>%
  ungroup()
  
###############################################################
## merge  -----------------------------
###############################################################

# metrics
merge1 <- left_join(case_death_df, test_df, by = c("state", "date"))
df <- left_join(merge1, hosp_df, by = c("state", "date")) %>%
  mutate(hosp_date = max(date, na.rm = TRUE)) # hosp will always be same date as latest case/death

# regions and state names
regions <- read_csv("https://raw.githubusercontent.com/cphalpert/census-regions/master/us%20census%20bureau%20regions%20and%20divisions.csv") %>%
  select(state_name = State,
         state = `State Code`,
         region = `Region`)

# population
pop_raw <- read_csv("https://raw.githubusercontent.com/COVID19Tracking/associated-data/master/us_census_data/us_census_2018_population_estimates_states.csv")

pop <- pop_raw %>%
  select(state, population) 

df <- left_join(df, regions, by = "state")
df <- left_join(df, pop, by = "state")

# max date indicator
df <- df %>%
  mutate(max_date_ind = ifelse(date == max(date, na.rm = TRUE), "Yes", "No"))

# get latest value of hosp, cases, deaths regardless of date
df <- df %>%
  group_by(state) %>%
  mutate(last_hosp = ifelse(is.na(currently_hospitalized), last(currently_hospitalized[!is.na(currently_hospitalized)]), currently_hospitalized)) %>%
  mutate(last_case_7d = ifelse(is.na(new_case_7d_avg), last(new_case_7d_avg[!is.na(new_case_7d_avg)]), new_case_7d_avg)) %>%
  mutate(last_death_7d = ifelse(is.na(new_death_7d_avg), last(new_death_7d_avg[!is.na(new_death_7d_avg)]), new_death_7d_avg)) %>%
  ungroup()

# fill in testing values as zero if they are inside the testing window
df <- df %>%
  mutate(test_date = max(test_date, na.rm = TRUE),
         new_tests = ifelse(is.na(new_tests) & date <= test_date, 0, new_tests),
         new_tests_positive = ifelse(is.na(new_tests_positive) & date <= test_date, 0, new_tests_positive)) %>%
  
  # calculate new 7-day averages for tests
  group_by(state) %>%
  arrange(state, date) %>%
  mutate(new_tests_7d_avg = movavg(new_tests, n = 7),
         new_pos_tests_7d_avg = movavg(new_tests_positive, n = 7),
         pct_pos = ifelse(new_tests == 0, 0, new_tests_positive / new_tests)) %>%
  ungroup()

## hex map
hex <- read_csv("hex.csv")

df <- left_join(df, hex, by = "state")

# write to disk
write_csv(df, "federal_state.csv")

# did it update?
last_cdc <- df %>% filter(!is.na(new_case))
last_test <- df %>% filter(!is.na(new_tests))
last_hosp <- dailies %>% filter(!is.na(currently_hospitalized))

# timing
Sys.time() - start

# check hosp values
dailies %>%
  filter(date >= max(date, na.rm = TRUE) - 5) %>%
  group_by(date) %>%
  summarize(currently_hospitalized = sum(currently_hospitalized, na.rm = TRUE))

# updates
print(paste0("Last HHS Hosp Date: ", max(last_hosp$date, na.rm = TRUE)))
print(paste0("Last CDC Date: ", max(last_cdc$date, na.rm = TRUE)))
print(paste0("Last Testing Date: ", max(last_test$date, na.rm = TRUE)))

