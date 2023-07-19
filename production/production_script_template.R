# Setup ----

# Libraries for ease
library(lubridate) # Easier manipulation of dates

# Fill in with years that are being updated for NI12 and NI13
years_ni12_ni13 <- tibble::tribble(
  ~year, ~year_start, ~year_end,
  "1617", ymd("2016-04-01"), ymd("2017-03-31"),
  "1718", ymd("2017-04-01"), ymd("2018-03-31"))
#   "1819", ymd("2018-04-01"), ymd("2019-03-31"),
#   "1920", ymd("2019-04-01"), ymd("2020-03-31"),
#   "2021", ymd("2020-04-01"), ymd("2021-03-31"),
#   "2122", ymd("2021-04-01"), ymd("2022-03-31"),
#   "2223", ymd("2022-04-01"), ymd("2023-03-31"),
#   "2324", ymd("2023-04-01"), ymd("2024-03-31"),
# )

# Outcome indicators ----

# NI12 and NI13 setup ----

prepped_slfs <- lapply(tester_years$year, function(x) {
  df <- prepare_slf_episode_file(x, ni_version = FALSE)
})

names(prepped_slfs) <- years_ni12_ni13$year

# NI12 ----

ni12_1617 <- calculate_ni12(prepped_slfs[["1617"]],
                            fin_year_start = (dplyr::filter(years_ni12_ni13, year == "1617"))$year_start,
                            fin_year_end = (dplyr::filter(years_ni12_ni13, year == "1617"))$year_end)

# NI13 ----

# NI14 ----

# NI15 ----

# NI16 ----

# NI17 ----

# NI18 ----

# NI19 ----

# NI20 ----

# Create Excel output ----

# Create Tableau output ----
