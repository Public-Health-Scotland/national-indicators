## Environment setup for National Indicators

# Load libraries ----
library(dplyr) # General
library(tidyr) # Mainly for pivot commands
library(haven) # For reading/writing .sav files
library(readr) # Reading other files
library(fs) # Tidy file paths
library(janitor) # Data cleaning
library(magrittr) # For assignment pipe
library(lubridate) # Handles dates easier
library(stringr) # Handling strings
library(glue) # For strings and code calls
library(phsmethods) # For geography matching and fin_year()
library(slfhelper) # Easy reading of Source Episode Files
library(purrr) # For map functions over lists
library(writexl) # Write excel
library(readxl)
library(tidylog)
library(reshape2)
library(dbplyr) # For reading SMRA

# Constants ----

# Get the current month in 'MMM-YY' format, for appending save outs
update_month <- str_c(month.abb[month(Sys.Date())], "-", year(Sys.Date()))
last_update <- ""
# General use - column names that every indicator dataset follows
ni_columns <- c()
# For NI 15, list of ICD-10 codes for external causes of death
external_cause_codes <- purrr::reduce(
  list(
    glue("V0{1:9}"), glue("V{10:99}"),
    glue("W0{0:9}"), glue("W{10:99}"),
    glue("X0{0:9}"), glue("X{10:99}"),
    glue("Y0{0:9}"), glue("Y{10:84}")
  ),
  union
)
# Codes representing falls
falls_codes <- union(glue("W0{as.character(c(0:9))}"), glue("W{as.character(c(10:19))}"))
excluded_codes <- generics::setdiff(external_cause_codes, falls_codes)
# Frr NI 15, excluded locations
excluded_locations <- c(
  "A240V", "F821V", "G105V", "G518V", "G203V", "G315V", "G424V", "G541V", "G557V", "H239V",
  "L112V", "L213V", "L215V", "L330V", "L365V", "N465R", "N498V", "S312R", "S327V", "T315S",
  "T337V", "Y121V"
)

# Geography Lookups ----

# Returns a lookup with simd, locality, datazone based on postcode
# For use with NI15
big_lookup <- function() {
  lookup_dir <- fs::path("/", "conf", "linkage", "output", "lookups", "Unicode")

  simd <- read_sav(
    fs::path(lookup_dir, "Deprivation", "postcode_2022_1_simd2020v2.zsav")
  ) %>%
    select("pc7", "simd2020v2_sc_decile", "simd2020v2_sc_quintile", "simd2020v2tp15")

  postcode <- read_sav(
    fs::path("/", "conf", "hscdiip", "SLF_Extracts", "Lookups", "Scottish_Postcode_Directory_2022_1.zsav")
  ) %>%
    clean_names() %>%
    select("pc7", "hb2018", "hscp2018", "ca2018", datazone2011 = "data_zone2011")

  locality <- read_rds(
    fs::path(lookup_dir, "Geography", "HSCP Locality", "HSCP Localities_DZ11_Lookup_20200825.rds")
  ) %>%
    select("datazone2011", "datazone2011name", locality = "hscp_locality")

  big_lookup <- postcode %>%
    left_join(simd, by = "pc7") %>%
    left_join(locality, by = "datazone2011")

  return(big_lookup)
}

# Standard LCA names
standardise_partnerships <- function(df, partnership) {
  return_df <- df %>%
    mutate(across({{ partnership }}, ~ .x %>%
      str_replace("^City of Edinburgh$", "Edinburgh") %>%
      str_replace("Edinburgh, City of", "Edinburgh") %>%
      str_replace("Na h-Eileanan Siar", "Western Isles") %>%
      str_replace("&", "and") %>%
      str_replace("(^Orkney$|^Shetland$)", "\\1 Islands")))
  return(return_df)
}

# Date functions ----
# Function to get the financial year and month within financial year
fin_year_month <- function(dataset, date_variable) {
  # Formats financial year as '20XX/YY' and gets the month number
  return_df <- df %>%
    mutate(year = fin_year({{ date_variable }})) %>%
    # Assigns months a value based on April
    # being the start of the financial year
    mutate(fin_month = case_when(
      between(month({{ date_variable }}), 1, 3)
      ~ month({{ date_variable }}) + 9,
      between(month({{ date_variable }}), 4, 12)
      ~ month({{ date_variable }}) - 3
    )) %>%
    # Puts an 'M' in front of the month number and makes 'month' a string
    mutate(month = paste0("M", fin_month)) %>%
    select(-fin_month)
  return(return_df)
}

to_fin_year <- function(df) {
  return_df <- df %>%
    mutate(last_two_digits = substr(year, 3, 4)) %>%
    mutate(next_year = as.numeric(last_two_digits) + 1) %>%
    mutate(year = str_c("20", last_two_digits, "/", next_year)) %>%
    select(-last_two_digits, -next_year)
  return(return_df)
}

# Populations ----
get_loc_pops <- function(est_years) {
  dz_pops <- readr::read_rds(fs::path("/", "conf", "linkage", "output", "lookups", "Unicode", "Populations", "Estimates", glue::glue("DataZone2011_pop_est_{est_years}.rds"))) %>%
    filter(year >= 2013) %>%
    mutate(
      over18_pop = rowSums(across(age18:age90plus)),
      over65_pop = rowSums(across(age65:age90plus)),
      over75_pop = rowSums(across(age75:age90plus))
    ) %>%
    select(-c(age0:age90plus)) %>%
    group_by(year, datazone2011) %>%
    summarise(across(over18_pop:over75_pop, sum), .groups = "keep")

  temp_pc <- get_pc_lookup() %>%
    select(ca2019name, datazone2011) %>%
    group_by(datazone2011) %>%
    summarise(lca = first(ca2019name))

  loc_pops <- left_join(dz_pops, temp_pc, by = "datazone2011") %>%
    left_join(., get_locality_lookup(),
      by = "datazone2011"
    ) %>%
    group_by(year, lca, hscp_locality) %>%
    summarise(across(over18_pop:over75_pop, sum), .groups = "keep")

  loc_pops <- loc_pops %>%
    mutate(
      temp_year1 = if_else(year == 2020, 2021, NA_real_),
      temp_year2 = if_else(year == 2020, 2022, NA_real_)
    ) %>%
    pivot_longer(
      cols = c(year, temp_year1, temp_year2),
      values_to = "year",
      values_drop_na = TRUE
    ) %>%
    select(-name) %>%
    mutate(temp_part = if_else(lca == "Clackmannanshire" | lca == "Stirling",
      "Clackmannanshire and Stirling",
      NA_character_
    )) %>%
    pivot_longer(
      cols = c(lca, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    select(-name) %>%
    mutate(temp_loc = "All") %>%
    pivot_longer(
      cols = c(hscp_locality, temp_loc),
      values_to = "locality",
      values_drop_na = TRUE
    ) %>%
    select(-name) %>%
    mutate(temp_part = if_else(partnership == "Clackmannanshire and Stirling",
      NA_character_,
      "Scotland"
    )) %>%
    pivot_longer(
      cols = c(partnership, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    select(-name) %>%
    filter(partnership != "Scotland" | locality == "All") %>%
    group_by(year, partnership, locality) %>%
    summarise(across(over18_pop:over75_pop, sum), .groups = "keep") %>%
    to_fin_year()

  return(loc_pops)
}

# Function to get length of stay within calendar months
create_monthly_beddays <- function(data, year,
                                   admission_date, discharge_date,
                                   include_costs = FALSE, count_last = TRUE,
                                   pivot_longer = FALSE) {
  # Create a 'stay interval' from the episode dates
  data <- data %>%
    dplyr::mutate(stay_interval = lubridate::interval(
      {{ admission_date }},
      {{ discharge_date }}
    ) %>%
      # Shift it forward by a day (default)
      # so we will count the last day and not the first.
      lubridate::int_shift(by = lubridate::days(ifelse(count_last, 1, 0))))

  # Create the start dates of the months for the financial year
  cal_year <- as.numeric(str_c("20", str_sub(as.character(year), 1, 2)))
  month_start <- c(
    lubridate::my(paste0(
      month.abb[4:12],
      cal_year
    )),
    lubridate::my(paste0(
      month.abb[1:3],
      cal_year + 1
    ))
  )

  # Turn the start dates into 1 month intervals
  month_intervals <- lubridate::interval(
    month_start,
    month_start + months(1)
  ) %>%
    # Name the intervals for use later
    rlang::set_names(paste0(tolower(month.abb[c(4:12, 1:3)]), "_beddays"))

  # Work out the beddays for each month
  beddays <- purrr::map_dfc(
    month_intervals,
    ~ lubridate::intersect(data$stay_interval, .x) %>%
      lubridate::time_length(unit = "days") %>%
      # Replace any NAs with zero
      tidyr::replace_na(0) %>%
      as.integer()
  )

  # Join the beddays back to the data
  data <- dplyr::bind_cols(data, beddays) %>%
    dplyr::select(-.data$stay_interval)

  # Work out costs for each month if applicable
  if (include_costs) {
    costs <- beddays %>%
      dplyr::rename_with(~ stringr::str_replace(., "_beddays", "_costs"))

    data <- dplyr::bind_cols(data, costs) %>%
      dplyr::mutate(dplyr::across(dplyr::ends_with("_costs"), ~ dplyr::if_else(.x != 0, .x / yearstay * cost_total_net, 0)))
  }

  if (pivot_longer) {
    data <- data %>%
      # Use pivot longer to create a month, year and beddays column which
      # can be used to aggregate later
      pivot_longer(
        cols = c(contains("beddays"), contains("cost")),
        names_to = c("month", ".value"),
        names_sep = "_"
      )
  }

  # Change month to "Mx" format, starting at April
  data <- data %>%
    mutate(month = str_c("M", as.character((match(month, str_to_lower(month.abb)) - 3) %% 12))) %>%
    mutate(month = if_else(month == "M0", "M12", month)) %>%
    left_join(., get_locality_lookup()) %>%
    # Aggregate to get monthly totals at locality level
    group_by(lca, ca2019name, hscp_locality, month) %>%
    summarise(across(c("beddays", "cost"), sum, na.rm = TRUE),
      .groups = "keep"
    ) %>%
    ungroup()

  return(data)
}
