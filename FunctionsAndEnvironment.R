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
library(phsmethods) # For geography matching and phsmethods::fin_year()
library(slfhelper) # Easy reading of Source Episode Files
library(purrr) # For map functions over lists
library(writexl) # Write excel
library(readxl)
library(tidylog)
library(reshape2)
library(dbplyr) # For reading SMRA

# Constants ----

# Get the current month in 'MMM-YY' format, for appending save outs
update_month <- stringr::str_c(month.abb[lubridate::month(Sys.Date())], "-", lubridate::year(Sys.Date()))

last_update <- ""

# General use - column names that every indicator dataset follows
ni_columns <- c()

# For NI 15, list of ICD-10 codes for external causes of death
external_cause_codes <- purrr::reduce(
  list(
    glue::glue("V0{1:9}"), glue::glue("V{10:99}"),
    glue::glue("W0{0:9}"), glue::glue("W{10:99}"),
    glue::glue("X0{0:9}"), glue::glue("X{10:99}"),
    glue::glue("Y0{0:9}"), glue::glue("Y{10:84}")
  ),
  union
)
# Codes representing falls
falls_codes <- union(
  glue::glue("W0{as.character(c(0:9))}"),
  glue::glue("W{as.character(c(10:19))}")
)

excluded_codes <- generics::setdiff(external_cause_codes, falls_codes)

# Frr NI 15, excluded locations
excluded_locations <- c(
  "A240V",
  "F821V",
  "G105V",
  "G518V",
  "G203V",
  "G315V",
  "G424V",
  "G541V",
  "G557V",
  "H239V",
  "L112V",
  "L213V",
  "L215V",
  "L330V",
  "L365V",
  "N465R",
  "N498V",
  "S312R",
  "S327V",
  "T315S",
  "T337V",
  "Y121V"
)

# Geography Lookups ----

# Returns a lookup with simd, locality, datazone based on postcode
# For use with NI15
big_lookup <- function() {
  lookup_dir <- fs::path("/", "conf", "linkage", "output", "lookups", "Unicode")

  simd <- haven::read_sav(
    fs::path(lookup_dir, "Deprivation", "postcode_2022_1_simd2020v2.zsav")
  ) %>%
    dplyr::select("pc7", "simd2020v2_sc_decile", "simd2020v2_sc_quintile", "simd2020v2tp15")

  postcode <- haven::read_sav(
    fs::path("/", "conf", "hscdiip", "SLF_Extracts", "Lookups", "Scottish_Postcode_Directory_2022_1.zsav")
  ) %>%
    janitor::clean_names() %>%
    dplyr::select("pc7", "hb2018", "hscp2018", "ca2018", datazone2011 = "data_zone2011")

  locality <- readr::read_rds(
    fs::path(lookup_dir, "Geography", "HSCP Locality", "HSCP Localities_DZ11_Lookup_20200825.rds")
  ) %>%
    dplyr::select("datazone2011", "datazone2011name", locality = "hscp_locality")

  big_lookup <- postcode %>%
    dplyr::left_join(simd, by = "pc7") %>%
    dplyr::left_join(locality, by = "datazone2011")

  return(big_lookup)
}

# Standard LCA names
standardise_partnerships <- function(df, partnership) {
  return_df <- df %>%
    dplyr::mutate(dplyr::across({{ partnership }}, ~ .x %>%
      stringr::str_replace("^City of Edinburgh$", "Edinburgh") %>%
      stringr::str_replace("Edinburgh, City of", "Edinburgh") %>%
      stringr::str_replace("Na h-Eileanan Siar", "Western Isles") %>%
      stringr::str_replace("&", "and") %>%
      stringr::str_replace("(^Orkney$|^Shetland$)", "\\1 Islands")))
  return(return_df)
}

# Date functions ----
# Function to get the financial year and month within financial year
fin_year_month <- function(dataset, date_variable) {
  # Formats financial year as '20XX/YY' and gets the month number
  return_df <- df %>%
    dplyr::mutate(year = phsmethods::fin_year({{ date_variable }})) %>%
    # Assigns months a value based on April
    # being the start of the financial year
    dplyr::mutate(fin_month = dplyr::case_when(
      dplyr::between(lubridate::month({{ date_variable }}), 1, 3)
      ~ lubridate::month({{ date_variable }}) + 9,
      dplyr::between(lubridate::month({{ date_variable }}), 4, 12)
      ~ lubridate::month({{ date_variable }}) - 3
    )) %>%
    # Puts an 'M' in front of the month number and makes 'month' a string
    dplyr::mutate(month = paste0("M", fin_month)) %>%
    dplyr::select(-"fin_month")
  return(return_df)
}

to_fin_year <- function(df) {
  return_df <- df %>%
    dplyr::mutate(last_two_digits = substr(year, 3, 4)) %>%
    dplyr::mutate(next_year = as.numeric(last_two_digits) + 1) %>%
    dplyr::mutate(year = stringr::str_c("20", last_two_digits, "/", next_year)) %>%
    dplyr::select(-"last_two_digits", -"next_year")
  return(return_df)
}

# Populations ----
get_loc_pops <- function(ext = "rds", min_year = 2013) {
  dz_pops <- readr::read_rds(
    get_loc_pops_path(ext = ext)
  ) %>%
    dplyr::filter(year >= min_year) %>%
    dplyr::mutate(
      over18_pop = rowSums(dplyr::across("age18":"age90plus")),
      over65_pop = rowSums(dplyr::across("age65":"age90plus")),
      over75_pop = rowSums(dplyr::across("age75":"age90plus"))
    ) %>%
    dplyr::select(-c("age0":"age90plus")) %>%
    dplyr::group_by(year, datazone2011) %>%
    dplyr::summarise(dplyr::across("over18_pop":"over75_pop", sum), .groups = "keep")

  temp_pc <- get_spd(c("ca2019name", "datazone2011")) %>%
    dplyr::group_by(datazone2011) %>%
    dplyr::summarise(lca = dplyr::first(ca2019name))

  loc_pops <- dz_pops %>%
    dplyr::left_join(temp_pc, by = "datazone2011") %>%
    dplyr::left_join(get_locality_lookup(), by = "datazone2011") %>%
    dplyr::group_by(year, lca, hscp_locality) %>%
    dplyr::summarise(dplyr::across("over18_pop":"over75_pop", sum), .groups = "keep")

  loc_pops <- loc_pops %>%
    dplyr::mutate(
      temp_year1 = dplyr::if_else(year == 2020, 2021, NA_real_),
      temp_year2 = dplyr::if_else(year == 2020, 2022, NA_real_)
    ) %>%
    tidyr::pivot_longer(
      cols = c(year, temp_year1, temp_year2),
      values_to = "year",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-"name") %>%
    dplyr::mutate(temp_part = dplyr::if_else(lca %in% c("Clackmannanshire", "Stirling"),
      "Clackmannanshire and Stirling",
      NA_character_
    )) %>%
    tidyr::pivot_longer(
      cols = c(lca, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-"name") %>%
    dplyr::mutate(temp_loc = "All") %>%
    tidyr::pivot_longer(
      cols = c(hscp_locality, temp_loc),
      values_to = "locality",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-"name") %>%
    dplyr::mutate(temp_part = dplyr::if_else(partnership == "Clackmannanshire and Stirling",
      NA_character_,
      "Scotland"
    )) %>%
    tidyr::pivot_longer(
      cols = c(partnership, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-"name") %>%
    dplyr::filter(partnership != "Scotland" | locality == "All") %>%
    dplyr::group_by(year, partnership, locality) %>%
    dplyr::summarise(dplyr::across(over18_pop:over75_pop, sum), .groups = "keep") %>%
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
  cal_year <- as.numeric(stringr::str_c("20", stringr::str_sub(as.character(year), 1, 2)))
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
      tidyr::pivot_longer(
        cols = c(contains("beddays"), contains("cost")),
        names_to = c("month", ".value"),
        names_sep = "_"
      )
  }

  # Change month to "Mx" format, starting at April
  data <- data %>%
    dplyr::mutate(month = stringr::str_c("M", as.character((match(month, stringr::str_to_lower(month.abb)) - 3) %% 12))) %>%
    dplyr::mutate(month = dplyr::if_else(month == "M0", "M12", month)) %>%
    dplyr::left_join(get_locality_lookup()) %>%
    # Aggregate to get monthly totals at locality level
    dplyr::group_by(lca, ca2019name, hscp_locality, month) %>%
    dplyr::summarise(dplyr::across(c("beddays", "cost"), sum, na.rm = TRUE),
      .groups = "keep"
    ) %>%
    dplyr::ungroup()

  return(data)
}

get_locality_lookup <- function(path = get_locality_path()) {
  readr::read_rds(get_locality_path())
}

get_spd <- function(vars = NULL, ext = "rds") {
  spd <- readr::read_rds(get_spd_path(ext = ext))

  if (!is.null(vars)) {
    spd <- dplyr::select(spd, dplyr::all_of(vars))
  }

  return(spd)
}
