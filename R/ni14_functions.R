#' Add flag for emergency readmissions within 28 days of last admission
#' @description An emergency readmission is defined as an admission with
#' codes 20, 21, 22 or between 30-39. If the person's next admission is within
#' 28 days of the previous one and is an emergency, set `emergency_readm_28` = `TRUE`.
#'
#' @param data A data frame
#'
#' @return A data frame with a new variable, emergency_readm_28
add_readmission_flag <- function(data) {
  return_data <- data %>%
    dplyr::group_by(.data$link_no) %>%
    dplyr::arrange("cis_admdate", "cis_disdate", .by_group = TRUE) %>%
    dplyr::mutate(
      # Bring the following admission date up in line with discharge date
      next_admission = dplyr::lead(.data$cis_admdate),
      # Find out the next admission type for the link_no
      next_admission_type = dplyr::lead(.data$admission_type),
      # If the next readmission was an emergency, flag it
      emergency_readm = .data$next_admission_type %in% c(20:22, 30:39),
      # Find the number of days between discharge and next admission
      days_between_stays = lubridate::time_length(
        lubridate::interval(.data$cis_disdate, .data$next_admission), "days"
      ),
      # Flag if the readmission was within 28 days and it was an emergency
      emergency_readm_28 = .data$days_between_stays %in% 0:28 & .data$emergency_readm,
      dplyr::across("emergency_readm_28", ~ replace(., is.na(.), FALSE))
    )

  return(return_data)
}

#' Read in and process SMRA extract for NI14
#'
#' @param min_date Defaults to 01-APR-2013 but can be changed. Represents earliest
#' discharge date from SMRA
#'
#' @return A tibble
#' @import data.table
process_ni14_smra_extract <- function(min_date = "01-APR-2013") {
  # Fetch the SMRA query from SQL file, min_date is used here for earliest
  # discharge date
  smra_query <- glue::glue(readr::read_file("SQL/ni14_smra.sql"))

  # Read in data
  smra_extract <-
    # Get SMRA extract
    tibble::as_tibble(
      odbc::dbGetQuery(connect_to_smra(), smra_query)
    ) %>%
    janitor::clean_names() %>%
    # Convert to date format for easier processing
    dplyr::mutate(
      admission_date = as.Date(.data$admission_date),
      discharge_date = as.Date(.data$discharge_date),
      discharge_type = as.integer(.data$discharge_type)
    ) %>%
    # Aggregate to cis level
    data.table::as.data.table() %>%
    dplyr::group_by(link_no, cis_marker) %>%
    dplyr::summarise(
      cis_admdate = min(admission_date),
      cis_disdate = max(discharge_date),
      admission_type = dplyr::first(admission_type),
      discharge_type = dplyr::last(discharge_type),
      postcode = dplyr::last(postcode)
    ) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble() %>%
    # Add flag for emergency readmissions within 28 days
    add_readmission_flag()

  return(smra_extract)
}

#' Read in and process GRO Deaths extract for NI14
#'
#' @param min_date The minimum date for date of death, read from gro
#'
#' @return A data frame of death dates and link_no
process_ni14_nrs_extract <- function(min_date = "01-APR-2022") {
  # Read query from SQL file
  nrs_query <- glue::glue(readr::read_file("SQL/ni14_gro.sql"))

  nrs_extract <- odbc::dbGetQuery(connect_to_smra(), nrs_query) %>%
    tibble::as_tibble() %>%
    # For R-standard column names
    janitor::clean_names()

  return(nrs_extract)
}

#' Match SMRA extract to NRS deaths extract to ensure data quality and
#' removal of records where individual died
#'
#' @param smra_data Extract created by [process_ni14_smra_extract]
#' @param nrs_data Extract created by [process_ni14_nrs_extract]
#'
#' @return A matched data frame with additional variables, key variable being 'stay',
#' which is TRUE when individual is not dead
match_smra_and_deaths <- function(smra_data, nrs_data) {
  matched_extracts <-
    dplyr::left_join(smra_data, nrs_data, by = "link_no", relationship = "many-to-one") %>%
    dplyr::mutate(
      # Variable for if person is discharged dead in SMRA, any discharge type
      # in the 40s
      discharged_dead = .data$discharge_type %/% 10 == 4,
      # Time between SMRA discharge date and NRS death date
      discharge_to_death = lubridate::int_length(lubridate::interval(cis_disdate, death_date)),
      # Flag if discharge occurs after death or death occurs before admission (data quality)
      death_before_discharge = .data$discharge_to_death <= 0 & death_date >= cis_admdate,
      # Flag to confirm person is declared dead in both extracts
      discharged_dead_both = death_before_discharge | discharged_dead,
      discharged_dead_both = tidyr::replace_na(discharged_dead_both, FALSE),
      # Set up a flag to keep records where patient is not dead at discharge date
      stay = !.data$discharged_dead_both,
    )
  return(matched_extracts)
}

#' Calculate locality totals
#'
#' @param data
#'
#' @return a [tibble][tibble::tibble-package]
calculate_locality_totals <- function(data) {
  geog_data <- data %>%
    get_locality_from_postcode() %>%
    dplyr::mutate(
      fin_month = calculate_financial_month(cis_disdate),
      fin_year = phsmethods::extract_fin_year(cis_disdate),
      cal_month = lubridate::month(cis_disdate),
      cal_year = lubridate::year(cis_disdate),
      partnership = phsmethods::match_area(ca2018)
    ) %>%
    dplyr::select(
      "fin_year",
      "fin_month",
      "cal_year",
      "cal_month",
      "partnership",
      `locality` = "hscp_locality",
      "emergency_readm_28",
      "stay"
    )

  fin_year_data <- geog_data %>%
    dplyr::group_by(fin_year, fin_month, partnership, locality) %>%
    dplyr::summarise(
      dplyr::across(c("emergency_readm_28", "stay"), ~ sum(.x, na.rm = TRUE))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::rename(year = fin_year, month = fin_month)

  cal_year_data <- geog_data %>%
    dplyr::group_by(cal_year, cal_month, partnership, locality) %>%
    dplyr::summarise(
      dplyr::across(c("emergency_readm_28", "stay"), ~ sum(.x, na.rm = TRUE))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::rename(year = cal_year, month = cal_month)

  return(list(
    "fin_year" = fin_year_data,
    "cal_year" = cal_year_data
  ))
}

#' Add 'all' groups to data frame - annual totals, partnership totals,
#' Clackmannanshire & Stirling totals, and Soctland totals, then aggregate
#'
#' @param data A data frame
#'
#' @return A data frame with the additional totals
add_additional_groups_ni14 <- function(data) {
  return_data <- data %>%
    # Make annual totals
    dplyr::mutate(month = as.character(month)) %>%
    add_all_grouping("month", "Annual") %>%
    # Make Clackmannanshire & Stirling totals
    dplyr::mutate(temp_part = dplyr::if_else(partnership %in% c("Clackamannanshire", "Stirling"),
      "Clackmannanshire and Stirling",
      NA_character_
    )) %>%
    tidyr::pivot_longer(
      cols = c(partnership, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Make partnership totals
    add_all_grouping("locality", "All") %>%
    # Make Scotland totals
    dplyr::mutate(temp_part = dplyr::if_else(partnership == "Clackmannanshire and Stirling",
      NA_character_,
      "Scotland"
    )) %>%
    tidyr::pivot_longer(
      cols = c(partnership, temp_part),
      values_to = "partnership",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Select out any rows where partnership is Scotland
    # and localities aren't 'all'
    dplyr::filter(partnership != "Scotland" | locality == "All") %>%
    # Aggregate
    dplyr::group_by(year, month, partnership, locality) %>%
    dplyr::summarise(dplyr::across(c("emergency_readm_28", "stay"), ~ sum(.x, na.rm = TRUE))) %>%
    dplyr::ungroup()

  return(return_data)
}

#' Use helper functions to calculate the final totals for NI14
#'
#' @return A data frame
#' @export
calculate_ni14 <- function() {
  smra_data <- process_ni14_smra_extract()
  nrs_data <- process_ni14_nrs_extract()

  final_data <-
    match_smra_and_deaths(smra_data, nrs_data) %>%
    calculate_locality_totals() %>%
    purrr::map(~ add_additional_groups_ni14(.x)) %>%
    purrr::map(~ dplyr::mutate(.x, value = .data$emergency_readm_28 / .data$stay * 1000))

  return(final_data)
}
