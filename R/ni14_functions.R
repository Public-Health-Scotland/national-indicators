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
        lubridate::interval(.data$cis_disdate, .data$next_admission), "days"),
      # Flag if the readmission was within 28 days and it was an emergency
      emergency_readm_28 = .data$days_between_stays %in% 0:28 & .data$emergency_readm,
      dplyr::across("emergency_readm_28", ~ replace(., is.na(.), FALSE)))

  return(return_data)
}

#' Create a flag of deaths from the SMRA discharge type
#'
#' @param discharge_type A vector of discharge types. Must be of class `integer`
#'
#' @return A boolean vector of death flags
add_smra_death_flag <- function(discharge_type) {
  discharged_dead <- discharge_type %/% 10 == 4
  return(discharged_dead)
}

#' Read in and process SMRA extract for NI14
#'
#' @return A tibble
#' @import data.table
process_ni14_smra_extract <- function() {
  # Read in data
  smra_extract <-
    tibble::as_tibble(
      odbc::dbGetQuery(connect_to_smra(), readr::read_file("SQL/ni14_smra.sql"))
    ) %>%
    # For R-standard column names
    janitor::clean_names() %>%
    # Make sure these variables are in date format for quicker summarising
    dplyr::mutate(
      cis_admdate = as.Date(.data$admission_date),
      cis_disdate = as.Date(.data$discharge_date),
      discharge_type = as.integer(.data$discharge_type)
    ) %>%
    # Aggregate to unique CIS level, based on link_no and cis_marker
    # Aggregate to unique CIS level, based on link_no and cis_marker
    data.table::as.data.table() %>%
    dplyr::group_by(link_no, cis_marker) %>%
    dplyr::summarise(
      cis_admdate = min(cis_admdate),
      cis_disdate = max(cis_disdate),
      admission_type = dplyr::first(admission_type),
      discharge_type = dplyr::last(discharge_type)
    ) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble() %>%
    add_readmission_flag() %>%
    dplyr::mutate(discharged_dead = add_smra_death_flag(discharge_type))

  return(smra_extract)
}

#' Read in and process GRO Deaths extract for NI14
#'
#' @return A data frame
process_ni14_gro_extract <- function() {
  gro_extract <-
    tibble::as_tibble(
      odbc::dbGetQuery(connect_to_smra(), readr::read_file("SQL/ni14_gro.sql"))
    ) %>%
    # For R-standard column names
    janitor::clean_names() %>%
    # Order by link no for matching
    dplyr::arrange(.data$link_no) %>%
    # Use Lubridate to put into date format
    dplyr::mutate(death_date = as.Date(.data$death_date))
  return(gro_extract)
}

match_smra_and_deaths <- function(smra_data, gro_data) {
  # Match the death dates onto the main table
  return_data <-
    dplyr::left_join(smra_data,
                     gro_data,
                     by = "link_no") %>%
    # If the death date is the same as a discharge
    # we will discount it, as this cannot result in a readmission
    dplyr::mutate(
      discharge_to_death = .data$death_date - .data$cis_disdate,
      discharged_dead_both =
        discharge_to_death <= 0 & death_date >= cis_admdate |
        discharged_dead,
      dplyr::across("discharged_dead_both", ~ replace(., is.na(.), FALSE)),
      # Set up a flag to keep records where patient is not dead at discharge date
      stay = !discharged_dead_both,
      # Extract the financial year and financial month
      fin_month = calculate_financial_month(cis_disdate),
      year = phsmethods::extract_fin_year(cis_disdate)
    )
  return(return_data)
}

match_on_geographies <- function(data) {
  return_data <- dplyr::left_join(data,
    readr::read_rds(get_spd_path()),
    by = c("postcode" = "pc7")
  ) %>%
    dplyr::left_join(.,
      readr::read_rds(get_locality_path()) %>%
        dplyr::select(datazone2011, hscp_locality),
      by = "datazone2011"
    )
  return(return_data)
}

#' Title
#'
#' @param data
#'
#' @return
#' @import data.table
#'
#' @examples
calculate_locality_totals <- function(data) {
  # Aggregate to locality-level at the lowest
  return_data <- data %>%
    dplyr::select(year, fin_month, ca2018, hscp_locality, datazone2011, emergency_readm_28, stay) %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(year, fin_month, ca2018, hscp_locality) %>%
    dplyr::summarise(dplyr::across(emergency_readm_28:stay, sum, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble() %>%
    dplyr::mutate(partnership = phsmethods::match_area(ca2018))
  return(return_data)
}

#' Use helper functions to calculate the final totals for NI14
#'
#' @return A data frame
#' @export
calculate_ni14 <- function() {
  smra_data <- process_ni14_smra_extract()
  gro_data <- process_ni14_gro_extract()

  final_data <-
    match_smra_and_deaths(smra_data, gro_data) %>%
    match_on_geographies() %>%
    calculate_locality_totals()
  return(final_data)

}
