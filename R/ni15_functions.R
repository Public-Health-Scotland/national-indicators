#' Calculate length of stay (los) for records from SMR01, SMR01e, and SMR04
#'
#' @param data The outputs from [process_ni15_smr_extracts()]
#'
#' @return A data frame with length of stay (los)
calculate_length_of_stay <- function(data) {
  return_data <- data %>%
    # Calculate date six months before death
    dplyr::mutate(
      six_months = .data$date_of_death - lubridate::days(183),

      # For stays spanning this date, fix admission date to six months before death
      admission_date = dplyr::if_else(.data$admission_date < .data$six_months &
        .data$discharge_date >= .data$six_months,
      .data$six_months,
      .data$admission_date
      )
    ) %>%
    # Select only stays within last six months of life
    dplyr::filter(.data$admission_date >= .data$six_months &
      # Remove records where admission date is after date of death
      .data$admission_date <= .data$date_of_death) %>%
    # Where discharge date is after date of death, fix to date of death
    dplyr::mutate(
      discharge_date = dplyr::if_else(.data$discharge_date > .data$date_of_death,
        .data$date_of_death,
        .data$discharge_date
      ),

      # Calculate length of stay
      los = lubridate::time_length(lubridate::interval(.data$admission_date, .data$discharge_date), "days")
    ) %>%
    # Aggregate to patient level
    dplyr::group_by(.data$link_no) %>%
    dplyr::summarise(los = sum(.data$los)) %>%
    dplyr::ungroup() %>%
    # Recode 183 LOS to 182.5 (exact six months)
    dplyr::mutate(los = dplyr::if_else(.data$los == 183, 182.5, .data$los))

  return(return_data)
}

#' Match the deaths data and the SMR data, and split into calendar and financial year
#'
#' @param smr_data SMR data with length of stay calculated
#' @param deaths_data GRO data
#'
#' @return A list of data frames
match_smra_and_deaths_ni15 <- function(smr_data, deaths_data) {
  matched <- dplyr::left_join(deaths_data, smr_data, by = "link_no")

  cal_year <- matched %>%
    dplyr::select(-fin_month, -fy, -quarter) %>%
    dplyr::rename(
      time_period = cal_month,
      year = cal_year
    )

  fin_year <- matched %>%
    dplyr::select(-cal_month, -cal_year) %>%
    dplyr::rename(
      time_period = fin_month,
      year = fy
    )

  return(list(
    "cal_year" = cal_year,
    "fin_year" = fin_year
  ))
}

#' Add additional groups and aggregate to locality level
#'
#' @param data A data frame from [match_smra_and_deaths_ni15()]
#'
#' @return A data frame with additional groups
add_additional_groups_ni15 <- function(data) {
  aggregated <- fin_year %>%
    dplyr::group_by(year, time_period, ca2019name, hscp_locality) %>%
    dplyr::summarise(
      numerator = sum(los, na.rm = TRUE),
      denominator = dplyr::n()
    )

  all_groups <- aggregated %>%
    add_all_grouping("time_period", "Annual") %>%
    dplyr::mutate(temp_part = dplyr::if_else(.data$ca2019name %in% c("Clackmannanshire", "Stirling"),
      "Clackmannanshire and Stirling",
      NA_character_
    )) %>%
    tidyr::pivot_longer(
      cols = c(ca2019name, temp_part),
      values_to = "ca2019name",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Make partnership totals
    add_all_grouping("hscp_locality", "All") %>%
    # Make Scotland totals
    dplyr::mutate(temp_part = dplyr::if_else(ca2019name == "Clackmannanshire and Stirling",
      NA_character_,
      "Scotland"
    )) %>%
    tidyr::pivot_longer(
      cols = c(ca2019name, temp_part),
      values_to = "ca2019name",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(-name) %>%
    # Select out any rows where partnership is Scotland
    # and localities aren't 'all'
    dplyr::filter(ca2019name != "Scotland" | hscp_locality == "All") %>%
    # Aggregate
    dplyr::group_by(year, time_period, ca2019name, hscp_locality) %>%
    dplyr::summarise(
      numerator = sum(numerator),
      denominator = sum(denominator)
    ) %>%
    dplyr::ungroup()

  return(all_groups)
}

#' Create financial year and calendar year totals for NI15
#'
#' @param extract_start Start of the extract to be taken from SMRA (%d-%b-%Y)
#' @param extract_end End of the extract to be taken from SMRA (%d-%b-%Y)
#'
#' @return A list of data frames
#' @export
calculate_ni15 <- function(extract_start, extract_end) {
  smr_data <- process_ni15_smr_extracts(extract_start, extract_end) %>%
    calculate_length_of_stay()

  deaths_data <- process_ni15_gro_extract(extract_start, extract_end)

  matched_list <- match_smra_and_deaths_ni15(smr_data, deaths_data)

  return_list <- matched_list %>%
    purrr::map(~ add_additional_groups_ni15(.x) %>%
      dplyr::mutate(
        value = 100 - (((numerator / denominator) / 182.5) * 100),
        indicator = "NI15"
      ))

  return(return_list)
}
