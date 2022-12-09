#' Add flag for emergency readmissions within 28 days of last admission
#' @description An emergency readmission is defined as an admission with
#' codes 20, 21, 22 or between 30-39. If the person's next admission is within
#' 28 days of the previous one and is an emergency, set `flag28` = `TRUE`.
#'
#' @param data A data frame
#'
#' @return A data frame with a new variable, flag28
#' @export
add_readmission_flag <- function(data) {
  return_data <- data %>%
    dplyr::group_by(.data$link_no) %>%
    dplyr::arrange(.data$cis_admdate, .data$cis_disdate, .by_group = TRUE) %>%
    dplyr::mutate(
      # If link_no is the same as the next link_no, calculate how many days there
      # are between the next admission and the last discharge
      days_between_stays = lubridate::interval(.data$cis_disdate, dplyr::lead(.data[["cis_admdate"]])) / lubridate::ddays(1),
      # Make a flag, emerg_adm, for admission types 20, 21, 22, and 30-39.
      emerg_adm = .data$admission_type %in% c(20:22, 30:39),
      # If the next admission was an emergency
      # and it was less than 28 days later, assign TRUE to flag28
      flag28 = dplyr::between(.data$days_between_stays, 0, 28) & dplyr::lead(.data[["emerg_adm"]]),
      # Flag admissions where a patient died, as these are excluded
      dplyr::across("flag28", ~ replace(., is.na(.), FALSE))
    )
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
