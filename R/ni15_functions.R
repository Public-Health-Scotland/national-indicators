#' Calculate length of stay (los) for records from SMR01, SMR01e, and SMR04
#'
#' @param data The outputs from [process_ni15_smr_extracts()]
#'
#' @return A data frame with length of stay (los)
#' @export
calculate_length_of_stay <- function(data) {
  return_data <- data %>%
    # Calculate date six months before death
    dplyr::mutate(
      six_months = .data$date_of_death - days(183),

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
    dplyr::mutate(los = if_else(.data$los == 183, 182.5, .data$los))

  return(return_data)
}
