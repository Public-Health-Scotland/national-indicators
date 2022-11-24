#' Convert financial month to financial quarter
#' @note The return vector will preserve any Annual values
#'
#' @param month_variable A vector of financial months in "MX" format
#'
#' @return A vector of financial quarters in "QX" format
#' @export
#'
#' @family Output functions
convert_fin_month_to_quarter <- function(month_variable) {
  accepted_months <- c(
    "M1", "M2", "M3", "M4", "M5", "M6",
    "M7", "M8", "M9", "M10", "M11", "M12",
    "Annual"
  )

  if (any(!(month_variable %in% accepted_months))) {
    offending_rows <- which(!(month_variable %in% accepted_months))
    cli::cli_abort("{cli::qty(length(offending_rows))}The value{?s} on the following
                   row{?s} are invalid for conversion:
                   {offending_rows}. Please check the data.")
  }

  quarters <- dplyr::case_when(
    month_variable %in% c("M1", "M2", "M3") ~ "Q1",
    month_variable %in% c("M4", "M5", "M6") ~ "Q2",
    month_variable %in% c("M7", "M8", "M9") ~ "Q3",
    month_variable %in% c("M10", "M11", "M12") ~ "Q4",
    month_variable %in% c("Annual") ~ "Annual"
  )

  return(quarters)
}

#' Recalculate monthly indicator values into quarterly values
#'
#' @param data A data frame
#'
#' @return A data frame with Value and Denominator recalculated on a quarterly basis
#'
#' @seealso [convert_fin_month_to_quarter()]
#' @export
#' @family Output functions
recalculate_quarterly_values <- function(data) {
  return_data <- data %>%
    # Group by break variables
    dplyr::group_by(Indicator1, Year1, Data1, Partnership1, Locality) %>%
    # For indicators 14 and 15, we want to add the denominators together,
    # and for all other indicators we want the maximum
    dplyr::summarise(
      Denominator = dplyr::case_when(
        Indicator1 %in% c("NI14", "NI15") ~ sum(Denominator),
        TRUE ~ max(Denominator)
      ),
      # We take the sum of the indicator values, numerators, and Scotland values
      dplyr::across(c("Value", "Scotland", "Numerator"), sum)
    ) %>%
    dplyr::ungroup() %>%
    # Now we recalculate the indicator values for NI14 and NI15 based
    # on their new denominators, and leave the value as-is for the others
    dplyr::mutate(Value = dplyr::case_when(
      Indicator1 %in% c("NI14") ~ (Numerator / Denominator) * 1000,
      Indicator1 %in% c("NI15") ~ 100 - ((Numerator / (Denominator * 182.5)) * 100),
      TRUE ~ Value
    )) %>%
    # As the above will create three identical rows per quarter for all indicators,
    # representing the three months in each quarter we started with, we remove duplicates
    dplyr::distinct()

  return(return_data)
}

#' Join a recalculated Scotland column to the indicator data frame
#'
#' @param data A data frame with recalculated indicator values
#'
#' @return A data frame with the Scotland column replaced with accurate values
#' @export
#' @family Output functions
join_recalculated_scotland_column <- function(data) {
  return_data <- dplyr::left_join(
    # Get the dataset without Scotland values as these will be wrong
    data %>%
      dplyr::filter(Partnership1 != "Scotland") %>%
      dplyr::select(-"Scotland"),
    # Put the Scotland values into the Scotland column and use this as a lookup
    # for the above
    data %>%
      dplyr::filter(Partnership1 == "Scotland") %>%
      dplyr::select("Year1", "Indicator1", "Data1", "Value") %>%
      dplyr::rename(Scotland = Value)
  )
}
