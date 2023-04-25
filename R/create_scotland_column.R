#' Create an additional column with Scotland values for use in Tableau
#'
#' @param data A data frame containing rows where the partnership is Scotland
#'
#' @return A data frame with an additional variable, `scotland`, where the values
#' can be matched on `year` and `time_period`
#' @export
create_scotland_column <- function(data) {
  return_data <- data %>%
    dplyr::filter(partnership == "Scotland") %>%
    dplyr::select("year", "time_period", scotland = "value")

  return(return_data)
}
