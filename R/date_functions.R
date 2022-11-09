#' Calculate financial month
#' @description Gives financial month in "MX" format, where "M1" is April
#' of a given year
#'
#' @param date_variable A vector of dates
#'
#' @return A vector of financial months
#' @export
calculate_financial_month <- function(date_variable) {
    fin_month <- dplyr::case_when(
      dplyr::between(lubridate::month(date_variable), 1, 3) ~
        glue::glue("M{lubridate::month(date_variable) + 9}"),
      dplyr::between(lubridate::month(date_variable), 4, 12) ~
        glue::glue("M{lubridate::month(date_variable) - 3}")
    )
  return(fin_month)
}

#' Convert four-character year into "20XX/YY"
#'
#' @param four_char_year A vector of years, in "CCYY" format
#'
#' @return A vector of financial years in "20XX/YY" format
#' @export
format_financial_year <- function(four_char_year) {
  last_two_digits <- substr(four_char_year, 3, 4)
  next_year <- as.numeric(last_two_digits) + 1
  financial_year <- stringr::str_c("20", last_two_digits, "/", next_year)
  return(financial_year)
}
