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

#' Given a vector of month names, calculate the financial month in "MX" format
#'
#' @param month_variable A vector of month names
#'
#' @return A vector of financial months
#' @export
calculate_fin_month_from_string <- function(month_variable) {
  names <- stringr::str_to_lower(month.name)

  month_num <- match(month_variable, names)
  fin_month <- dplyr::case_when(
    dplyr::between(month_num, 1, 3) ~ glue::glue("M{month_num + 9}"),
    dplyr::between(month_num, 4, 12) ~ glue::glue("M{month_num - 3}")
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
  first_two_digits <- substr(four_char_year, 1, 2)
  financial_year <- stringr::str_c("20", first_two_digits, "/", last_two_digits)
  return(financial_year)
}

#' Get the full financial year, calendar years, and population year from a
#' single year input
#'
#' @param year A financial year in format "XXYY"
#'
#' @return A named list containing the financial_year, first_cal_year, second_cal_year and pop_year.
#' @export
get_different_years <- function(year) {
  financial_year <- format_financial_year(year)
  first_cal_year <- paste0("20", stringr::str_sub(year, 1, 2))
  second_cal_year <- paste0("20", stringr::str_sub(year, 3, 4))
  pop_year <- first_cal_year

  years <- list(financial_year, first_cal_year, second_cal_year, pop_year)
  names(years) <- c("financial_year", "first_cal_year", "second_cal_year", "pop_year")

  return(years)
}
