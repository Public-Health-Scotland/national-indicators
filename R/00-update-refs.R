#' Latest update
#'
#' @description Get the date of the latest update, e.g 'Sep-2022'
#'
#' @return Latest update as MMM-YYYY
#' @export
#'
#' @family Initialisation
latest_update <- function() {
  "Mar-2026"
}

#' Previous update
#'
#' @description Get the date of the previous update, e.g 'Apr-2022'
#'
#' @return previous update as MMM-YYYY
#' @export
#'
#' @family Initialisation
previous_update <- function() {
  "Jul-2025"
}

#' Latest year of the HACE indicators
#'
#' @description Get the date of the latest HACE year
#'
#' @return latest HACE year as YYYY
#' @export
#'
#' @family Initialisation
latest_hace_year <- function() {
  "2024"
}

#' Vector of the indicators being updated this run
#'
#' @return A character vector of indicators, in format "NIXX"
#' @export
#'
#' @family Initialisation
indicators_to_update <- function() {
  c("NI12", "NI13", "NI14", "NI15", "NI16", "NI19", "NI20")
}

#' List of the update years and quarters for indicators 12, 13 and 20
#'
#' @return A list of two vectors: \describe{
#' \item{years}{A vector of financial years in "20XX/YY" format}
#' \item{quarters}{A vector of financial quarters in "20XX/YY QZ" format}}
#'
#' @export
#'
#' @family Initialisation
slf_indicator_update_periods <- function() {
  periods <- list(
    years = c("2020/21", "2021/22", "2022/23", "2023/24", "2024/25"),
    quarters = c(
      "2020/21 Q1", "2020/21 Q2", "2020/21 Q3", "2020/21 Q4",
      "2021/22 Q1", "2021/22 Q2", "2021/22 Q3", "2021/22 Q4",
      "2022/23 Q1", "2022/23 Q2", "2022/23 Q3", "2022/23 Q4",
      "2023/24 Q1", "2023/24 Q2", "2023/24 Q3", "2023/24 Q4",
      "2024/25 Q1", "2024/25 Q2", "2024/25 Q3", "2024/25 Q4",
      "2025/26 Q1", "2025/26 Q2", "2025/26 Q3", "2025/26 Q4"
    )
  )

  return(periods)
}
