#' Latest update
#'
#' @description Get the date of the latest update, e.g 'Sep-2022'
#'
#' @return Latest update as MMM-YYYY
#' @export
#'
#' @family Initialisation
latest_update <- function() {
  "Dec-2022"
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
  "Sep-2022"
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
  "2022"
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
    years = c("2020/21", "2021/22", "2022/23"),
    quarters = c(
      "2020/21 Q1", "2020/21 Q2", "2020/21 Q3", "2020/21 Q4",
      "2021/22 Q1", "2021/22 Q2", "2021/22 Q3", "2021/22 Q4",
      "2022/23 Q1", "2022/23 Q2", "2022/23 Q3"
    )
  )
}

#' Vector of diagnosis codes related to falls
#'
#' @return A vector containing the following diagnosis codes:
#' c("W00", "W01", "W02", "W03", "W04", "W05", "W06", "W07", "W08",
#' "W09", "W10", "W11", "W12", "W13", "W14", "W15", "W16", "W17",
#' "W18", "W19")
#' @export
#'
#' @family Initialisation
falls_diagnosis_codes <- function() {
  falls_codes <- paste0("W", stringr::str_pad(0:19, 2, "left", "0"))
}
