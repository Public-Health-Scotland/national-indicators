#' NI directory (irf)
#'
#' @return The path to the NI folder
#' @export
get_ni_dir <- function(indicator = NULL) {
  ni_dir <- fs::path(
    "/",
    "conf",
    "irf",
    "03-Integration-Indicators",
    "01-Core-Suite"
  )

  if (!is.null(indicator)) {
    ni_dir <- fs::path(
      ni_dir,
      dplyr::case_match(
        indicator,
        1:9 ~ "NI 1-9",
        10:22 ~ paste("NI", indicator),
        .default = NULL
      )
    )
  }
}

#' NI Spreadsheet output dir
#'
#' @return The path to the spreadsheet output folder
get_ni_excel_output_dir <- function() {
  output_dir <- fs::path(get_ni_dir(), "Spreadsheet outputs")
  return(output_dir)
}

#' NI Tableau output dir
#'
#' @return The path to the Tableau output folder
get_ni_tableau_output_dir <- function() {
  output_dir <- fs::path(get_ni_dir(), "Tableau Outputs")
  return(output_dir)
}
