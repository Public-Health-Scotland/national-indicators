#' NI directory (irf)
#'
#' @return The path to the NI folder
#' @export
get_ni_dir <- function(indicator = NULL) {
  ni_dir <- fs::path("/","conf","irf","03-Integration-Indicators","01-Core-Suite")
}

#' NI R output dir
#'
#' @return The path to the spreadsheet output folder
#' @export
get_ni_output_dir <- function() {
  output_dir <- fs::path(get_ni_dir(), "outputs")
  return(output_dir)
}


#' NI Spreadsheet output dir
#'
#' @return The path to the spreadsheet output folder
#' @export
get_ni_excel_output_dir <- function() {
  output_dir <- fs::path(get_ni_dir(), "spreadsheet_outputs")
  return(output_dir)
}

#' NI Tableau output dir
#'
#' @return The path to the Tableau output folder
#' @export
get_ni_tableau_output_dir <- function() {
  output_dir <- fs::path(get_ni_dir(), "tableau_outputs")
  return(output_dir)
}
