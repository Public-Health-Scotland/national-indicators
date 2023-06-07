#' NI directory (irf)
#'
#' @return The path to the NI folder
#' @export
get_ni_dir <- function() {
  return(fs::path("/", "conf", "irf", "03-Integration-Indicators", "01-Core-Suite"))
}

#' NI R output dir
#'
#' @return The path to the spreadsheet output folder
#' @export
get_ni_output_dir <- function() {
  return(fs::path(get_ni_dir(), "outputs"))
}


#' NI Spreadsheet output dir
#'
#' @return The path to the spreadsheet output folder
#' @export
get_ni_excel_output_dir <- function() {
  return(fs::path(get_ni_dir(), "spreadsheet_outputs"))
}

#' NI Tableau output dir
#'
#' @return The path to the Tableau output folder
#' @export
get_ni_tableau_output_dir <- function() {
  return(fs::path(get_ni_dir(), "tableau_outputs"))
}

#' NI Tableau input dir
#'
#' @return The path to the Tableau input folder
get_ni_input_dir <- function() {
  input_dir <- fs::path(get_ni_dir(), "data_inputs")
  return(input_dir)
}
