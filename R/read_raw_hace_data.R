#' Read in raw HACE data for manipulation
#'
#' @param hace_year The year of the published HACE results
#' @param sheet_num_hscp The Excel sheet number relevant to the HSCP-level data
#' @param sheet_num_loc The Excel sheet number relevant to the Locality-level data
#'
#' @return A list of length `2`, with the HSCP then the Locality data
#' @export
read_raw_hace_data <- function(hace_year, sheet_num_hscp, sheet_num_loc) {
  hscp_data <- readxl::read_excel(
    glue::glue("{get_ni_dir()}/NI 1-9/HSCP_{hace_year}_final_results_For_LIST.xlsx"),
    sheet = sheet_num_hscp,
    col_names = TRUE
  )

  locality_data <- readxl::read_excel(
    glue::glue("{get_ni_dir()}/NI 1-9/Locality_{hace_year}_final_results_For_LIST.xlsx"),
    sheet = sheet_num_loc,
    col_names = TRUE
  )

  return(list(hscp_data = hscp_data, locality_data = locality_data))
}
