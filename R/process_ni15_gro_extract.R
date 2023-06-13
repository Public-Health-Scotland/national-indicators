#' Process NRS deaths extract for use with NI15
#'
#' @param extract_start Start date of extract in 'DD-MMM-YYYY' format
#' @param extract_end  End date of extract in 'DD-MMM-YYYY' format
#'
#' @return A tibble
#' @export
#' @seealso "SQL/ni15_gro.sql"
process_ni15_gro_extract <- function(extract_start, extract_end) {
  deaths_query <- glue::glue(readr::read_file("SQL/ni15_gro.sql"))

  deaths <- odbc::dbGetQuery(connect_to_smra(), deaths_query) %>%
    janitor::clean_names()

  return(deaths)
}
