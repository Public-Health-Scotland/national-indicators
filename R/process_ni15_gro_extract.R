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
    janitor::clean_names() %>%
    dplyr::mutate(
      cal_month = lubridate::month(.data$date_of_death),
      cal_year = lubridate::year(.data$date_of_death)
    ) %>%
    dplyr::left_join(., readr::read_rds(get_spd_path()), by = c("postcode" = "pc7")) %>%
    dplyr::left_join(., readr::read_rds(get_locality_path())) %>%
    dplyr::select("link_no":"cal_year", "ca2019name", "hscp_locality")

  return(deaths)
}
