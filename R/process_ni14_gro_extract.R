#' Read in and process GRO Deaths extract for NI14
#'
#' @return A data frame
#' @export
process_ni14_gro_extract <- function() {
  gro_extract <-
    tibble::as_tibble(
      odbc::dbGetQuery(connect_to_smra(), ni14_gro_query())
    ) %>%
    # For R-standard column names
    janitor::clean_names() %>%
    # Order by link no for matching
    dplyr::arrange(.data$link_no) %>%
    # Use Lubridate to put into date format
    dplyr::mutate(death_date = as.Date(.data$death_date))
  return(gro_extract)
}
