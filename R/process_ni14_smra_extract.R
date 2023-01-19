#' Read in and process SMRA extract for NI14
#'
#' @return A tibble
#' @export
#' @import data.table
process_ni14_smra_extract <- function() {
  # Read in data
  smra_extract <-
    tibble::as_tibble(
      odbc::dbGetQuery(connect_to_smra(), readr::read_file("SQL/ni14_smra.sql"))
    ) %>%
    # For R-standard column names
    janitor::clean_names() %>%
    # Make sure these variables are in date format for quicker summarising
    dplyr::mutate(
      cis_admdate = as.Date(.data$admission_date),
      cis_disdate = as.Date(.data$discharge_date),
      discharge_type = as.integer(.data$discharge_type)
    ) %>%
    # Aggregate to unique CIS level, based on link_no and cis_marker
    # Aggregate to unique CIS level, based on link_no and cis_marker
    data.table::as.data.table() %>%
    dplyr::group_by(link_no, cis_marker) %>%
    dplyr::summarise(
      cis_admdate = min(cis_admdate),
      cis_disdate = max(cis_disdate),
      admission_type = dplyr::first(admission_type),
      discharge_type = dplyr::last(discharge_type)
    ) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble() %>%
    add_readmission_flag() %>%
    dplyr::mutate(discharged_dead = add_smra_death_flag(discharge_type))

  return(smra_extract)
}
