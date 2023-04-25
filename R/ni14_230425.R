process_ni14_smra_extract_new <- function(min_date = "01-APR-2022") {

  smra_query <- glue::glue(readr::read_file("SQL/ni14_smra.sql"))

  # Read in data
  smra_extract <-
    tibble::as_tibble(
      odbc::dbGetQuery(connect_to_smra(), smra_query)
    ) %>%
    janitor::clean_names() %>%
    dplyr::mutate(
      admission_date = as.Date(.data$admission_date),
      discharge_date = as.Date(.data$discharge_date),
      discharge_type = as.integer(.data$discharge_type)
    ) %>%
    data.table::as.data.table() %>%
    dplyr::group_by(link_no, cis_marker) %>%
    dplyr::summarise(
      cis_admdate = min(admission_date),
      cis_disdate = max(discharge_date),
      admission_type = dplyr::first(admission_type),
      discharge_type = dplyr::last(discharge_type),
      postcode = dplyr::last(postcode)
    ) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble() %>%
    add_readmission_flag()
  return(smra_extract)

}




